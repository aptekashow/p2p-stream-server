from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import re

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from email.parser import BytesParser
from email.policy import default
from urllib.parse import urljoin, urlparse
import hashlib
import time
import httpx

from app.config import settings
from pydantic import BaseModel, Field

from app.acestream import AceStreamClient
from app.db import connect, init_db
from app.hls_relay import parse_playlist, delayed_playlist

ace = AceStreamClient()
PLAYLIST_PATH = Path("/app/data/ace-stream-playlist.m3u8")
CACHE_ROOT = Path("/app/data/hls-cache")
ACTIVE_STREAMS = {}
STREAM_LOCKS = {}
CACHE_TASKS = {}
CACHE_CLEANUP_TASK = None
WATCHDOG_TASK = None
SEGMENT_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(8)


def stream_lock(stream_id: int):
    return STREAM_LOCKS.setdefault(stream_id, asyncio.Lock())


async def create_stream_session(stream_id: int, infohash: str):
    result = await ace.start_hls(infohash)
    response_data = result.get("response") or {}
    playback_url = response_data.get("playback_url")
    if not playback_url:
        raise RuntimeError(f"AceStream не вернул playback_url: {result}")
    previous = ACTIVE_STREAMS.get(stream_id)
    recoveries = previous.get("recoveries", 0) if previous else 0
    ACTIVE_STREAMS[stream_id] = {
        "infohash": infohash,
        "playback_url": playback_url,
        "command_url": response_data.get("command_url"),
        "stat_url": response_data.get("stat_url"),
        "started_at": time.time(),
        "started_monotonic": time.monotonic(),
        "last_access_monotonic": time.monotonic(),
        "last_cache_update": None,
        "last_segment_progress_monotonic": time.monotonic(),
        "last_segment_sequence": None,
        "last_segment_signature": None,
        "last_error": None,
        "segments": {},
        "cached_segments": {},
        "playlist_header": [],
        "playlist_entries": [],
        "consecutive_errors": 0,
        "recoveries": recoveries,
    }
    return result


async def recover_stream_session(stream_id: int, failed_session: dict):
    async with stream_lock(stream_id):
        current = ACTIVE_STREAMS.get(stream_id)
        if current is not failed_session:
            return current
        try:
            await ace.stop_session(failed_session.get("command_url"))
        except Exception:
            pass
        result = await ace.start_hls(failed_session["infohash"])
        response_data = result.get("response") or {}
        playback_url = response_data.get("playback_url")
        if not playback_url:
            raise RuntimeError(f"AceStream не вернул playback_url при восстановлении: {result}")
        failed_session["playback_url"] = playback_url
        failed_session["command_url"] = response_data.get("command_url")
        failed_session["stat_url"] = response_data.get("stat_url")
        failed_session["last_error"] = None
        failed_session["started_monotonic"] = time.monotonic()
        failed_session["last_segment_progress_monotonic"] = time.monotonic()
        failed_session["last_segment_sequence"] = None
        failed_session["last_segment_signature"] = None
        failed_session["segments"] = {}
        failed_session["cached_segments"] = {}
        failed_session["playlist_header"] = []
        failed_session["playlist_entries"] = []
        failed_session["recoveries"] = failed_session.get("recoveries", 0) + 1
        return failed_session


def ensure_cache_worker(stream_id: int, session: dict):
    old_task = CACHE_TASKS.get(stream_id)
    if old_task and not old_task.done():
        old_task.cancel()
    CACHE_TASKS[stream_id] = asyncio.create_task(cache_stream_loop(stream_id, session))

async def ensure_stream_started(stream_id: int):
    with connect() as db:
        row = db.execute(
            "SELECT infohash FROM streams WHERE id=?", (stream_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "Канал не найден")

    async with stream_lock(stream_id):
        session = ACTIVE_STREAMS.get(stream_id)
        task = CACHE_TASKS.get(stream_id)
        if session and task and not task.done():
            return {"already_running": True}

        if session:
            old_task = CACHE_TASKS.pop(stream_id, None)
            if old_task and not old_task.done():
                old_task.cancel()
                await asyncio.gather(old_task, return_exceptions=True)
            try:
                await ace.stop_session(session.get("command_url"))
            except Exception:
                pass
            ACTIVE_STREAMS.pop(stream_id, None)

        result = await create_stream_session(stream_id, row["infohash"])
        session = ACTIVE_STREAMS[stream_id]
        ensure_cache_worker(stream_id, session)
        return result


def cleanup_channel_cache(stream_id: int):
    directory = CACHE_ROOT / str(stream_id)
    if not directory.exists():
        return
    cutoff = time.time() - settings.hls_cache_seconds
    for path in directory.iterdir():
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except OSError:
            pass
    session = ACTIVE_STREAMS.get(stream_id)
    if session:
        cached = session.get("cached_segments", {})
        valid = {}
        for sequence, item in cached.items():
            path = Path(item["path"])
            try:
                if path.is_file() and path.stat().st_mtime >= cutoff:
                    valid[sequence] = item
            except OSError:
                continue
        session["cached_segments"] = valid


def cleanup_all_channel_caches():
    if not CACHE_ROOT.exists():
        return
    cutoff = time.time() - settings.hls_cache_seconds
    for directory in CACHE_ROOT.iterdir():
        if not directory.is_dir():
            continue
        for path in directory.iterdir():
            try:
                if path.is_file() and path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
            except OSError:
                pass
        try:
            # Не удалять пустую папку активного канала: фоновый загрузчик
            # продолжает писать в неё следующие сегменты.
            try:
                stream_id = int(directory.name)
            except ValueError:
                stream_id = None
            if stream_id not in ACTIVE_STREAMS and not any(directory.iterdir()):
                directory.rmdir()
        except OSError:
            pass


async def watchdog_loop():
    """Перезапускает задачи и AceStream-сессии, если поток перестал выдавать сегменты."""
    while True:
        for stream_id, session in list(ACTIVE_STREAMS.items()):
            task = CACHE_TASKS.get(stream_id)
            if task is None or task.done():
                async with stream_lock(stream_id):
                    if ACTIVE_STREAMS.get(stream_id) is session:
                        current_task = CACHE_TASKS.get(stream_id)
                        if current_task is None or current_task.done():
                            ensure_cache_worker(stream_id, session)
                continue

            now = time.monotonic()
            if session.get("last_segment_sequence") is None:
                # Дать Engine время на первичную P2P-буферизацию, не перезапускать старт каждые 45 секунд.
                if now - session.get("started_monotonic", now) < 120:
                    continue
            last_progress = session.get("last_segment_progress_monotonic", now)
            if now - last_progress < 45:
                continue

            async with stream_lock(stream_id):
                if ACTIVE_STREAMS.get(stream_id) is not session:
                    continue
                old_task = CACHE_TASKS.pop(stream_id, None)
                if old_task and old_task is not asyncio.current_task() and not old_task.done():
                    old_task.cancel()
                    await asyncio.gather(old_task, return_exceptions=True)
                try:
                    await ace.stop_session(session.get("command_url"))
                except Exception:
                    pass
                try:
                    result = await ace.start_hls(session["infohash"])
                    response_data = result.get("response") or {}
                    playback_url = response_data.get("playback_url")
                    if not playback_url:
                        raise RuntimeError(f"AceStream не вернул playback_url при watchdog-перезапуске: {result}")
                    session.update({
                        "playback_url": playback_url,
                        "command_url": response_data.get("command_url"),
                        "stat_url": response_data.get("stat_url"),
                        "last_error": None,
                        "started_monotonic": time.monotonic(),
                        "last_segment_progress_monotonic": time.monotonic(),
                        "last_segment_sequence": None,
                        "last_segment_signature": None,
                        "segments": {},
                        "cached_segments": {},
                        "playlist_header": [],
                        "playlist_entries": [],
                        "recoveries": session.get("recoveries", 0) + 1,
                    })
                    ensure_cache_worker(stream_id, session)
                except Exception as exc:
                    session["last_error"] = f"Watchdog: {exc}"
                    session["last_segment_progress_monotonic"] = time.monotonic()
        await asyncio.sleep(5)


async def cache_cleanup_loop():
    while True:
        now = time.monotonic()
        idle_streams = []
        for stream_id, session in list(ACTIVE_STREAMS.items()):
            # HLS-клиент обновляет live-плейлист с интервалом, зависящим
            # от EXT-X-TARGETDURATION. Не обрываем поток раньше двух таких интервалов.
            # VLC обновляет HLS-плейлист не так часто, как браузерный плеер.
            # Даём клиентам запас времени; после закрытия VLC сессия всё равно остановится.
            idle_timeout = max(60.0, float(session.get("target_duration", 0) or 0) * 4.0)
            if now - session.get("last_access_monotonic", now) > idle_timeout:
                idle_streams.append(stream_id)
        for stream_id in idle_streams:
            try:
                await stop_stream_session(stream_id)
            except Exception:
                pass
        cleanup_all_channel_caches()
        await asyncio.sleep(1)


async def cache_stream_loop(stream_id: int, session: dict):
    """Fetch AceStream HLS continuously and persist complete segments in a 60s ring cache."""
    directory = CACHE_ROOT / str(stream_id)
    directory.mkdir(parents=True, exist_ok=True)
    failures = 0
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_connections=12, max_keepalive_connections=8),
        follow_redirects=True,
    ) as client:
        while ACTIVE_STREAMS.get(stream_id) is session:
            try:
                cleanup_channel_cache(stream_id)
                playlist_url = session["playback_url"]
                response = await client.get(playlist_url)
                response.raise_for_status()
                parsed = parse_playlist(response.text, playlist_url)
                if parsed.variant_url:
                    playlist_url = parsed.variant_url
                    response = await client.get(playlist_url)
                    response.raise_for_status()
                    parsed = parse_playlist(response.text, playlist_url)
                    if parsed.variant_url:
                        raise RuntimeError("Вложенные master-плейлисты AceStream не поддерживаются")

                session["playlist_header"] = list(parsed.header)
                session["target_duration"] = parsed.target_duration
                session["playlist_entries"] = [
                    {
                        "sequence": segment.sequence,
                        "extinf": segment.extinf,
                        "duration": segment.duration,
                        "tags": list(segment.tags),
                        "source_url": segment.uri,
                    }
                    for segment in parsed.segments
                ]
                session["last_cache_update"] = time.time()

                semaphore = asyncio.Semaphore(2)

                async def download_segment(segment):
                    sequence = segment.sequence
                    source_url = segment.uri
                    digest = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:12]
                    path = directory / f"{sequence}-{digest}.ts"
                    cached = session.setdefault("cached_segments", {}).get(sequence)
                    if (
                        cached
                        and cached.get("source_url") == source_url
                        and Path(cached["path"]).is_file()
                    ):
                        return None

                    async with semaphore, SEGMENT_DOWNLOAD_SEMAPHORE:
                        segment_response = await client.get(source_url)
                        segment_response.raise_for_status()
                        if not segment_response.content:
                            raise RuntimeError(f"Пустой HLS-сегмент: {source_url}")
                        temporary = path.with_suffix(".ts.tmp")
                        await asyncio.to_thread(temporary.write_bytes, segment_response.content)
                        await asyncio.to_thread(temporary.replace, path)

                    return sequence, {
                        "path": str(path),
                        "extinf": segment.extinf,
                        "duration": segment.duration,
                        "tags": list(segment.tags),
                        "source_url": source_url,
                        "cached_at": time.time(),
                    }

                # Сначала забираем самые новые сегменты: старые элементы скользящего
                # окна Engine могут уже исчезнуть и не должны блокировать весь канал.
                tasks = [
                    asyncio.create_task(download_segment(segment))
                    for segment in reversed(parsed.segments)
                ]
                download_errors = []
                try:
                    for completed in asyncio.as_completed(tasks):
                        try:
                            item = await completed
                            if item:
                                session["cached_segments"][item[0]] = item[1]
                        except Exception as download_exc:
                            download_errors.append(str(download_exc))
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                newest = parsed.segments[-1]
                newest_cached = session.get("cached_segments", {}).get(newest.sequence)
                if not newest_cached or newest_cached.get("source_url") != newest.uri:
                    detail = download_errors[0] if download_errors else "последний сегмент ещё не доступен"
                    raise RuntimeError(f"Не удалось получить новый HLS-сегмент: {detail}")
                newest_signature = f"{newest.sequence}|{newest.uri}"
                if session.get("last_segment_signature") != newest_signature:
                    session["last_segment_signature"] = newest_signature
                    session["last_segment_sequence"] = newest.sequence
                    session["last_segment_progress_monotonic"] = time.monotonic()

                failures = 0
                session["consecutive_errors"] = 0
                session["last_error"] = None
                delay = max(0.5, min(settings.hls_cache_poll_seconds, parsed.target_duration / 2 or 1.5))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                session["consecutive_errors"] = failures
                session["last_error"] = str(exc)
                delay = min(15, 2 ** min(failures, 4))
                if failures >= 4:
                    try:
                        await recover_stream_session(stream_id, session)
                        failures = 0
                        session["consecutive_errors"] = 0
                    except Exception as recovery_exc:
                        session["last_error"] = f"{exc}; ошибка восстановления AceStream: {recovery_exc}"
            cleanup_channel_cache(stream_id)
            await asyncio.sleep(delay)


@asynccontextmanager
async def lifespan(app):
    global CACHE_CLEANUP_TASK, WATCHDOG_TASK
    init_db()
    cleanup_all_channel_caches()
    CACHE_CLEANUP_TASK = asyncio.create_task(cache_cleanup_loop())
    WATCHDOG_TASK = asyncio.create_task(watchdog_loop())
    try:
        yield
    finally:
        tasks = list(CACHE_TASKS.values())
        if CACHE_CLEANUP_TASK:
            tasks.append(CACHE_CLEANUP_TASK)
        if WATCHDOG_TASK:
            tasks.append(WATCHDOG_TASK)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(
    title="P2P Stream Server",
    version="0.3.0",
    lifespan=lifespan,
)


class ImportItem(BaseModel):
    infohash: str = Field(min_length=40, max_length=40)
    name: str = Field(min_length=1, max_length=300)
    categories: list[str] = Field(default_factory=list)
    availability: float = 0
    status: int = 0


def parse_import_playlist(content: str):
    items = []
    invalid = 0
    current_name = None
    current_category = ""
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if line.startswith("#EXTINF:"):
            current_name = line.split(",", 1)[1].strip() if "," in line else None
            group = re.search(r'group-title="([^"]*)"', line, re.IGNORECASE)
            current_category = group.group(1).strip() if group else ""
            continue
        if not line or line.startswith("#"):
            continue
        match = re.search(r"infohash=([a-fA-F0-9]{40})", line)
        if not match or not current_name:
            invalid += 1
            current_name = None
            current_category = ""
            continue
        items.append((match.group(1).lower(), current_name[:300], current_category[:200], 0, 0))
        current_name = None
        current_category = ""
    # Один infohash — одна запись. Последнее название в файле имеет приоритет.
    unique = {}
    for item in items:
        unique[item[0]] = item
    return list(unique.values()), invalid


def import_playlist():
    if not PLAYLIST_PATH.is_file():
        raise FileNotFoundError(f"Файл каталога не найден: {PLAYLIST_PATH}")
    content = PLAYLIST_PATH.read_text(encoding="utf-8-sig", errors="replace")
    items, invalid = parse_import_playlist(content)
    if not items:
        raise ValueError("В плейлисте не найдено ни одного корректного AceStream infohash")
    with connect() as db:
        previous = db.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
        db.execute("DELETE FROM streams")
        db.executemany(
            "INSERT INTO streams(infohash,name,categories,availability,status) VALUES(?,?,?,?,?)",
            items,
        )
    return {
        "ok": True,
        "mode": "replace",
        "deleted": previous,
        "imported": len(items),
        "invalid": invalid,
        "source": PLAYLIST_PATH.name,
    }


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse("""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>P2P Stream Server</title>
<style>
*{box-sizing:border-box}
body{background:#10151f;color:#e8edf5;font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 20px}
section{background:#1a2332;border:1px solid #2b374b;border-radius:14px;padding:20px;margin-top:20px}
input,button{padding:12px;border-radius:8px;border:1px solid #3a4961;background:#111a28;color:white}
input{width:min(100%,420px)}
button{background:#2563eb;cursor:pointer}
pre{white-space:pre-wrap;overflow-wrap:anywhere}
.small{color:#9ba9bd}
.stream-item{border:1px solid #34445c;border-radius:10px;padding:12px;margin:10px 0}
.stream-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.stream-actions button{padding:8px 12px}
button.secondary{background:#334155}
button.danger{background:#b42332}
.status-ok{color:#5ee0a0}
.status-error{color:#ff9b9b}
</style>
</head>
<body>
<h1>P2P Stream Server</h1>
<p class="small">AceStream → HLS · задержка {settings.default_buffer_seconds} сек. · Ubuntu 24.04 · v0.3.0</p>

<section>
<h2>Каталог каналов</h2>
<form class="stream-actions" method="post" action="/api/catalog/upload" enctype="multipart/form-data">
<input id="playlist-file" name="playlist" type="file" accept=".m3u,.m3u8,audio/x-mpegurl,application/vnd.apple.mpegurl" required>
<button id="upload-playlist" type="submit">Загрузить и заменить каталог</button>

<a href="/playlist.m3u" download="p2p-stream-playlist.m3u" style="display:inline-block;padding:12px;border-radius:8px;background:#334155;color:white;text-decoration:none">Скачать весь плейлист</a>
</form>
<p id="import-result" class="small">Выберите M3U-файл и нажмите «Загрузить и заменить каталог». Замена выполняется автоматически.</p>
</section>

<section>
<h2>Воспроизведение</h2>
<video id="stream-player" controls autoplay muted playsinline style="display:none;width:100%;max-height:520px;background:#000;border-radius:10px"></video>
<p id="player-status" class="small">Выберите канал и нажмите «Запустить».</p>
</section>

<section>
<h2>Активные каналы</h2>
<button id="refresh-streams" type="button">Обновить</button>
<div id="active-streams" class="small">Активных каналов пока нет.</div>
</section>

<section>
<h2>Поиск в своём каталоге</h2>
<form id="f">
<input id="q" placeholder="Название канала или часть названия" required>
<button>Искать</button>
</form>
<div id="out">Введите запрос.</div>
</section>

<section>
<h2>Состояние Engine</h2>
<pre id="health">Проверка...</pre>
</section>

<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<script>
let activeHls = null;
let activeStreamId = null;
let streamHeartbeat = null;

function stopStreamHeartbeat(){
    if(streamHeartbeat){ clearInterval(streamHeartbeat); streamHeartbeat=null; }
}
function startStreamHeartbeat(id){
    stopStreamHeartbeat();
    const ping=()=>fetch('/live/'+id+'/keepalive',{method:'POST',cache:'no-store'}).catch(()=>{});
    ping();
    streamHeartbeat=setInterval(ping,2000);
}

async function playStream(id){
    const video = document.querySelector('#stream-player');
    const status = document.querySelector('#player-status');
    const previousId = activeStreamId;
    stopStreamHeartbeat();
    if(previousId !== null && Number(previousId) !== Number(id)){
        try{ await fetch('/api/streams/'+previousId+'/stop',{method:'POST'}); }catch(e){}
    }
    activeStreamId = Number(id);
    const url = '/live/'+id+'/index.m3u8';
    video.style.display='block';
    if(activeHls){ activeHls.destroy(); activeHls=null; }
    video.pause();
    video.removeAttribute('src');
    video.load();
    startStreamHeartbeat(id);
    video.muted=true;
    status.textContent='Запуск потока и заполнение буфера...';
    if(video.canPlayType('application/vnd.apple.mpegurl')){
        video.src=url;
        video.oncanplay=()=>video.play().then(()=>{
            status.textContent='Воспроизведение запущено';
        }).catch(()=>{status.textContent='Нажмите Play для начала воспроизведения';});
        video.onerror=()=>{status.textContent='Ошибка загрузки HLS-потока';};
    }else if(window.Hls && Hls.isSupported()){
        activeHls=new Hls({enableWorker:true,lowLatencyMode:false,
            manifestLoadingTimeOut:60000,manifestLoadingMaxRetry:5,
            levelLoadingTimeOut:60000,levelLoadingMaxRetry:5,
            fragLoadingTimeOut:60000,fragLoadingMaxRetry:6});
        activeHls.loadSource(url);
        activeHls.attachMedia(video);
        activeHls.on(Hls.Events.MANIFEST_PARSED,()=>{
            video.play().then(()=>{
                status.textContent='Воспроизведение запущено';
            }).catch(()=>{status.textContent='Нажмите Play для начала воспроизведения';});
        });
        activeHls.on(Hls.Events.ERROR,(event,data)=>{
            if(data.fatal) status.textContent='Ошибка HLS: '+(data.details||data.type);
        });
    }else{
        status.textContent='Этот браузер не поддерживает HLS';
    }
}

const streamVideo=document.querySelector('#stream-player');
streamVideo.addEventListener('pause',stopStreamHeartbeat);
streamVideo.addEventListener('play',()=>{
    if(activeStreamId !== null) startStreamHeartbeat(activeStreamId);
});

function escapeHtml(value){
    return String(value ?? '').replace(/[&<>"']/g, ch => ({
        '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[ch]));
}

async function streamAction(id, action){
    const out=document.querySelector('#active-streams');
    if((action==='start' || action==='restart') && activeStreamId !== null && Number(activeStreamId)!==Number(id)){
        const previousId=activeStreamId;
        stopStreamHeartbeat();
        activeStreamId=null;
        if(activeHls){ activeHls.destroy(); activeHls=null; }
        const video=document.querySelector('#stream-player');
        video.pause();
        video.removeAttribute('src');
        video.load();
        try{ await fetch('/api/streams/'+previousId+'/stop',{method:'POST'}); }catch(e){}
    }
    if(action==='stop' && activeStreamId !== null && Number(activeStreamId)===Number(id)){
        stopStreamHeartbeat();
        activeStreamId=null;
        if(activeHls){ activeHls.destroy(); activeHls=null; }
        const video=document.querySelector('#stream-player');
        video.pause();
        video.removeAttribute('src');
        video.load();
        document.querySelector('#player-status').textContent='Поток остановлен';
    }
    out.textContent='Выполняется операция...';
    try{
        const r=await fetch('/api/streams/'+id+'/'+action,{method:'POST'});
        const data=await r.json();
        if(!r.ok) throw new Error(data.detail || JSON.stringify(data));
        if(action==='start' || action==='restart') await playStream(id);
        await loadActiveStreams();
    }catch(e){
        out.textContent='Ошибка: '+e;
    }
}

async function loadActiveStreams(){
    const out=document.querySelector('#active-streams');
    try{
        const r=await fetch('/api/streams/status');
        const data=await r.json();
        if(!r.ok) throw new Error(data.detail || JSON.stringify(data));
        if(!data.length){
            out.textContent='Активных каналов пока нет.';
            return;
        }
        out.innerHTML=data.map(item=>{
            const state=item.status==='running'
                ? '<span class="status-ok">Работает</span>'
                : item.status==='recovering'
                    ? '<span class="status-error">Восстанавливается</span>'
                    : '<span class="status-error">Ошибка</span>';
            const stats=item.peers===null ? 'статистика недоступна'
                : 'пиров: '+item.peers+' · ↓ '+item.speed_down+' КБ/с · ↑ '+item.speed_up+' КБ/с';
            const error=item.last_error
                ? '<div class="status-error">'+escapeHtml(item.last_error)+'</div>' : '';
            return '<div class="stream-item"><strong>'+escapeHtml(item.name)+'</strong><div>'+state+
                ' · сегментов в кэше: '+item.cached_segments+' · восстановлений: '+item.recoveries+
                '</div><div class="small">'+stats+'</div>'+error+
                '<div class="stream-actions"><button class="danger" onclick="streamAction('+item.id+',\'stop\')">Остановить</button>'+
                '<button class="secondary" onclick="streamAction('+item.id+',\'restart\')">Перезапустить</button></div></div>';
        }).join('');
    }catch(e){
        out.textContent='Ошибка загрузки статуса: '+e;
    }
}

async function health(){
    try{
        let r=await fetch('/api/health');
        document.querySelector('#health').textContent=
            JSON.stringify(await r.json(),null,2);
    }catch(e){
        document.querySelector('#health').textContent=String(e);
    }
}


document.querySelector('#f').onsubmit=async e=>{
    e.preventDefault();
    let out=document.querySelector('#out');
    out.textContent='Поиск...';
    try{
        let r=await fetch('/api/search?q='+encodeURIComponent(
            document.querySelector('#q').value
        ));
        const data=await r.json();
        if(!r.ok) throw new Error(data.detail || JSON.stringify(data));
        if(!data.results.length){
            out.textContent='Ничего не найдено.';
            return;
        }
        out.innerHTML=data.results.map(item=>
            '<div class="stream-item"><strong>'+escapeHtml(item.name)+'</strong>'+
            '<div class="small">ID: '+item.id+'</div>'+
            '<div class="stream-actions"><button onclick="streamAction('+item.id+',\'start\')">Запустить</button></div></div>'
        ).join('');
    }catch(e){
        out.textContent=String(e);
    }
};


document.querySelector('#refresh-streams').onclick=loadActiveStreams;
health();
loadActiveStreams();
setInterval(loadActiveStreams,5000);
</script>
</body>
</html>""")


@app.get("/api/health")
async def health():
    try:
        return {"ok": True, "engine": await ace.health()}
    except Exception as exc:
        return {"ok": False, "engine_error": str(exc)}


@app.post("/api/catalog/upload")
async def catalog_upload(request: Request):
    """Принимает M3U через обычную multipart/form-data HTML-форму."""
    content_type = request.headers.get("content-type", "")
    if not content_type.lower().startswith("multipart/form-data"):
        raise HTTPException(415, "Отправьте файл через форму загрузки M3U на главной странице")
    body = await request.body()
    if len(body) > 6 * 1024 * 1024:
        raise HTTPException(413, "Плейлист больше 5 МБ")
    try:
        message = BytesParser(policy=default).parsebytes(
            b"Content-Type: " + content_type.encode("ascii", "ignore") +
            b"\r\nMIME-Version: 1.0\r\n\r\n" + body
        )
        uploaded = None
        filename = "playlist.m3u"
        for part in message.iter_parts():
            if part.get_content_disposition() == "form-data" and part.get_param("name", header="content-disposition") == "playlist":
                uploaded = part.get_payload(decode=True)
                filename = part.get_filename() or filename
                break
    except Exception as exc:
        raise HTTPException(400, f"Не удалось прочитать загруженный файл: {exc}")
    if not uploaded:
        raise HTTPException(400, "Файл не выбран или он пустой")
    content = uploaded.decode("utf-8-sig", errors="replace")
    if "#EXTM3U" not in content[:500] or not re.search(r"infohash=[a-fA-F0-9]{40}", content):
        raise HTTPException(400, "Файл не похож на AceStream M3U-плейлист")
    items, invalid = parse_import_playlist(content)
    if not items:
        raise HTTPException(400, "В плейлисте не найдено ни одного корректного AceStream-канала")
    stopped = 0
    for stream_id in list(ACTIVE_STREAMS):
        await stop_stream_session(stream_id)
        stopped += 1
    PLAYLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = PLAYLIST_PATH.with_suffix(PLAYLIST_PATH.suffix + ".tmp")
    try:
        temporary.write_text(content, encoding="utf-8")
        with connect() as db:
            previous = db.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
            db.execute("DELETE FROM streams")
            db.executemany(
                "INSERT INTO streams(infohash,name,categories,availability,status) VALUES(?,?,?,?,?)",
                items,
            )
        temporary.replace(PLAYLIST_PATH)
    except Exception as exc:
        temporary.unlink(missing_ok=True)
        raise HTTPException(500, f"Не удалось заменить каталог: {exc}")
    return HTMLResponse(
        "<!doctype html><html lang='ru'><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Каталог заменён</title>"
        "<body style='font:16px system-ui;background:#10151f;color:#e8edf5;max-width:700px;margin:60px auto;padding:20px'>"
        "<h1>Каталог успешно заменён</h1>"
        "<p>M3U-файл принят и обработан.</p>"
        f"<p>Удалено старых каналов: {previous}</p>"
        f"<p>Загружено каналов: {len(items)}</p>"
        f"<p>Пропущено строк: {invalid}</p>"
        f"<p>Остановлено старых трансляций: {stopped}</p>"
        "<p><a style='color:#7db4ff' href='/'>Вернуться к каталогу</a></p>"
        "</body></html>",
        status_code=200,
    )

@app.post("/api/catalog/import")
async def catalog_import():
    try:
        # Старые сессии относятся к прежнему каталогу — остановить их перед заменой.
        for stream_id in list(ACTIVE_STREAMS):
            await stop_stream_session(stream_id)
        return import_playlist()
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc))
    except Exception as exc:
        raise HTTPException(500, f"Ошибка импорта каталога: {exc}")


@app.get("/api/catalog/stats")
async def catalog_stats():
    with connect() as db:
        total = db.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
    return {"total_channels": total}


@app.get("/api/search")
async def search(
    q: str = Query(min_length=1, max_length=200),
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=200),
):
    search_term = q.strip()

    with connect() as db:
        total = db.execute(
            "SELECT COUNT(*) FROM streams WHERE instr(casefold(name), casefold(?)) > 0",
            (search_term,),
        ).fetchone()[0]

        rows = db.execute(
            """
            SELECT id, name, infohash, categories, availability, status
            FROM streams
            WHERE instr(casefold(name), casefold(?)) > 0
            ORDER BY name COLLATE NOCASE
            LIMIT ? OFFSET ?
            """,
            (search_term, page_size, page * page_size),
        ).fetchall()

    return {
        "query": q,
        "total": total,
        "page": page,
        "page_size": page_size,
        "results": [dict(row) for row in rows],
    }


@app.post("/api/streams/import")
async def import_stream(item: ImportItem):
    if not re.fullmatch(r"[a-fA-F0-9]{40}", item.infohash):
        raise HTTPException(400, "Некорректный infohash")

    infohash = item.infohash.lower()

    with connect() as db:
        db.execute(
            """
            INSERT INTO streams(infohash,name,categories,availability,status)
            VALUES(?,?,?,?,?)
            ON CONFLICT(infohash) DO UPDATE SET
                name=excluded.name,
                categories=excluded.categories,
                availability=excluded.availability,
                status=excluded.status
            """,
            (
                infohash,
                item.name,
                ",".join(item.categories),
                item.availability,
                item.status,
            ),
        )

    return {"ok": True, "infohash": infohash}


@app.get("/api/streams")
async def list_streams():
    with connect() as db:
        rows = db.execute(
            "SELECT * FROM streams ORDER BY name COLLATE NOCASE"
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def playlist(request: Request):
    with connect() as db:
        rows = db.execute(
            "SELECT id,name FROM streams ORDER BY name COLLATE NOCASE"
        ).fetchall()

    lines = ["#EXTM3U"]

    for row in rows:
        lines += [
            f"#EXTINF:-1,{row['name']}",
            f"{str(request.base_url).rstrip('/')}/live/{row['id']}/index.m3u8",
        ]

    return "\n".join(lines) + "\n"


async def stop_stream_session(stream_id: int):
    async with stream_lock(stream_id):
        session = ACTIVE_STREAMS.get(stream_id)
        if not session:
            return {"stopped": False, "message": "Канал уже остановлен"}
        task = CACHE_TASKS.pop(stream_id, None)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        engine_result = None
        engine_error = None
        try:
            engine_result = await ace.stop_session(session.get("command_url"))
        except Exception as exc:
            engine_error = str(exc)
        ACTIVE_STREAMS.pop(stream_id, None)
        return {
            "stopped": True,
            "engine_stopped": engine_error is None,
            "engine_result": engine_result,
            "engine_error": engine_error,
        }


@app.get("/api/streams/status")
async def streams_status():
    with connect() as db:
        rows = db.execute("SELECT id, name FROM streams ORDER BY name COLLATE NOCASE").fetchall()
    active = []
    for row in rows:
        stream_id = row["id"]
        session = ACTIVE_STREAMS.get(stream_id)
        if not session:
            continue
        task = CACHE_TASKS.get(stream_id)
        stats = {}
        try:
            stats = await ace.session_stats(session.get("stat_url"))
        except Exception:
            pass
        active.append({
            "id": stream_id,
            "name": row["name"],
            "status": ("recovering" if session.get("last_error") else "running") if task and not task.done() else "error",
            "peers": stats.get("peers"),
            "speed_down": stats.get("speed_down", 0),
            "speed_up": stats.get("speed_up", 0),
            "cached_segments": len(session.get("cached_segments", {})),
            "recoveries": session.get("recoveries", 0),
            "last_cache_update": session.get("last_cache_update"),
            "last_error": session.get("last_error"),
        })
    return active


@app.post("/api/streams/{stream_id}/stop")
async def stop_stream(stream_id: int):
    return await stop_stream_session(stream_id)


@app.post("/api/streams/{stream_id}/restart")
async def restart_stream(stream_id: int):
    with connect() as db:
        row = db.execute("SELECT infohash FROM streams WHERE id=?", (stream_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Канал не найден")
    await stop_stream_session(stream_id)
    try:
        async with stream_lock(stream_id):
            result = await create_stream_session(stream_id, row["infohash"])
            session = ACTIVE_STREAMS[stream_id]
        ensure_cache_worker(stream_id, session)
        return {**result, "stream_url": f"/live/{stream_id}/index.m3u8"}
    except Exception as exc:
        raise HTTPException(502, f"Не удалось перезапустить AceStream: {exc}")


@app.post("/api/streams/{stream_id}/start")
async def start_stream(stream_id: int):
    try:
        result = await ensure_stream_started(stream_id)
        return {**result, "stream_url": f"/live/{stream_id}/index.m3u8"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Не удалось запустить AceStream: {exc}")


@app.get("/live/{stream_id}/index.m3u8", response_class=PlainTextResponse)
async def live_playlist(stream_id: int):
    try:
        await ensure_stream_started(stream_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Не удалось автоматически запустить канал: {exc}")

    deadline = time.monotonic() + 60
    while True:
        session = ACTIVE_STREAMS.get(stream_id)
        if not session:
            raise HTTPException(503, "Сессия канала перезапускается")
        session["last_access_monotonic"] = time.monotonic()
        cleanup_channel_cache(stream_id)
        output = delayed_playlist(
            session.get("playlist_header", []),
            session.get("playlist_entries", []),
            session.get("cached_segments", {}),
            settings.default_buffer_seconds,
            settings.hls_cache_seconds,
            time.time(),
            stream_id,
        )
        if output:
            return PlainTextResponse(
                output,
                media_type="application/vnd.apple.mpegurl",
                headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
            )
        if time.monotonic() >= deadline:
            error = session.get("last_error")
            detail = (
                f"Буфер {settings.default_buffer_seconds} сек. не сформирован за 60 секунд; "
                f"готовых сегментов: {len(session.get('cached_segments', {}))}"
            )
            if error:
                detail += f"; последняя ошибка: {error}"
            raise HTTPException(503, detail)
        await asyncio.sleep(0.5)


@app.post("/live/{stream_id}/keepalive")
async def live_keepalive(stream_id: int):
    session = ACTIVE_STREAMS.get(stream_id)
    if not session:
        raise HTTPException(404, "Поток уже остановлен")
    session["last_access_monotonic"] = time.monotonic()
    return {"ok": True}


@app.get("/live/{stream_id}/segment/{sequence}.ts")
async def live_segment(stream_id: int, sequence: int):
    session = ACTIVE_STREAMS.get(stream_id)
    if not session:
        raise HTTPException(404, "Поток не запущен")

    session["last_access_monotonic"] = time.monotonic()
    cleanup_channel_cache(stream_id)
    item = session.get("cached_segments", {}).get(sequence)
    if not item:
        raise HTTPException(404, "Сегмент отсутствует в минутном кэше")
    path = Path(item["path"])
    if not path.is_file():
        raise HTTPException(404, "Срок хранения сегмента истёк (60 секунд)")
    try:
        content = await asyncio.to_thread(path.read_bytes)
    except OSError as exc:
        raise HTTPException(500, f"Не удалось прочитать сегмент кэша: {exc}")
    return Response(
        content=content,
        media_type="video/mp2t",
        headers={"Cache-Control": "no-store"},
    )
