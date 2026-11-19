from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import re

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from urllib.parse import urljoin, urlparse
import hashlib
import time
import httpx

from app.config import settings
from pydantic import BaseModel, Field

from app.acestream import AceStreamClient
from app.db import connect, init_db

ace = AceStreamClient()
PLAYLIST_PATH = Path("/app/data/ace-stream-playlist.m3u8")
CACHE_ROOT = Path("/app/data/hls-cache")
ACTIVE_STREAMS = {}
STREAM_LOCKS = {}
CACHE_TASKS = {}


def stream_lock(stream_id: int):
    return STREAM_LOCKS.setdefault(stream_id, asyncio.Lock())


async def create_stream_session(stream_id: int, infohash: str):
    result = await ace.start_hls(infohash)
    playback_url = (result.get("response") or {}).get("playback_url")
    if not playback_url:
        raise RuntimeError(f"AceStream не вернул playback_url: {result}")
    previous = ACTIVE_STREAMS.get(stream_id)
    recoveries = previous.get("recoveries", 0) if previous else 0
    ACTIVE_STREAMS[stream_id] = {
        "infohash": infohash,
        "playback_url": playback_url,
        "segments": {},
        "cached_segments": {},
        "playlist_header": [],
        "recoveries": recoveries,
    }
    return result


async def recover_stream_session(stream_id: int, failed_session: dict):
    async with stream_lock(stream_id):
        current = ACTIVE_STREAMS.get(stream_id)
        if current is not failed_session:
            return current
        result = await ace.start_hls(failed_session["infohash"])
        playback_url = (result.get("response") or {}).get("playback_url")
        if not playback_url:
            raise RuntimeError(f"AceStream не вернул playback_url при восстановлении: {result}")
        failed_session["playback_url"] = playback_url
        failed_session["segments"] = {}
        failed_session["playlist_header"] = []
        failed_session["recoveries"] = failed_session.get("recoveries", 0) + 1
        return failed_session


def ensure_cache_worker(stream_id: int, session: dict):
    old_task = CACHE_TASKS.get(stream_id)
    if old_task and not old_task.done():
        old_task.cancel()
    CACHE_TASKS[stream_id] = asyncio.create_task(cache_stream_loop(stream_id, session))


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
        session["cached_segments"] = {
            seq: item for seq, item in cached.items()
            if Path(item["path"]).is_file()
            and Path(item["path"]).stat().st_mtime >= cutoff
        }


async def cache_stream_loop(stream_id: int, session: dict):
    directory = CACHE_ROOT / str(stream_id)
    directory.mkdir(parents=True, exist_ok=True)
    while ACTIVE_STREAMS.get(stream_id) is session:
        cleanup_channel_cache(stream_id)
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(session["playback_url"])
                response.raise_for_status()
                header, entries, pending_extinf = [], [], None
                for raw_line in response.text.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    if line.startswith("#EXTINF:"):
                        pending_extinf = line
                    elif line.startswith("#"):
                        if pending_extinf is None and not entries:
                            header.append(line)
                    elif pending_extinf:
                        source_url = urljoin(session["playback_url"], line)
                        match = re.search(r"/(\d+)\.ts$", urlparse(source_url).path)
                        if not match:
                            continue
                        duration_match = re.search(r"#EXTINF:([0-9.]+)", pending_extinf)
                        duration = float(duration_match.group(1)) if duration_match else 0.0
                        entries.append((int(match.group(1)), pending_extinf, duration, source_url))
                        pending_extinf = None
                if entries:
                    session["playlist_header"] = header
                    semaphore = asyncio.Semaphore(4)

                    async def download_entry(entry):
                        sequence, extinf, duration, source_url = entry
                        digest = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:12]
                        path = directory / f"{sequence}-{digest}.ts"
                        cached = session.setdefault("cached_segments", {}).get(sequence)
                        if cached and Path(cached["path"]).is_file():
                            return None
                        async with semaphore:
                            segment_response = await client.get(source_url)
                            segment_response.raise_for_status()
                            temporary = path.with_suffix(".ts.tmp")
                            await asyncio.to_thread(temporary.write_bytes, segment_response.content)
                            await asyncio.to_thread(temporary.replace, path)
                        return sequence, {
                            "path": str(path),
                            "extinf": extinf,
                            "duration": duration,
                            "cached_at": time.time(),
                        }

                    results = await asyncio.gather(
                        *(download_entry(entry) for entry in entries),
                        return_exceptions=True,
                    )
                    cached_segments = session.setdefault("cached_segments", {})
                    for result in results:
                        if isinstance(result, tuple):
                            cached_segments[result[0]] = result[1]
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await recover_stream_session(stream_id, session)
            except Exception:
                pass
        cleanup_channel_cache(stream_id)
        await asyncio.sleep(settings.hls_cache_poll_seconds)


@asynccontextmanager
async def lifespan(app):
    init_db()
    try:
        yield
    finally:
        tasks = list(CACHE_TASKS.values())
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


def import_playlist():
    if not PLAYLIST_PATH.is_file():
        raise FileNotFoundError(f"Файл каталога не найден: {PLAYLIST_PATH}")

    content = PLAYLIST_PATH.read_text(
        encoding="utf-8-sig",
        errors="replace",
    )

    imported = 0
    invalid = 0
    current_name = None

    with connect() as db:
        for line in content.splitlines():
            line = line.strip()

            if line.startswith("#EXTINF:"):
                current_name = line.split(",", 1)[1].strip() if "," in line else None
                continue

            if not line or line.startswith("#"):
                continue

            match = re.search(r"infohash=([a-fA-F0-9]{40})", line)

            if not match or not current_name:
                invalid += 1
                current_name = None
                continue

            infohash = match.group(1).lower()

            db.execute(
                """
                INSERT INTO streams
                    (infohash, name, categories, availability, status)
                VALUES (?, ?, '', 0, 0)
                ON CONFLICT(infohash) DO UPDATE SET
                    name = excluded.name
                """,
                (infohash, current_name[:300]),
            )

            imported += 1
            current_name = None

    return {
        "ok": True,
        "imported": imported,
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
</style>
</head>
<body>
<h1>P2P Stream Server</h1>
<p class="small">AceStream → HLS · задержка {settings.default_buffer_seconds} сек. · Ubuntu 24.04 · v0.3.0</p>

<section>
<h2>Каталог каналов</h2>
<button id="import">Импортировать каталог</button>
<p id="import-result" class="small">Каталог ещё не импортирован.</p>
</section>

<section>
<h2>Поиск каналов</h2>
<form id="f">
<input id="q" placeholder="Название канала или часть названия" required>
<button>Искать</button>
</form>
<pre id="out">Введите запрос.</pre>
</section>

<section>
<h2>Состояние Engine</h2>
<pre id="health">Проверка...</pre>
</section>

<script>
async function health(){
    try{
        let r=await fetch('/api/health');
        document.querySelector('#health').textContent=
            JSON.stringify(await r.json(),null,2);
    }catch(e){
        document.querySelector('#health').textContent=String(e);
    }
}

document.querySelector('#import').onclick=async()=>{
    let out=document.querySelector('#import-result');
    out.textContent='Импорт выполняется...';
    try{
        let r=await fetch('/api/catalog/import',{method:'POST'});
        let data=await r.json();
        out.textContent=JSON.stringify(data,null,2);
    }catch(e){
        out.textContent=String(e);
    }
};

document.querySelector('#f').onsubmit=async e=>{
    e.preventDefault();
    let out=document.querySelector('#out');
    out.textContent='Поиск...';
    try{
        let r=await fetch('/api/search?q='+encodeURIComponent(
            document.querySelector('#q').value
        ));
        out.textContent=JSON.stringify(await r.json(),null,2);
    }catch(e){
        out.textContent=String(e);
    }
};

health();
</script>
</body>
</html>""")


@app.get("/api/health")
async def health():
    try:
        return {"ok": True, "engine": await ace.health()}
    except Exception as exc:
        return {"ok": False, "engine_error": str(exc)}


@app.post("/api/catalog/import")
async def catalog_import():
    try:
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


@app.post("/api/streams/{stream_id}/start")
async def start_stream(stream_id: int):
    with connect() as db:
        row = db.execute("SELECT infohash FROM streams WHERE id=?", (stream_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Канал не найден")
    try:
        async with stream_lock(stream_id):
            result = await create_stream_session(stream_id, row["infohash"])
            session = ACTIVE_STREAMS[stream_id]
        ensure_cache_worker(stream_id, session)
        return {**result, "stream_url": f"/live/{stream_id}/index.m3u8"}
    except Exception as exc:
        raise HTTPException(502, f"Не удалось запустить AceStream: {exc}")


@app.get("/live/{stream_id}/index.m3u8", response_class=PlainTextResponse)
async def live_playlist(stream_id: int):
    session = ACTIVE_STREAMS.get(stream_id)
    if not session:
        raise HTTPException(404, "Поток не запущен. Сначала вызовите POST /api/streams/{id}/start")

    cleanup_channel_cache(stream_id)
    cached = session.get("cached_segments", {})
    entries = [
        (sequence, item)
        for sequence, item in sorted(cached.items())
        if Path(item["path"]).is_file()
    ]
    if len(entries) < 3:
        raise HTTPException(503, "Кэш ещё накапливает HLS-сегменты")

    # Убираем самые новые сегменты из выдачи, сохраняя задержку относительно live edge.
    visible = list(entries)
    removed_duration = 0.0
    while len(visible) > 3 and removed_duration < settings.default_buffer_seconds:
        _, newest = visible.pop()
        removed_duration += newest["duration"]
    if removed_duration < settings.default_buffer_seconds:
        raise HTTPException(
            503,
            f"Ожидание накопления {settings.default_buffer_seconds}-секундного буфера",
        )

    first_sequence = visible[0][0]
    output = []
    media_sequence_written = False
    for line in session.get("playlist_header", []):
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            output.append(f"#EXT-X-MEDIA-SEQUENCE:{first_sequence}")
            media_sequence_written = True
        else:
            output.append(line)
    if not media_sequence_written:
        output.insert(1 if output else 0, f"#EXT-X-MEDIA-SEQUENCE:{first_sequence}")

    for sequence, item in visible:
        output.extend((item["extinf"], f"/live/{stream_id}/segment/{sequence}.ts"))

    return PlainTextResponse(
        "\\n".join(output) + "\\n",
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/live/{stream_id}/segment/{sequence}.ts")
async def live_segment(stream_id: int, sequence: int):
    session = ACTIVE_STREAMS.get(stream_id)
    if not session:
        raise HTTPException(404, "Поток не запущен")

    cleanup_channel_cache(stream_id)
    item = session.get("cached_segments", {}).get(sequence)
    if not item:
        raise HTTPException(404, "Сегмент отсутствует в пятиминутном кэше")
    path = Path(item["path"])
    if not path.is_file():
        raise HTTPException(404, "Срок хранения сегмента истёк")
    try:
        content = await asyncio.to_thread(path.read_bytes)
    except OSError as exc:
        raise HTTPException(500, f"Не удалось прочитать сегмент кэша: {exc}")
    return Response(
        content=content,
        media_type="video/mp2t",
        headers={"Cache-Control": "no-store"},
    )
