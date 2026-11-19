from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import re

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
import httpx

from app.config import settings
from pydantic import BaseModel, Field

from app.acestream import AceStreamClient
from app.db import connect, init_db

ace = AceStreamClient()
PLAYLIST_PATH = Path("/app/data/ace-stream-playlist.m3u8")
ACTIVE_STREAMS = {}
STREAM_LOCKS = {}


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
        recovered = {
            "infohash": failed_session["infohash"],
            "playback_url": playback_url,
            "segments": {},
            "recoveries": failed_session.get("recoveries", 0) + 1,
        }
        ACTIVE_STREAMS[stream_id] = recovered
        return recovered


@asynccontextmanager
async def lifespan(app):
    init_db()
    yield


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
        return {**result, "stream_url": f"/live/{stream_id}/index.m3u8"}
    except Exception as exc:
        raise HTTPException(502, f"Не удалось запустить AceStream: {exc}")


@app.get("/live/{stream_id}/index.m3u8", response_class=PlainTextResponse)
async def live_playlist(stream_id: int):
    session = ACTIVE_STREAMS.get(stream_id)
    if not session:
        raise HTTPException(404, "Поток не запущен. Сначала вызовите POST /api/streams/{id}/start")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            upstream = await client.get(session["playback_url"])
            upstream.raise_for_status()
    except httpx.HTTPError as first_error:
        try:
            session = await recover_stream_session(stream_id, session)
            if not session:
                raise RuntimeError("Сессия потока отсутствует после восстановления")
            async with httpx.AsyncClient(timeout=15) as client:
                upstream = await client.get(session["playback_url"])
                upstream.raise_for_status()
        except Exception as recovery_error:
            raise HTTPException(
                502,
                f"Плейлист AceStream недоступен; автоматическое восстановление не удалось. "
                f"Первичная ошибка: {first_error}; ошибка восстановления: {recovery_error}",
            )

    header, entries, pending_extinf = [], [], None
    for raw_line in upstream.text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            pending_extinf = line
        elif line.startswith("#"):
            if not entries:
                header.append(line)
        elif pending_extinf:
            match = re.search(r"^(?:https?://[^/]+)?/hls/c/.+/(\d+)\.ts(?:\?.*)?$", line)
            if not match:
                raise HTTPException(502, f"Неизвестный адрес HLS-сегмента: {line}")
            sequence = int(match.group(1))
            duration_match = re.search(r"#EXTINF:([0-9.]+)", pending_extinf)
            duration = float(duration_match.group(1)) if duration_match else 0.0
            entries.append((pending_extinf, sequence, duration, line))
            pending_extinf = None

    if len(entries) < 3:
        raise HTTPException(503, "AceStream пока не подготовил достаточно HLS-сегментов")

    removed_duration, keep_count = 0.0, len(entries)
    while keep_count > 3 and removed_duration < settings.default_buffer_seconds:
        keep_count -= 1
        removed_duration += entries[keep_count][2]
    if removed_duration < settings.default_buffer_seconds:
        raise HTTPException(503, f"Ожидание накопления {settings.default_buffer_seconds}-секундного буфера")

    kept_entries = entries[:keep_count]
    first_sequence = kept_entries[0][1]
    session["segments"].update({seq: url for _, seq, _, url in kept_entries})
    session["segments"] = {seq: url for seq, url in session["segments"].items() if seq >= first_sequence}

    output, media_sequence_updated = [], False
    for line in header:
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            output.append(f"#EXT-X-MEDIA-SEQUENCE:{first_sequence}")
            media_sequence_updated = True
        else:
            output.append(line)
    if not media_sequence_updated:
        output.insert(1, f"#EXT-X-MEDIA-SEQUENCE:{first_sequence}")
    for extinf, sequence, _, _ in kept_entries:
        output.extend((extinf, f"/live/{stream_id}/segment/{sequence}.ts"))

    return PlainTextResponse("\n".join(output) + "\n", media_type="application/vnd.apple.mpegurl", headers={"Cache-Control": "no-store"})


@app.get("/live/{stream_id}/segment/{sequence}.ts")
async def live_segment(stream_id: int, sequence: int):
    session = ACTIVE_STREAMS.get(stream_id)
    if not session:
        raise HTTPException(404, "Поток не запущен")
    upstream_url = session["segments"].get(sequence)
    if not upstream_url:
        raise HTTPException(404, "Сегмент устарел или отсутствует. Обновите HLS-плейлист")
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            upstream = await client.get(upstream_url)
            upstream.raise_for_status()
    except httpx.HTTPError as exc:
        if getattr(exc, "response", None) is not None and exc.response.status_code in (404, 410, 500, 502, 503):
            try:
                await recover_stream_session(stream_id, session)
            except Exception as recovery_error:
                raise HTTPException(
                    502,
                    f"Сегмент недоступен, автоматическое восстановление не удалось: {recovery_error}",
                )
            raise HTTPException(
                503,
                "P2P-сессия автоматически перезапущена. Обновите HLS-плейлист для продолжения воспроизведения.",
            )
        raise HTTPException(502, f"Ошибка получения HLS-сегмента AceStream: {exc}")
    return Response(content=upstream.content, media_type="video/mp2t", headers={"Cache-Control": "no-store"})
