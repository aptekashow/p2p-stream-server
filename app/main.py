from contextlib import asynccontextmanager
from pathlib import Path
import re

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from app.acestream import AceStreamClient
from app.db import connect, init_db

ace = AceStreamClient()
PLAYLIST_PATH = Path("/app/data/ace-stream-playlist.m3u8")


@asynccontextmanager
async def lifespan(app):
    init_db()
    yield


app = FastAPI(
    title="P2P Stream Server",
    version="0.2.0",
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
<p class="small">AceStream → HLS · Ubuntu 24.04 · v0.2.0</p>

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
async def playlist():
    with connect() as db:
        rows = db.execute(
            "SELECT id,name FROM streams ORDER BY name COLLATE NOCASE"
        ).fetchall()

    lines = ["#EXTM3U"]

    for row in rows:
        lines += [
            f"#EXTINF:-1,{row['name']}",
            f"http://SERVER_IP:9999/live/{row['id']}/index.m3u8",
        ]

    return "\n".join(lines) + "\n"


@app.post("/api/streams/{stream_id}/start")
async def start_stream(stream_id: int):
    with connect() as db:
        row = db.execute(
            "SELECT infohash FROM streams WHERE id=?",
            (stream_id,),
        ).fetchone()

    if not row:
        raise HTTPException(404, "Канал не найден")

    try:
        return await ace.start_hls(row["infohash"])
    except Exception as exc:
        raise HTTPException(502, f"Не удалось запустить AceStream: {exc}")
