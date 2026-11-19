from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field
from app.acestream import AceStreamClient
from app.db import connect, init_db

ace = AceStreamClient()

@asynccontextmanager
async def lifespan(app):
    init_db()
    yield

app = FastAPI(title="P2P Stream Server", version="0.1.0", lifespan=lifespan)

class ImportItem(BaseModel):
    infohash: str = Field(min_length=20, max_length=80)
    name: str = Field(min_length=1, max_length=300)
    categories: list[str] = []
    availability: float = 0
    status: int = 0

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse("""<!doctype html><html lang="ru"><meta charset="utf-8"><title>P2P Stream Server</title>
    <style>body{background:#10151f;color:#e8edf5;font:16px system-ui;max-width:1000px;margin:40px auto;padding:0 20px}
    section{background:#1a2332;border:1px solid #2b374b;border-radius:14px;padding:20px;margin-top:20px}
    input,button{padding:12px;border-radius:8px;border:1px solid #3a4961;background:#111a28;color:white}
    button{background:#2563eb}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>
    <h1>P2P Stream Server</h1><p>AceStream → HLS · Ubuntu 24.04 · v0.1.0</p>
    <section><h2>Поиск AceStream</h2><form id="f"><input id="q" placeholder="sport, news, movie" required><button>Искать</button></form><pre id="out">Введите запрос.</pre></section>
    <section><h2>Состояние Engine</h2><pre id="health">Проверка...</pre></section>
    <script>
    async function health(){try{let r=await fetch('/api/health');document.querySelector('#health').textContent=JSON.stringify(await r.json(),null,2)}catch(e){document.querySelector('#health').textContent=String(e)}}
    document.querySelector('#f').onsubmit=async e=>{e.preventDefault();let o=document.querySelector('#out');o.textContent='Поиск...';try{let r=await fetch('/api/search?q='+encodeURIComponent(document.querySelector('#q').value));o.textContent=JSON.stringify(await r.json(),null,2)}catch(x){o.textContent=String(x)}};
    health();</script></html>""")

@app.get("/api/health")
async def health():
    try:
        return {"ok": True, "engine": await ace.health()}
    except Exception as exc:
        return {"ok": False, "engine_error": str(exc)}

@app.get("/api/search")
async def search(q: str = Query(min_length=1, max_length=200), category: str | None = None,
                 page: int = Query(0, ge=0), page_size: int = Query(50, ge=1, le=200)):
    try:
        return await ace.search(q, category, page, page_size)
    except Exception as exc:
        raise HTTPException(502, f"Ace Search недоступен: {exc}")

@app.post("/api/streams/import")
async def import_stream(item: ImportItem):
    with connect() as db:
        db.execute("INSERT INTO streams(infohash,name,categories,availability,status) VALUES(?,?,?,?,?) ON CONFLICT(infohash) DO UPDATE SET name=excluded.name,categories=excluded.categories,availability=excluded.availability,status=excluded.status",
            (item.infohash, item.name, ",".join(item.categories), item.availability, item.status))
    return {"ok": True, "infohash": item.infohash}

@app.get("/api/streams")
async def list_streams():
    with connect() as db:
        rows = db.execute("SELECT * FROM streams ORDER BY name COLLATE NOCASE").fetchall()
    return [dict(row) for row in rows]

@app.get("/playlist.m3u", response_class=PlainTextResponse)
async def playlist():
    with connect() as db:
        rows = db.execute("SELECT id,name FROM streams ORDER BY name COLLATE NOCASE").fetchall()
    lines = ["#EXTM3U"]
    for row in rows:
        lines += [f"#EXTINF:-1,{row['name']}", f"http://SERVER_IP:9999/live/{row['id']}/index.m3u8"]
    return "\n".join(lines) + "\n"

@app.post("/api/streams/{stream_id}/start")
async def start_stream(stream_id: int):
    with connect() as db:
        row = db.execute("SELECT infohash FROM streams WHERE id=?", (stream_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Канал не найден")
    try:
        return await ace.start_hls(row["infohash"])
    except Exception as exc:
        raise HTTPException(502, f"Не удалось запустить AceStream: {exc}")
