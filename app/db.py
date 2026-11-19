import sqlite3
from pathlib import Path
from app.config import settings

DB_PATH = settings.database_url.replace("sqlite:///", "")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

def connect():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    with connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS streams (id INTEGER PRIMARY KEY AUTOINCREMENT, infohash TEXT NOT NULL UNIQUE, name TEXT NOT NULL, categories TEXT DEFAULT '', availability REAL DEFAULT 0, status INTEGER DEFAULT 0, added_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
