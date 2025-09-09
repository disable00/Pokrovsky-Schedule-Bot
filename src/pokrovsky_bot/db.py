import sqlite3
from typing import Optional, Tuple, Dict
from .config import settings
from .utils import fmt_msk

DB: Optional[sqlite3.Connection] = None


def ensure_db():
    global DB
    DB = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
    DB.execute("PRAGMA journal_mode=WAL;")
    DB.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,
      joined_at TEXT, last_seen TEXT, msg_count INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
      ts TEXT NOT NULL, type TEXT NOT NULL, meta TEXT,
      FOREIGN KEY(user_id) REFERENCES users(user_id));
    -- список известных расписаний (по датам)
    CREATE TABLE IF NOT EXISTS schedules(
      date_label TEXT PRIMARY KEY,         -- '08.09'
      link_url   TEXT NOT NULL,
      google_url TEXT,
      created_at TEXT NOT NULL
    );
    -- хэши листов (чтобы видеть правки)
    CREATE TABLE IF NOT EXISTS sheet_hashes(
      date_label TEXT NOT NULL,
      gid        TEXT NOT NULL,
      title      TEXT,
      hash       TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(date_label, gid)
    );
    """); DB.commit()


def now_utc() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def upsert_user(u):
    if DB is None:
        return
    uid, first, uname = u.id, (u.first_name or "").strip(), (u.username or "").strip()
    if DB.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone():
        DB.execute("UPDATE users SET first_name=?, username=?, last_seen=?, msg_count=msg_count+1 WHERE user_id=?",
                   (first, uname, now_utc(), uid))
    else:
        DB.execute("INSERT INTO users(user_id,first_name,username,joined_at,last_seen,msg_count) VALUES (?,?,?,?,?,1)",
                   (uid, first, uname, now_utc(), now_utc()))
    DB.commit()


def log_event(uid: int, t: str, meta: str = ""):
    if DB is None:
        return
    DB.execute("INSERT INTO events(user_id,ts,type,meta) VALUES (?,?,?,?)", (uid, now_utc(), t, meta)); DB.commit()


def sched_get_all() -> Dict[str, Tuple[str, Optional[str]]]:
    """return {date_label: (link_url, google_url)}"""
    cur = DB.execute("SELECT date_label, link_url, google_url FROM schedules")
    return {d: (lu, gu) for d, lu, gu in cur.fetchall()}


def sched_upsert(date_label: str, link_url: str, google_url: Optional[str]):
    if DB.execute("SELECT 1 FROM schedules WHERE date_label=?", (date_label,)).fetchone():
        DB.execute("UPDATE schedules SET link_url=?, google_url=? WHERE date_label=?", (link_url, google_url, date_label))
    else:
        DB.execute("INSERT INTO schedules(date_label, link_url, google_url, created_at) VALUES (?,?,?,?)",
                   (date_label, link_url, google_url, now_utc()))
    DB.commit()


def hash_get(date_label: str, gid: str) -> Optional[str]:
    row = DB.execute("SELECT hash FROM sheet_hashes WHERE date_label=? AND gid=?", (date_label, gid)).fetchone()
    return row[0] if row else None


def hash_set(date_label: str, gid: str, title: str, h: str):
    if DB.execute("SELECT 1 FROM sheet_hashes WHERE date_label=? AND gid=?", (date_label, gid)).fetchone():
        DB.execute("UPDATE sheet_hashes SET title=?, hash=?, updated_at=? WHERE date_label=? AND gid=?",
                   (title, h, now_utc(), date_label, gid))
    else:
        DB.execute("INSERT INTO sheet_hashes(date_label, gid, title, hash, updated_at) VALUES (?,?,?,?,?)",
                   (date_label, gid, title, h, now_utc()))
    DB.commit()
