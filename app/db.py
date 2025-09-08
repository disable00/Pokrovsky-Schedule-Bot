# -*- coding: utf-8 -*-
import sqlite3
from typing import Dict, Tuple, Optional, List
from datetime import datetime, timezone
from app.config import DB_PATH, MSK

DB: Optional[sqlite3.Connection] = None


def ensure_db():
    global DB
    if DB:
        return DB
    DB = sqlite3.connect(DB_PATH, check_same_thread=False)
    DB.execute("PRAGMA journal_mode=WAL;")
    DB.executescript(
        '''
        CREATE TABLE IF NOT EXISTS users(
          user_id INTEGER PRIMARY KEY,
          first_name TEXT,
          username TEXT,
          joined_at TEXT,
          last_seen TEXT,
          msg_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          ts TEXT NOT NULL,
          type TEXT NOT NULL,
          meta TEXT,
          FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS schedules(
          date_label TEXT PRIMARY KEY,
          link_url   TEXT NOT NULL,
          google_url TEXT,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sheet_hashes(
          date_label TEXT NOT NULL,
          gid        TEXT NOT NULL,
          title      TEXT,
          hash       TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY(date_label, gid)
        );
        '''
    )
    DB.commit()
    return DB


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fmt_msk(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        msk = dt.astimezone(MSK)
        return f"{msk:%d.%m.%Y} ({msk:%H:%M} MSK)"
    except Exception:
        return str(iso)

def upsert_user(user):
    db = ensure_db()
    uid, first, uname = user.id, (user.first_name or "").strip(), (user.username or "").strip()
    if db.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone():
        db.execute(
            "UPDATE users SET first_name=?, username=?, last_seen=?, msg_count=msg_count+1 WHERE user_id=?",
            (first, uname, now_utc(), uid),
        )
    else:
        db.execute(
            "INSERT INTO users(user_id, first_name, username, joined_at, last_seen, msg_count) VALUES (?,?,?,?,?,1)",
            (uid, first, uname, now_utc(), now_utc()),
        )
    db.commit()

def log_event(uid: int, t: str, meta: str = ""):
    db = ensure_db()
    db.execute("INSERT INTO events(user_id, ts, type, meta) VALUES (?,?,?,?)", (uid, now_utc(), t, meta))
    db.commit()


def all_user_ids() -> List[int]:
    db = ensure_db()
    return [row[0] for row in db.execute("SELECT user_id FROM users").fetchall()]

def sched_get_all() -> Dict[str, Tuple[str, Optional[str]]]:
    db = ensure_db()
    cur = db.execute("SELECT date_label, link_url, google_url FROM schedules")
    return {d: (lu, gu) for d, lu, gu in cur.fetchall()}


def sched_upsert(date_label: str, link_url: str, google_url: Optional[str]):
    db = ensure_db()
    if db.execute("SELECT 1 FROM schedules WHERE date_label=?", (date_label,)).fetchone():
        db.execute("UPDATE schedules SET link_url=?, google_url=? WHERE date_label=?", (link_url, google_url, date_label))
    else:
        db.execute(
            "INSERT INTO schedules(date_label, link_url, google_url, created_at) VALUES (?,?,?,?)",
            (date_label, link_url, google_url, now_utc()),
        )
    db.commit()


def hash_get(date_label: str, gid: str) -> Optional[str]:
    db = ensure_db()
    row = db.execute("SELECT hash FROM sheet_hashes WHERE date_label=? AND gid=?", (date_label, gid)).fetchone()
    return row[0] if row else None


def hash_set(date_label: str, gid: str, title: str, h: str):
    db = ensure_db()
    if db.execute("SELECT 1 FROM sheet_hashes WHERE date_label=? AND gid=?", (date_label, gid)).fetchone():
        db.execute(
            "UPDATE sheet_hashes SET title=?, hash=?, updated_at=? WHERE date_label=? AND gid=?",
            (title, h, now_utc(), date_label, gid),
        )
    else:
        db.execute(
            "INSERT INTO sheet_hashes(date_label, gid, title, hash, updated_at) VALUES (?,?,?,?,?)",
            (date_label, gid, title, h, now_utc()),
        )
    db.commit()
