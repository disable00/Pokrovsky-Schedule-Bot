from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from app.config import ADMIN_ID
from app.db import ensure_db, fmt_msk

router = Router()
db = ensure_db()

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

@router.message(Command("admin"))
async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Доступ запрещён.")
    tu = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    te = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    a24 = db.execute("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= datetime('now','-1 day')").fetchone()[0]
    top = db.execute("SELECT user_id, first_name, username, msg_count, last_seen FROM users ORDER BY msg_count DESC, last_seen DESC LIMIT 10").fetchall()
    last = db.execute("SELECT e.ts, e.type, u.user_id, u.username, e.meta FROM events e JOIN users u USING(user_id) ORDER BY e.id DESC LIMIT 10").fetchall()

    def ulabel(r):
        uid, fn, un, cnt, ls = r
        tag = f"@{un}" if un else str(uid)
        return f"{tag} — {cnt} событий (посл.: {fmt_msk(ls)})"
    def eline(r):
        ts, et, uid, un, meta = r
        tag = f"@{un}" if un else str(uid)
        return f"{fmt_msk(ts)} · {et} · {tag} · {meta or ''}"

    msg = [
        "🛠 <b>Админ-панель</b>",
        f"👥 Пользователей: <b>{tu}</b>",
        f"📨 Событий: <b>{te}</b>",
        f"🟢 Активно за 24ч: <b>{a24}</b>",
        "",
        "🏆 <b>Топ 10 по активности</b>",
    ]
    msg += [f"• {ulabel(r)}" for r in top] or ["— нет данных —"]
    msg += ["", "📝 <b>Последние 10 событий</b>"] + ([f"• {eline(r)}" for r in last] or ["— нет данных —"])
    await m.answer("\n".join(msg))
