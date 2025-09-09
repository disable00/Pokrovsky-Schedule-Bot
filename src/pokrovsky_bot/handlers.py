import asyncio
import html
import re
from typing import List
from aiogram import F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from .config import settings
from .db import upsert_user, log_event
from .keyboard import MAIN_KB
from .models import SLink
from .parser import collapse_by_time, extract_schedule, pretty, grade_from_label
from .sheets import resolve_google_url, sheets_meta
from .site import get_links_from_site
from .state import (
    LINKS, DOC_URL, GID_BY_GRADE, MATRIX, STATE,
    kb_dates, kb_grades, kb_labels
)


async def show_loader(cb_or_msg, toast="Загружаю…", text="⚙️ Загружаю…") -> Message:
    if isinstance(cb_or_msg, CallbackQuery):
        try:
            await cb_or_msg.answer(toast, show_alert=False)
        except Exception:
            pass
        return await cb_or_msg.message.answer(text)
    return await cb_or_msg.answer(text)


async def replace_loader(loader: Message, text: str, **kw):
    try:
        await loader.edit_text(text, **kw)
    except Exception:
        try:
            await loader.answer(text, **kw)
        finally:
            try:
                await loader.delete()
            except Exception:
                pass


async def ensure_links():
    global LINKS
    if not LINKS:
        LINKS = await get_links_from_site()


async def cmd_start(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "cmd_start")
    await m.answer("Ищу расписания (площадка №1)...", reply_markup=MAIN_KB)
    await show_dates(m)


async def show_dates(m: Message):
    await ensure_links()
    if not LINKS:
        return await m.answer("Не нашёл ссылки в секции №1.", reply_markup=MAIN_KB)
    STATE[m.chat.id] = {"step": "dates"}
    await m.answer("Выбери дату:", reply_markup=kb_dates(LINKS))


async def on_main(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "click_main")
    await show_dates(m)


async def on_back(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "click_back")
    st = STATE.get(m.chat.id) or {}
    if st.get("step") in (None, "dates"):
        return await show_dates(m)
    if st.get("step") == "grades":
        return await show_dates(m)
    if st.get("step") == "classes":
        date = st.get("date")
        if not date:
            return await show_dates(m)
        return await ask_grades(m, date)
    if st.get("step") == "shown":
        date, gid, grade = st.get("date"), st.get("gid"), st.get("grade")
        if not (date and gid and grade is not None):
            return await show_dates(m)
        rows, labels, _hr, _cab = MATRIX.get((date, gid), (None, None, None, None))
        if rows is None:
            from .ensure import ensure_sheet_for_grade  # local import to avoid cycle
            rows, labels, hr, cab = (await ensure_sheet_for_grade(date, grade))[2]
            MATRIX[(date, gid)] = (rows, labels, hr, cab)
        ks = [L for L in labels if grade_from_label(L) == grade]
        await m.answer("Выбери класс:", reply_markup=kb_labels(date, gid, ks))
        STATE[m.chat.id] = {"step": "classes", "date": date, "gid": gid, "grade": grade}


async def on_news(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "click_news")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Открыть новостной канал", url=settings.NEWS_CHANNEL_URL)]])
    await m.answer("Наш новостной канал:", reply_markup=kb)


async def on_pick_date(c: CallbackQuery):
    upsert_user(c.from_user)
    idx = int(c.data.split(":", 1)[1])
    if idx < 0 or idx >= len(LINKS):
        return await c.answer()
    link = LINKS[idx]; log_event(c.from_user.id, "pick_date", link.date)
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю список классов…")
    try:
        g_url = await resolve_google_url(link.url)
    except Exception as e:
        return await replace_loader(loader, f"Не удалось найти Google Sheets: {e}")
    DOC_URL[link.date] = g_url
    from .db import sched_upsert
    sched_upsert(link.date, link.url, g_url)
    gid2title, _ = await sheets_meta(g_url)
    from .state import parse_class_label
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    grades = [g for g in quick.keys() if g and 5 <= g <= 11]
    if grades:
        GID_BY_GRADE[link.date] = {g: quick[g] for g in grades}
        await replace_loader(loader, f"Выбери номер класса ({link.date}):", reply_markup=kb_grades(link.date, grades))
    else:
        await replace_loader(loader, f"Выбери номер класса ({link.date}):", reply_markup=kb_grades(link.date, list(range(5, 12))))
    STATE[c.message.chat.id] = {"step": "grades", "date": link.date}


async def ask_grades(msg_target: Message, date: str):
    g_url = DOC_URL.get(date)
    from .db import sched_upsert
    from .sheets import sheets_meta
    from .site import get_links_from_site
    if not g_url:
        await ensure_links()
        link = next((l for l in LINKS if l.date == date), None)
        if not link:
            return await msg_target.answer("Не нашёл такую дату.", reply_markup=MAIN_KB)
        g_url = await resolve_google_url(link.url); DOC_URL[date] = g_url; sched_upsert(date, link.url, g_url)
    gid2title, _ = await sheets_meta(g_url)
    from .state import parse_class_label
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    grades = [g for g in quick.keys() if g and 5 <= g <= 11]
    if grades:
        GID_BY_GRADE[date] = {g: quick[g] for g in grades}
        await msg_target.answer(f"Выбери номер класса ({date}):", reply_markup=kb_grades(date, grades))
    else:
        await msg_target.answer(f"Выбери номер класса ({date}):", reply_markup=kb_grades(date, list(range(5, 12))))


async def on_pick_grade(c: CallbackQuery):
    upsert_user(c.from_user)
    _, date, gs = c.data.split(":", 2)
    grade = int(gs); log_event(c.from_user.id, "pick_grade", f"{date}|{grade}")
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю расписание…")
    try:
        from .ensure import ensure_sheet_for_grade
        _g_url, gid, payload = await ensure_sheet_for_grade(date, grade)
    except Exception as e:
        return await replace_loader(loader, f"Не нашёл вкладку: {e}")
    rows, labels, _hr, _cab = payload
    ks = [L for L in labels if grade_from_label(L) == grade]
    await replace_loader(loader, "Выбери класс:", reply_markup=kb_labels(date, gid, ks))
    STATE[c.message.chat.id] = {"step": "classes", "date": date, "gid": gid, "grade": grade}


async def on_pick_label(c: CallbackQuery):
    upsert_user(c.from_user)
    _, date, gid, klass = c.data.split(":", 3)
    log_event(c.from_user.id, "pick_class", f"{date}|{klass}")
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю расписание…")

    if (date, gid) not in MATRIX:
        try:
            grade = int(re.match(r"(\d{1,2})", klass).group(1))
            from .ensure import ensure_sheet_for_grade
            _g_url, _gid, _payload = await ensure_sheet_for_grade(date, grade)
        except Exception as e:
            return await replace_loader(loader, f"Ошибка доступа к листу: {e}")

    rows, labels, headers, cab_map = MATRIX[(date, gid)]
    key = klass.upper()
    if key not in labels:
        return await replace_loader(loader, "Такой класс не нашёлся на листе.")
    items = collapse_by_time(extract_schedule(rows, labels, headers, key, cab_map.get(key, (None, 0))))
    await replace_loader(loader, pretty(date, key, items), parse_mode="HTML")
    STATE[c.message.chat.id] = {"step": "shown", "date": date, "gid": gid, "grade": grade_from_label(key), "klass": key}
    log_event(c.from_user.id, "show_schedule", f"{date}|{key}")


def is_admin(uid: int) -> bool:
    return uid == settings.ADMIN_ID


async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Доступ запрещён.")
    upsert_user(m.from_user)
    from .db import DB
    from .utils import fmt_msk

    tu = DB.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    te = DB.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    a24 = DB.execute("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= datetime('now','-1 day')").fetchone()[0]
    top = DB.execute("SELECT user_id, first_name, username, msg_count, last_seen FROM users ORDER BY msg_count DESC, last_seen DESC LIMIT 10").fetchall()
    last = DB.execute("SELECT e.ts, e.type, u.user_id, u.username, e.meta FROM events e JOIN users u USING(user_id) ORDER BY e.id DESC LIMIT 10").fetchall()

    def ulabel(r):
        uid, fn, un, cnt, ls = r
        tag = f"@{un}" if un else str(uid)
        return f"{tag} — {cnt} событий (посл.: {fmt_msk(ls)})"

    def eline(r):
        ts, et, uid, un, meta = r
        tag = f"@{un}" if un else str(uid)
        return f"{fmt_msk(ts)} · {et} · {tag} · {meta or ''}"

    msg = ["🛠 <b>Админ-панель</b>",
           f"👥 Пользователей: <b>{tu}</b>",
           f"📨 Событий: <b>{te}</b>",
           f"🟢 Активно за 24ч: <b>{a24}</b>",
           "",
           "🏆 <b>Топ 10 по активности</b>"]
    msg += [f"• {ulabel(r)}" for r in top] or ["— нет данных —"]
    msg += ["", "📝 <b>Последние 10 событий</b>"] + ([f"• {eline(r)}" for r in last] or ["— нет данных —"])
    await m.answer("".join(msg), parse_mode="HTML")
