# -*- coding: utf-8 -*-
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from app.db import ensure_db, upsert_user, log_event
from app.ui.keyboards import MAIN_KB, kb_dates, kb_grades, kb_labels, kb_news_link
from app.ui.messages import show_loader, replace_loader
from app.parsers.site import SLink
from app.parsers.sheets import resolve_google_url, sheets_meta
from app.schedule import (
    LINKS, ensure_links, DOC_URL, GID_BY_GRADE, MATRIX, STATE,
    grade_from_label, parse_class_label,
    ensure_sheet_for_grade, collapse_by_time, extract_schedule, pretty
)

router = Router()
ensure_db()  # инициализируем БД сразу


@router.message(Command("start"))
async def cmd_start(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "cmd_start")
    await m.answer("Ищу расписания (площадка №1)...", reply_markup=MAIN_KB)
    await show_dates(m)


@router.message(F.text.casefold() == "📅 посмотреть расписание".casefold())
async def on_main(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "click_main")
    await show_dates(m)


@router.message(F.text.casefold() == "⬅️ назад".casefold())
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
            rows, labels, hr, cab = (await ensure_sheet_for_grade(date, grade))[2]
            MATRIX[(date, gid)] = (rows, labels, hr, cab)
        ks = [L for L in labels if grade_from_label(L) == grade]
        await m.answer("Выбери класс:", reply_markup=kb_labels(date, gid, ks))
        STATE[m.chat.id] = {"step": "classes", "date": date, "gid": gid, "grade": grade}


@router.message(F.text.casefold() == "🔔 новостной канал".casefold())
async def on_news(m: Message):
    upsert_user(m.from_user); log_event(m.from_user.id, "click_news")
    await m.answer("Наш новостной канал:", reply_markup=kb_news_link())


async def show_dates(m: Message):
    links = await ensure_links()
    if not links:
        return await m.answer("Не нашёл ссылки в секции №1.", reply_markup=MAIN_KB)
    STATE[m.chat.id] = {"step": "dates"}
    await m.answer("Выбери дату:", reply_markup=kb_dates([l.date for l in links]))


@router.callback_query(F.data.startswith("d:"))
async def on_pick_date(c: CallbackQuery):
    upsert_user(c.from_user)
    idx = int(c.data.split(":", 1)[1])
    if idx < 0 or idx >= len(LINKS):
        return await c.answer()
    link: SLink = LINKS[idx]
    log_event(c.from_user.id, "pick_date", link.date)

    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю список классов…")
    try:
        g_url = await resolve_google_url(link.url)
    except Exception as e:
        return await replace_loader(loader, f"Не удалось найти Google Sheets: {e}")

    DOC_URL[link.date] = g_url
    gid2title, _ = await sheets_meta(g_url)
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    grades = [g for g in quick.keys() if g and 5 <= g <= 11]
    if grades:
        GID_BY_GRADE[link.date] = {g: quick[g] for g in grades}
        await replace_loader(loader, f"Выбери номер класса ({link.date}):", reply_markup=kb_grades(link.date, grades))
    else:
        await replace_loader(loader, f"Выбери номер класса ({link.date}):", reply_markup=kb_grades(link.date, list(range(5, 12))))
    STATE[c.message.chat.id] = {"step": "grades", "date": link.date}


async def ask_grades(msg: Message, date: str):
    g_url = DOC_URL.get(date)
    if not g_url:
        links = await ensure_links()
        link = next((l for l in links if l.date == date), None)
        if not link:
            return await msg.answer("Не нашёл такую дату.", reply_markup=MAIN_KB)
        from app.parsers.sheets import resolve_google_url
        g_url = await resolve_google_url(link.url)
        DOC_URL[date] = g_url

    from app.parsers.sheets import sheets_meta
    gid2title, _ = await sheets_meta(g_url)
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    grades = [g for g in quick.keys() if g and 5 <= g <= 11]
    if grades:
        GID_BY_GRADE[date] = {g: quick[g] for g in grades}
        await msg.answer(f"Выбери номер класса ({date}):", reply_markup=kb_grades(date, grades))
    else:
        await msg.answer(f"Выбери номер класса ({date}):", reply_markup=kb_grades(date, list(range(5, 12))))


@router.callback_query(F.data.startswith("g:"))
async def on_pick_grade(c: CallbackQuery):
    upsert_user(c.from_user)
    _, date, gs = c.data.split(":", 2)
    grade = int(gs)
    log_event(c.from_user.id, "pick_grade", f"{date}|{grade}")
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю расписание…")
    try:
        _g_url, gid, payload = await ensure_sheet_for_grade(date, grade)
    except Exception as e:
        return await replace_loader(loader, f"Не нашёл вкладку: {e}")
    rows, labels, _hr, _cab = payload
    ks = [L for L in labels if grade_from_label(L) == grade]
    await replace_loader(loader, "Выбери класс:", reply_markup=kb_labels(date, gid, ks))
    STATE[c.message.chat.id] = {"step": "classes", "date": date, "gid": gid, "grade": grade}


@router.callback_query(F.data.startswith("c:"))
async def on_pick_label(c: CallbackQuery):
    upsert_user(c.from_user)
    _, date, gid, klass = c.data.split(":", 3)
    log_event(c.from_user.id, "pick_class", f"{date}|{klass}")
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю расписание…")
    # ensure cache
    if (date, gid) not in MATRIX:
        try:
            import re as _re
            grade = int(_re.match(r"(\d{1,2})", klass).group(1))
            await ensure_sheet_for_grade(date, grade)
        except Exception as e:
            return await replace_loader(loader, f"Ошибка доступа к листу: {e}")
    rows, labels, headers, cab_map = MATRIX[(date, gid)]
    key = klass.upper()
    if key not in labels:
        return await replace_loader(loader, "Такой класс не нашёлся на листе.")
    items = collapse_by_time(extract_schedule(rows, labels, headers, key, cab_map.get(key, (None, 0))))
    await replace_loader(loader, pretty(date, key, items))
    STATE[c.message.chat.id] = {"step": "shown", "date": date, "gid": gid, "grade": grade_from_label(key), "klass": key}
    log_event(c.from_user.id, "show_schedule", f"{date}|{key}")
