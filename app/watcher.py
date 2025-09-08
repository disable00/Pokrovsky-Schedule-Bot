# -*- coding: utf-8 -*-
import asyncio, random, hashlib, html
from typing import Optional
from aiogram import Bot

from app.parsers.site import get_links_from_site
from app.parsers.sheets import resolve_google_url, sheets_meta, csv_url
from app.http import fetch_text
from app.db import (
    ensure_db, sched_get_all, sched_upsert,
    hash_get, hash_set, all_user_ids, fmt_msk, now_utc
)
from app.schedule import LINKS, DOC_URL

db = ensure_db()

async def broadcast(bot: Bot, text: str):
    users = all_user_ids()
    sem = asyncio.Semaphore(20)
    async def send(uid: int):
        async with sem:
            try:
                await bot.send_message(uid, text, disable_notification=True)
            except Exception:
                pass
    await asyncio.gather(*(send(u) for u in users))

async def check_once(bot: Bot):
    try:
        links = await get_links_from_site()
    except Exception:
        return
    known = sched_get_all()

    # Новые даты
    for l in links:
        if l.date not in known:
            try:
                g_url: Optional[str] = await resolve_google_url(l.url)
            except Exception:
                g_url = None
            sched_upsert(l.date, l.url, g_url)
            DOC_URL[l.date] = g_url or DOC_URL.get(l.date, "")
            await broadcast(bot, f"🆕 Появилось новое расписание на <b>{l.date}</b>")

    # Правки внутри таблиц
    for date, (link_url, g_url) in sched_get_all().items():
        if not g_url:
            try:
                g_url = await resolve_google_url(link_url)
                sched_upsert(date, link_url, g_url)
            except Exception:
                continue
        try:
            gid2title, gids = await sheets_meta(g_url)
        except Exception:
            continue

        for gid in (gid2title.keys() or gids):
            try:
                csv_text = await fetch_text(csv_url(g_url, gid))
            except Exception:
                continue
            h = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
            old = hash_get(date, gid)
            if old is None:
                hash_set(date, gid, gid2title.get(gid, ""), h)
            elif old != h:
                hash_set(date, gid, gid2title.get(gid, ""), h)
                title = gid2title.get(gid, f"лист {gid}")
                await broadcast(
                    bot,
                    f"✏️ Обновлено расписание на <b>{date}</b> — внесены правки в лист «{html.escape(title)}»\n{fmt_msk(now_utc())}",
                )

    # Обновим кэш дат в памяти
    if links:
        LINKS[:] = links

async def watch_loop(bot: Bot):
    await check_once(bot)  # первый прогон сразу
    while True:
        await asyncio.sleep(random.randint(300, 600))  # 5–10 мин
        await check_once(bot)
