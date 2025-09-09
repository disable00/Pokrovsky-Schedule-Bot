import hashlib
import random
import asyncio
from aiogram import Bot

from .db import sched_get_all, sched_upsert, hash_get, hash_set
from .sheets import resolve_google_url, sheets_meta, csv_url
from .site import get_links_from_site
from .http import fetch_text
from .utils import fmt_msk
from . import state  # <-- важно: работаем с одним модулем состояния


async def broadcast(bot: Bot, text: str):
    """Шлём всем пользователям (кто писал боту). Ошибки тихо игнорируем."""
    from .db import DB
    users = [row[0] for row in DB.execute("SELECT user_id FROM users").fetchall()]
    sem = asyncio.Semaphore(20)

    async def send(uid):
        async with sem:
            try:
                await bot.send_message(uid, text, disable_notification=True)
            except Exception:
                pass

    await asyncio.gather(*(send(uid) for uid in users))


async def check_once(bot: Bot):
    """Один проход: ищем новые даты и правки в таблицах."""
    try:
        links = await get_links_from_site()
    except Exception:
        return

    known = sched_get_all()

    # 1) Новые даты
    for l in links:
        if l.date not in known:
            try:
                g_url = await resolve_google_url(l.url)
            except Exception:
                g_url = None
            sched_upsert(l.date, l.url, g_url)
            await broadcast(bot, f"🆕 Появилось новое расписание на <b>{l.date}</b>")
            # кладём в общий кэш URL таблицы (мутируем общий dict)
            state.DOC_URL[l.date] = g_url or state.DOC_URL.get(l.date)

    # 2) Правки внутри таблиц
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
                tnow = fmt_msk(None)
                title = gid2title.get(gid, f"лист {gid}")
                await broadcast(
                    bot,
                    f"✏️ Обновлено расписание на <b>{date}</b> — внесены правки в лист «{title}»\n{tnow}"
                )

    # 3) Обновляем общий список ссылок для интерфейса (мутируем объект!)
    state.LINKS.clear()
    state.LINKS.extend(links or [])


async def watch_loop(bot: Bot):
    """Фоновый цикл: каждые ~5–10 минут"""
    await check_once(bot)
    while True:
        await asyncio.sleep(random.randint(300, 600))  # 5–10 мин
        await check_once(bot)
