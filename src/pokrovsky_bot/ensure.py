import asyncio
import csv
from io import StringIO
from typing import Dict, List, Optional, Set, Tuple
import aiohttp
from .config import HEADERS
from .db import sched_upsert
from .http import fetch_text
from .sheets import resolve_google_url, sheets_meta, csv_url
from .site import get_links_from_site
from .state import DOC_URL, GID_BY_GRADE, MATRIX, LINKS
from .parser import parse_headers, build_cab_map, grade_from_label

async def ensure_links():
    global LINKS
    if not LINKS:
        LINKS = await get_links_from_site()


async def get_rows_from_csv(g_url: str, gid: str) -> List[List[str]]:
    text = await fetch_text(csv_url(g_url, gid))
    return [list(r) for r in csv.reader(StringIO(text))]


async def ensure_sheet_for_grade(date: str, grade: int):
    g_url = DOC_URL.get(date)
    if not g_url:
        await ensure_links()
        link = next((l for l in LINKS if l.date == date), None)
        if not link:
            raise RuntimeError("Дата не найдена.")
        g_url = await resolve_google_url(link.url); DOC_URL[date] = g_url; sched_upsert(date, link.url, g_url)

    if date in GID_BY_GRADE and grade in GID_BY_GRADE[date]:
        gid = GID_BY_GRADE[date][grade]
        if (date, gid) not in MATRIX:
            rows = await get_rows_from_csv(g_url, gid)
            labels, headers = parse_headers(rows)
            MATRIX[(date, gid)] = (rows, labels, headers, build_cab_map(rows, labels, headers))
        return g_url, GID_BY_GRADE[date][grade], MATRIX[(date, GID_BY_GRADE[date][grade])]

    gid2title, gids = await sheets_meta(g_url)
    from .state import parse_class_label
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    if grade in quick and quick[grade]:
        gid = quick[grade]
        rows = await get_rows_from_csv(g_url, gid)
        labels, headers = parse_headers(rows)
        MATRIX[(date, gid)] = (rows, labels, headers, build_cab_map(rows, labels, headers))
        GID_BY_GRADE.setdefault(date, {})[grade] = gid
        return g_url, gid, MATRIX[(date, gid)]

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        sem = asyncio.Semaphore(6)
        async def try_gid(gid):
            async with sem:
                try:
                    from .sheets import csv_url as _csv_url
                    async with session.get(_csv_url(g_url, gid)) as r:
                        r.raise_for_status()
                        txt = await r.text()
                    rws = [list(r) for r in csv.reader(StringIO(txt))]
                    lm, hr = parse_headers(rws)
                    if grade in {grade_from_label(L) for L in lm}:
                        return gid, rws, lm, hr
                except Exception:
                    return None
        tasks = [asyncio.create_task(try_gid(g)) for g in (list(gids) or ["0"])]
        for t in asyncio.as_completed(tasks):
            res = await t
            if res:
                gid, rows, labels, headers = res
                MATRIX[(date, gid)] = (rows, labels, headers, build_cab_map(rows, labels, headers))
                GID_BY_GRADE.setdefault(date, {})[grade] = gid
                return g_url, gid, MATRIX[(date, gid)]
    raise RuntimeError("Не нашёл вкладку для выбранного номера класса.")
