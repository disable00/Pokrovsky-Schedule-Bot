# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List
from bs4 import BeautifulSoup
import re

from app.config import PAGE_URL
from app.http import fetch_text

@dataclass
class SLink:
    title: str
    url: str
    date: str  # 'dd.mm'

SECTION_RX = re.compile(r"образовательная\s+площадка\s*№\s*(\d+)", re.IGNORECASE)
TITLE_RX   = re.compile(r"расписан\w*\s+урок\w*\s+на\s+(\d{2}\.\d{2})", re.IGNORECASE)
EXCLUDE = {"начальная школа"}

def _norm(s: str) -> str:
    return " ".join((s or "").split()).strip()

async def get_links_from_site() -> List[SLink]:
    """Возвращает ссылки на расписание для площадки №1 (без начальной школы)."""
    soup = BeautifulSoup(await fetch_text(PAGE_URL), "html.parser")
    cur, out = None, []
    for el in soup.find_all(True):
        text = _norm(el.get_text(" ", strip=True))
        m = SECTION_RX.search(text)
        if m:
            cur = int(m.group(1)) if m.group(1).isdigit() else None
            continue
        if cur != 1:
            continue
        for a in el.find_all("a", href=True):
            title = _norm(a.get_text(" ", strip=True))
            if any(x in title.lower() for x in EXCLUDE):
                continue
            m2 = TITLE_RX.search(title)
            if m2:
                out.append(SLink(title=title, url=a["href"], date=m2.group(1)))
    uniq = {(l.title, l.url): l for l in out}
    res = list(uniq.values())
    res.sort(key=lambda k: tuple(map(int, k.date.split(".")[::-1])), reverse=True)
    return res
