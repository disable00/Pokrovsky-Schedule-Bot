from typing import List
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from .config import settings
from .http import fetch_text
from .models import SLink
from .state import SECTION_RX, TITLE_RX, EXCLUDE_SUBSTRINGS
from .utils import norm


async def get_links_from_site() -> List[SLink]:
    PAGE_URL = settings.PAGE_URL
    soup = BeautifulSoup(await fetch_text(PAGE_URL), "html.parser")
    cur_section, out = None, []
    for el in soup.find_all(True):
        text = norm(el.get_text(" ", strip=True))
        m = SECTION_RX.search(text)
        if m:
            cur_section = int(m.group(1)) if m.group(1).isdigit() else None
            continue
        if cur_section != 1:
            continue
        for a in el.find_all("a", href=True):
            title = norm(a.get_text(" ", strip=True))
            if any(x in title.lower() for x in EXCLUDE_SUBSTRINGS):
                continue
            m2 = TITLE_RX.search(title)
            if m2:
                out.append(SLink(title=title, url=urljoin(PAGE_URL, a["href"]), date=m2.group(1)))
    uniq = {(l.title, l.url): l for l in out}
    res = list(uniq.values())
    def sort_key(x: SLink):
        dd, mm = x.date.split(".")
        return (int(mm), int(dd))
    res.sort(key=sort_key, reverse=True)
    return res
