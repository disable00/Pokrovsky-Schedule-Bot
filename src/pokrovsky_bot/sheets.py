import csv
import re
from io import StringIO
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup
from .http import fetch_text


def _rebuild(url: str, tail: str, extra: Dict[str, str]) -> str:
    u = urlparse(url)
    parts = u.path.split("/")
    sid = parts[parts.index("d") + 1]
    return urlunparse((u.scheme, u.netloc, f"/spreadsheets/d/{sid}/{tail}", "", urlencode(extra), ""))


def htmlview_url(url: str) -> str:
    return _rebuild(url, "htmlview", {})


def csv_url(url: str, gid: str) -> str:
    return _rebuild(url, "export", {"format": "csv", "gid": gid})


async def resolve_google_url(schedule_page_url: str) -> str:
    if "docs.google.com/spreadsheets" in schedule_page_url:
        return schedule_page_url
    soup = BeautifulSoup(await fetch_text(schedule_page_url), "html.parser")
    iframe = soup.find("iframe", src=lambda s: s and "docs.google.com/spreadsheets" in s)
    if iframe:
        return iframe["src"]
    a = soup.find("a", href=lambda s: s and "docs.google.com/spreadsheets" in s)
    if a:
        return a["href"]
    raise RuntimeError("Не нашли ссылку на Google Sheets.")


async def sheets_meta(google_url: str) -> Tuple[Dict[str, str], Set[str]]:
    html_text = await fetch_text(htmlview_url(google_url))
    soup = BeautifulSoup(html_text, "html.parser")
    gid2title: Dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        if "gid=" in a["href"]:
            gid = parse_qs(urlparse(a["href"]).query).get("gid", ["0"])[0]
            title = (a.get("aria-label") or a.get_text(" ", strip=True) or "").strip()
            if title:
                gid2title[gid] = title
    for m in re.finditer(r'"gid"\s*:\s*(\d+).*?"title"\s*:\s*"([^"]+)"', html_text, flags=re.DOTALL):
        gid2title.setdefault(m.group(1), m.group(2))
    for m in re.finditer(r'"sheetId"\s*:\s*(\d+).*?"title"\s*:\s*"([^"]+)"', html_text, flags=re.DOTALL):
        gid2title.setdefault(m.group(1), m.group(2))
    gids: Set[str] = set(
        re.findall(r"[?&]gid=(\d+)", html_text)
        + re.findall(r'data-gid="(\d+)"', html_text)
        + re.findall(r'gid\\?":\s*"?(\d+)"?', html_text)
    )
    if not gids:
        gids.add("0")
    return gid2title, gids


async def get_rows_from_csv(g_url: str, gid: str) -> List[List[str]]:
    text = await fetch_text(csv_url(g_url, gid))
    return [list(r) for r in csv.reader(StringIO(text))]
