import re
from typing import Any, Dict, List, Optional, Set, Tuple

from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
)

SECTION_RX = re.compile(r"образовательная\s+площадка\s*№\s*(\d+)", re.IGNORECASE)
TITLE_RX   = re.compile(r"расписан\w*\s+урок\w*\s+на\s+(\d{2}\.\d{2})", re.IGNORECASE)
CLASS_LABEL_RX = re.compile(r"(\d{1,2})\s*([^\d\s][^\d]*)", re.UNICODE)
TIME_RX = re.compile(r"\d{1,2}[:.]\d{2}\s*[-–—]\s*\d{1,2}[:.]\d{2}")
CLASS_PURE_RX = re.compile(r'^\s*\d{1,2}\s*[A-Za-zА-Яа-яЁё]{1,6}\s*$')
EXCLUDE_SUBSTRINGS = {"начальная школа"}

LINKS: List[Any] = []
DOC_URL: Dict[str, str] = {}
GID_BY_GRADE: Dict[str, Dict[int, str]] = {}
ALL_GIDS: Dict[str, Set[str]] = {}
MATRIX: Dict[Tuple[str, str], Tuple[Any, Any, Any, Any]] = {}
STATE: Dict[int, Dict[str, Any]] = {}

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Посмотреть расписание")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="🔔 Новостной канал")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="Выберите действие…"
)


def parse_class_label(cell: str) -> Optional[str]:
    from .utils import norm
    s = norm(cell).upper().replace("Ё", "Е")
    m = CLASS_LABEL_RX.search(s)
    if not m:
        return None
    import re as _re
    suf = _re.sub(r"[ \-\d]+", "", m.group(2))
    return f"{m.group(1)}{suf}" if suf else None


def grade_from_label(label: str) -> Optional[int]:
    m = re.match(r"(\d{1,2})", label)
    return int(m.group(1)) if m else None


def kb_dates(links: List[Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=l.date, callback_data=f"d:{i}")] for i, l in enumerate(links)]
    )


def kb_grades(date: str, grades: List[int]) -> InlineKeyboardMarkup:
    grades = sorted({g for g in grades if 5 <= g <= 11}) or list(range(5, 12))
    rows, row = [], []
    for g in grades:
        row.append(InlineKeyboardButton(text=f"{g} класс", callback_data=f"g:{date}:{g}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_labels(date: str, gid: str, labels: List[str]) -> InlineKeyboardMarkup:
    import re as _re
    if not labels:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет классов", callback_data="noop")]])
    base = int(_re.match(r"(\d{1,2})", labels[0]).group(1))
    suffixes = sorted({_re.sub(r"^\d{1,2}", "", L) for L in labels if grade_from_label(L) == base})
    rows, row = [], []
    for suf in suffixes:
        row.append(InlineKeyboardButton(text=suf, callback_data=f"c:{date}:{gid}:{base}{suf}"))
        if len(row) == 4:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
