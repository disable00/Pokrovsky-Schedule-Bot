# -*- coding: utf-8 -*-
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from typing import List
import re

from app.config import NEWS_CHANNEL_URL

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Посмотреть расписание")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="🔔 Новостной канал")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="Выберите действие…",
)

def kb_news_link() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть новостной канал", url=NEWS_CHANNEL_URL)]]
    )

def kb_dates(labels: List[str]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=lbl, callback_data=f"d:{i}")] for i, lbl in enumerate(labels)]
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
    if not labels:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет классов", callback_data="noop")]])
    base = int(re.match(r"(\d{1,2})", labels[0]).group(1))
    suffixes = sorted({re.sub(r"^\d{1,2}", "", L) for L in labels})
    rows, row = [], []
    for suf in suffixes:
        row.append(InlineKeyboardButton(text=suf, callback_data=f"c:{date}:{gid}:{base}{suf}"))
        if len(row) == 4:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)
