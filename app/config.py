# -*- coding: utf-8 -*-
import os
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# Токен и прочее
TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN") or "PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE"
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))
NEWS_CHANNEL_URL = os.getenv("NEWS_CHANNEL_URL", "https://t.me/your_channel")

# Источник
PAGE_URL = "https://pokrovsky.gosuslugi.ru/glavnoe/raspisanie/"
HEADERS = {"User-Agent": "ScheduleBot/1.0"}

# DB
DB_PATH = os.getenv("DB_PATH", "bot_stats.sqlite3")

# Время
MSK = ZoneInfo("Europe/Moscow")
