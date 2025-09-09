from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

from .config import settings
from .db import ensure_db
from .subscription import SubscriptionMiddleware
from .handlers import (
    cmd_start, on_main, on_back, on_news,
    on_pick_date, on_pick_grade, on_pick_label, cmd_admin,
    on_check_subscription,
)


def build_bot_dp():
    if not settings.BOT_TOKEN or settings.BOT_TOKEN == "PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE":
        raise SystemExit("Вставьте токен бота в переменную окружения BOT_TOKEN (см. .env).")

    ensure_db()

    bot = Bot(
        settings.BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
            link_preview_is_disabled=True,
        ),
    )

    dp = Dispatcher()

    sub_mw = SubscriptionMiddleware(
        settings.NEWS_CHANNEL_ID,   
        settings.NEWS_CHANNEL_URL,  
        settings.ADMIN_ID           
    )
    dp.message.middleware(sub_mw)
    dp.callback_query.middleware(sub_mw)

    from aiogram.filters import Command

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(on_main, F.text.casefold() == "📅 посмотреть расписание".casefold())
    dp.message.register(on_back, F.text.casefold() == "⬅️ назад".casefold())
    dp.message.register(on_news, F.text.casefold() == "🔔 новостной канал".casefold())

    dp.callback_query.register(on_check_subscription, F.data == "check_sub")

    dp.callback_query.register(on_pick_date, F.data.startswith("d:"))
    dp.callback_query.register(on_pick_grade, F.data.startswith("g:"))
    dp.callback_query.register(on_pick_label, F.data.startswith("c:"))

    dp.message.register(cmd_admin, Command("admin"))

    async def on_startup():
        await bot.set_my_commands([
            BotCommand(command="start", description="Посмотреть расписание"),
        ])

    dp.startup.register(on_startup)

    return bot, dp
