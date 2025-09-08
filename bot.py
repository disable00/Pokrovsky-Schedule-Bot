# -*- coding: utf-8 -*-
from aiogram import Dispatcher, Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand
import asyncio

from app.config import TOKEN
from app.handlers.user import router as user_router
from app.handlers.admin import router as admin_router
from app.watcher import watch_loop


def main():
    if not TOKEN or TOKEN == "PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE":
        raise SystemExit("⚠️ Вставьте токен в app/config.py (TOKEN) или .env.")

    bot = Bot(
        TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
            link_preview_is_disabled=True,
        ),
    )
    dp = Dispatcher()

    # Роутеры
    dp.include_router(user_router)
    dp.include_router(admin_router)

    async def run():
        # Команда в меню
        await bot.set_my_commands([BotCommand(command="start", description="Посмотреть расписание")])

        # Фоновый наблюдатель
        asyncio.create_task(watch_loop(bot))

        await dp.start_polling(bot)

    asyncio.run(run())


if __name__ == "__main__":
    main()
