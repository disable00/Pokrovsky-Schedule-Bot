import asyncio
from .bot import build_bot_dp
from .watcher import watch_loop


def main():
    bot, dp = build_bot_dp()

    async def run():
        import asyncio as _asyncio
        _asyncio.create_task(watch_loop(bot))
        await dp.start_polling(bot)

    asyncio.run(run())


if __name__ == "__main__":
    main()
