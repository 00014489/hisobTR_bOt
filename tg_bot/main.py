from dotenv import load_dotenv
load_dotenv()
import sys
import asyncio
import logging
from aiogram.exceptions import TelegramNetworkError
from aiohttp import ClientConnectorError
import os
from app.handlers.common import router as common
# from app.handlers.help import router as help
# from app.handlers.rashod import router as rashod
# from app.handlers.akount import router as akount
# from app.handlers.settings import router as settings
from aiogram import Bot, Dispatcher
# Initialize logging
logging.basicConfig(level=logging.INFO)


TOKEN = os.getenv('FSOCIETY')
# Initialize Bot and Dispatcher
bot = Bot(token = TOKEN)
dp = Dispatcher()

async def main():
    dp.include_router(common)
    # dp.include_router(rashod)
    # dp.include_router(akount)
    # dp.include_router(settings)

    # dp.update.middleware(UnifiedMessageMiddleware())  # Update middleware
    # scheduler_task = asyncio.create_task(schedule_daily_task(bot))
    try:
        # Start polling
        await dp.start_polling(bot)
    except (ClientConnectorError, TelegramNetworkError, asyncio.TimeoutError, OSError, ConnectionError) as e:
        logging.error(f"Internet connection lost or Telegram unreachable: {e}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Unexpected error occurred: {e}")
    finally:
        # Ensure the bot's session is closed on shutdown
        # scheduler_task.cancel()
        # try:
            # await scheduler_task
        # except asyncio.CancelledError:
            # logging.info("Scheduler task cancelled.")
        await bot.session.close()
        logging.info("Bot session closed.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
