import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from config import load_config
from database.db import Database
from handlers.user import router as user_router
from handlers.admin import router as admin_router
from services.effective_pricing import build_default_settings_dict
from services.pricing_middleware import register_pricing_middleware
from services.reminders import ReminderService


async def main() -> None:
    load_dotenv()
    cfg = load_config()

    logging.basicConfig(level=logging.INFO)

    bot = Bot(token=cfg.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    db = Database(cfg.db_path)
    await db.init()
    await db.ensure_settings_defaults(build_default_settings_dict(cfg))
    await db.seed_days(60)

    scheduler = AsyncIOScheduler(timezone=cfg.timezone)
    scheduler.start()
    reminder_service = ReminderService(scheduler=scheduler, db=db, bot=bot)
    await reminder_service.restore_jobs()

    register_pricing_middleware(user_router)
    register_pricing_middleware(admin_router)

    dp.include_router(user_router)
    dp.include_router(admin_router)

    try:
        await dp.start_polling(bot, config=cfg, db=db, reminder_service=reminder_service)
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

