import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramUnauthorizedError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import load_config
from database.db import Database
from handlers.user import router as user_router
from handlers.admin import router as admin_router
from services.effective_pricing import build_default_settings_dict
from services.pricing_middleware import register_pricing_middleware
from services.reminders import ReminderService
from services.webhook_context import set_payment_webhook_context
from yookassa_webhook import app as yookassa_webhook_app


async def _run_yookassa_uvicorn(host: str, port: int) -> None:
    import uvicorn

    config = uvicorn.Config(
        yookassa_webhook_app,
        host=host,
        port=port,
        log_level="info",
        access_log=True,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main() -> None:
    cfg = load_config()

    logging.basicConfig(level=logging.INFO)
    _log = logging.getLogger(__name__)
    if cfg.yookassa_shop_id and cfg.yookassa_secret_key:
        _log.info(
            "ЮKassa включена: онлайн-оплата по ссылке (shop_id загружен, длина ключа: %s)",
            len(cfg.yookassa_secret_key),
        )
    else:
        _log.warning(
            "ЮKassa выключена: в .env задайте YOOKASSA_SHOP_ID и YOOKASSA_SECRET_KEY "
            "(или SHOP_ID и SECRET_KEY). Нужен файл .env в папке проекта; при конфликте с системными "
            "переменными значения из .env теперь перекрывают их."
        )

    bot = Bot(token=cfg.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    scheduler = None

    try:
        db = Database(cfg.db_path)
        await db.init()
        await db.ensure_settings_defaults(build_default_settings_dict(cfg))
        await db.seed_booking_window()

        scheduler = AsyncIOScheduler(timezone=cfg.timezone)
        scheduler.start()
        reminder_service = ReminderService(
            scheduler=scheduler, db=db, bot=bot, timezone=cfg.timezone
        )
        await reminder_service.restore_jobs()

        register_pricing_middleware(user_router)
        register_pricing_middleware(admin_router)

        dp.include_router(user_router)
        dp.include_router(admin_router)

        set_payment_webhook_context(
            bot=bot, db=db, cfg=cfg, reminder_service=reminder_service
        )

        # Снять webhook, если раньше бот работал через webhook — иначе long polling не стартует.
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except TelegramUnauthorizedError:
            _log.error(
                "Telegram отклонил BOT_TOKEN (Unauthorized). Это не ошибка ЮKassa: "
                "проверьте токен в панели хостинга (BOT_TOKEN / TELEGRAM_TOKEN), что он совпадает "
                "с @BotFather (не отозван ли, нет ли кавычек, пробелов в начале/конце)."
            )
            raise SystemExit(1) from None

        _log.info(
            "Запуск long polling. Если в логах «Conflict: other getUpdates» — с тем же токеном уже "
            "крутится другой процесс (вторая консоль, VPS, systemd, PM2, тест на другой машине). "
            "Остановите лишние копии; один бот = один активный polling."
        )

        async def _polling() -> None:
            await dp.start_polling(bot, config=cfg, db=db, reminder_service=reminder_service)

        if cfg.yookassa_shop_id and cfg.yookassa_secret_key:
            wh_host = os.getenv("WEBHOOK_HOST", "0.0.0.0").strip() or "0.0.0.0"
            # Bothost/PAAS часто задаёт порт через переменную PORT.
            wh_port = int(os.getenv("WEBHOOK_PORT", os.getenv("PORT", "8080")))
            await asyncio.gather(
                _polling(),
                _run_yookassa_uvicorn(wh_host, wh_port),
            )
        else:
            await _polling()
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

