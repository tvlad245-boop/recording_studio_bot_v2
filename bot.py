import asyncio
import logging
import os
import socket

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
from services.yclients_studio import yclients_studio_enabled


def _pick_free_tcp_port(*, preferred: int, span: int = 48) -> int:
    """
    Первый свободный порт начиная с preferred (0.0.0.0), чтобы не падать с Errno 98,
    если 8080 уже занят другим процессом в контейнере.
    """
    last = preferred + max(span, 1)
    for port in range(preferred, last):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return port
        except OSError:
            continue
        finally:
            s.close()
    raise RuntimeError(
        f"Не найден свободный TCP-порт в диапазоне {preferred}..{last - 1} для ЮKassa webhook"
    )


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


async def _yookassa_webhook_background(host: str, port: int, log: logging.Logger) -> None:
    """
    Uvicorn при ошибке bind часто вызывает sys.exit(1) — это SystemExit.
    В asyncio.gather() это роняет весь процесс и polling не стартует.
    Здесь глотаем SystemExit/OSError: бот остаётся в polling, webhook просто не работает.
    """
    try:
        await _run_yookassa_uvicorn(host, port)
    except asyncio.CancelledError:
        raise
    except SystemExit as e:
        exc_code = e.args[0] if e.args else "?"
        log.error(
            "ЮKassa webhook (uvicorn) завершился с SystemExit(%s) на порту %s — "
            "часто это «address already in use». Задайте свободный WEBHOOK_PORT или освободите порт. "
            "Бот продолжает работу в polling; автоподтверждение оплат по webhook недоступно.",
            exc_code,
            port,
        )
    except OSError as e:
        log.error(
            "ЮKassa webhook: порт %s — %s. Бот в polling работает; webhook отключён.",
            port,
            e,
        )
    except Exception:
        log.exception("ЮKassa webhook: неожиданная ошибка; бот в polling продолжает работу.")


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

        # HTTP вебхуки в этом же процессе:
        # - ЮKassa (если настроена)
        # - Yclients (если включён режим студии из CRM или задан token)
        yclients_token = (os.getenv("YCLIENTS_WEBHOOK_TOKEN", "").strip() or os.getenv("YCLIENTS_WEBHOOK_SECRET", "").strip())
        need_http_webhook = bool(cfg.yookassa_shop_id and cfg.yookassa_secret_key) or bool(
            yclients_studio_enabled(cfg) or yclients_token
        )

        if need_http_webhook:
            wh_host = os.getenv("WEBHOOK_HOST", "0.0.0.0").strip() or "0.0.0.0"
            wp_raw = os.getenv("WEBHOOK_PORT", "").strip()
            port_raw = os.getenv("PORT", "").strip()
            if wp_raw.isdigit():
                preferred = int(wp_raw)
            elif port_raw.isdigit():
                preferred = int(port_raw)
            else:
                preferred = 8080
            wh_port = _pick_free_tcp_port(preferred=preferred)
            if wh_port != preferred:
                _log.warning(
                    "Порт %s занят — для ЮKassa выбран свободный %s. "
                    "Обновите проброс портов в Docker / upstream nginx, если трафик шёл на %s.",
                    preferred,
                    wh_port,
                    preferred,
                )
            _log.info(
                "Webhook HTTP: http://%s:%s (ЮKassa: /yookassa-webhook, Yclients: /yclients-webhook)",
                wh_host,
                wh_port,
            )
            yk_task = asyncio.create_task(
                _yookassa_webhook_background(wh_host, wh_port, _log),
                name="yookassa-uvicorn",
            )
            try:
                await _polling()
            finally:
                yk_task.cancel()
                try:
                    await yk_task
                except asyncio.CancelledError:
                    pass
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

