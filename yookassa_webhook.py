"""
FastAPI: webhook ЮKassa. Тот же процесс, что и aiogram — контекст через services.webhook_context.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from handlers.user import finalize_confirmed_payment
from services.webhook_context import get_bot, get_config, get_db, get_reminder_service
from services.yookassa_payments import payments

logger = logging.getLogger(__name__)

app = FastAPI(title="Studio bot — YooKassa webhook")

@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/")
async def root() -> dict[str, str]:
    """
    Некоторые прокси/хостинги (в т.ч. PaaS) могут «срезать» path и прокидывать в приложение только '/'.
    Поэтому даём понятный ответ и используем '/' как алиас для webhook (POST).
    """
    return {
        "status": "ok",
        "service": "studio-bot",
        "webhook": "/yookassa-webhook",
        "note": "If your hosting strips path, YooKassa POST may arrive to '/'",
    }

@app.middleware("http")
async def _log_all_requests(request: Request, call_next):
    """
    Bothost может не показывать print() в логах стабильно.
    Поэтому дополнительно логируем вход на /yookassa-webhook через logging.
    """
    try:
        raw = await request.body()
        preview = raw[:2000].decode("utf-8", errors="replace") if raw else ""
    except Exception as e:
        preview = f"<failed to read body: {e}>"

    # Логируем всё, что приходит на webhook-роуты (и на '/', если хостинг режет path).
    is_webhookish = request.url.path in ("/yookassa-webhook", "/") or request.url.path.startswith(
        "/yookassa-webhook"
    )
    if is_webhookish:
        logger.warning("WEBHOOK RECEIVED %s %s", request.method, request.url.path)
        # Иногда прокси передаёт оригинальный URL в заголовках — полезно увидеть.
        for hk in (
            "x-original-uri",
            "x-forwarded-uri",
            "x-rewrite-url",
            "x-forwarded-proto",
            "x-forwarded-host",
        ):
            hv = request.headers.get(hk)
            if hv:
                logger.warning("WEBHOOK HDR %s: %s", hk, hv)
        if preview:
            logger.warning("WEBHOOK RAW BODY (preview): %s", preview)
        else:
            logger.warning("WEBHOOK RAW BODY (empty)")

    resp = await call_next(request)

    if is_webhookish:
        logger.warning("WEBHOOK RESP STATUS: %s", getattr(resp, "status_code", "?"))
    return resp


@app.get("/yookassa-webhook")
async def yookassa_webhook_ping() -> dict[str, str]:
    """Проверка, что URL снаружи открывается (ЮKassa шлёт только POST)."""
    return {
        "status": "ok",
        "hint": "Для ЮKassa нужен POST https://ваш-домен/yookassa-webhook",
    }


def _parse_webhook_meta(obj: dict) -> tuple[int, int]:
    """user_id, booking_id из тела webhook (object.metadata — строки)."""
    om = obj.get("metadata") or {}
    uid = int(str(om.get("user_id") or "0").strip() or "0")
    bid = int(str(om.get("booking_id") or "0").strip() or "0")
    return uid, bid


def _safe_preview(body: dict) -> dict:
    """
    Логируем входящий webhook без чувствительных данных:
    оставляем event, object.id, object.status и object.metadata.
    """
    obj = body.get("object") or {}
    return {
        "event": body.get("event"),
        "type": body.get("type"),
        "object": {
            "id": obj.get("id"),
            "status": obj.get("status"),
            "paid": obj.get("paid"),
            "metadata": obj.get("metadata"),
        },
    }


@app.post("/yookassa-webhook")
async def yookassa_webhook(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    # --- подробные логи для отладки ---
    logger.warning("WEBHOOK RECEIVED")
    try:
        logger.warning("WEBHOOK BODY: %s", _safe_preview(body))
    except Exception:
        logger.warning("WEBHOOK BODY: <failed to preview>")

    ev = body.get("event")
    logger.warning("EVENT: %s", ev)
    if ev != "payment.succeeded":
        return JSONResponse({"ok": True})

    obj = body.get("object") or {}
    payment_id = obj.get("id")
    logger.warning("PAYMENT ID: %s", payment_id)
    if not payment_id:
        logger.warning("PAYMENT ID: <missing>")
        logger.warning("YooKassa webhook: payment.succeeded without object.id")
        return JSONResponse({"ok": True})

    pid = str(payment_id)
    # 1) Сначала пробуем из metadata (надёжнее: не зависит от RAM)
    u_meta, b_meta = _parse_webhook_meta(obj)
    logger.warning("METADATA: %s", obj.get("metadata"))
    user_id = int(u_meta or 0)
    booking_id = int(b_meta or 0)

    if not user_id or not booking_id:
        logger.warning("METADATA ERROR: user_id or booking_id is missing/empty")

        # 2) Если metadata нет/пустая — пробуем локальное хранилище payments
        meta = payments.get(pid) or {}
        if meta:
            try:
                user_id = int(meta.get("user_id", 0) or 0)
            except Exception:
                user_id = 0
            slot = meta.get("slot") or {}
            try:
                booking_id = int(slot.get("booking_id", 0) or 0)
            except Exception:
                booking_id = 0

    if user_id:
        logger.warning("USER FOUND")
    else:
        logger.warning("USER NOT FOUND")

    bot = get_bot()
    db = get_db()
    cfg = get_config()
    reminder = get_reminder_service()

    if not bot or not db or not cfg or not reminder:
        logger.error("ERROR: webhook context is not initialized")
        logger.error("Webhook context is not initialized")
        return JSONResponse({"ok": False}, status_code=503)

    if not booking_id:
        print("ERROR: BOOKING_ID missing; cannot confirm booking")
        logger.warning("YooKassa webhook: missing booking_id for payment %s", pid)
        payments.pop(pid, None)
        return JSONResponse({"ok": True})

    try:
        # Цель задачи: пользователь получает это сообщение автоматически после оплаты
        await bot.send_message(user_id, "Оплата прошла, запись подтверждена ✅")
    except Exception:
        logger.exception("Failed to notify user %s", user_id)

    try:
        ok, _msg = await finalize_confirmed_payment(bot, db, cfg, reminder, booking_id)
        if not ok:
            logger.warning(
                "finalize_confirmed_payment failed for booking_id=%s (maybe already done)",
                booking_id,
            )
        else:
            logger.info("YooKassa webhook: заявка #%s переведена в оплаченную", booking_id)
    except Exception:
        logger.exception("finalize_confirmed_payment booking_id=%s", booking_id)
        return JSONResponse({"ok": True})

    payments.pop(pid, None)
    return JSONResponse({"ok": True})


# Алиас: если хостинг/прокси режет path и YooKassa POST фактически приходит на '/',
# мы всё равно обработаем событие.
@app.post("/")
async def yookassa_webhook_root_alias(request: Request) -> JSONResponse:
    return await yookassa_webhook(request)
