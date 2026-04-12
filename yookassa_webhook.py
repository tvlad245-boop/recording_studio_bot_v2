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


@app.post("/yookassa-webhook")
async def yookassa_webhook(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    ev = body.get("event")
    if ev != "payment.succeeded":
        return JSONResponse({"ok": True})

    obj = body.get("object") or {}
    payment_id = obj.get("id")
    if not payment_id:
        logger.warning("YooKassa webhook: payment.succeeded without object.id")
        return JSONResponse({"ok": True})

    pid = str(payment_id)
    meta = payments.get(pid)
    if not meta:
        u_meta, b_meta = _parse_webhook_meta(obj)
        if b_meta and u_meta:
            meta = {"user_id": u_meta, "slot": {"booking_id": b_meta}}
            logger.info(
                "YooKassa webhook: восстановление по metadata (нет записи в RAM) payment=%s booking=%s",
                pid,
                b_meta,
            )
        else:
            logger.warning(
                "YooKassa webhook: неизвестный платёж %s и нет booking_id в metadata — "
                "проверьте URL webhook в кабинете ЮKassa и что бот с HTTPS доступен из интернета.",
                pid,
            )
            return JSONResponse({"ok": True})

    user_id = int(meta.get("user_id", 0))
    slot = meta.get("slot") or {}
    booking_id = int(slot.get("booking_id", 0) or 0)

    bot = get_bot()
    db = get_db()
    cfg = get_config()
    reminder = get_reminder_service()

    if not bot or not db or not cfg or not reminder:
        logger.error("Webhook context is not initialized")
        return JSONResponse({"ok": False}, status_code=503)

    if not booking_id:
        logger.warning("YooKassa webhook: missing booking_id in slot for payment %s", pid)
        payments.pop(pid, None)
        return JSONResponse({"ok": True})

    try:
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
