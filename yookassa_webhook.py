"""
FastAPI: webhook ЮKassa. Тот же процесс, что и aiogram — контекст через services.webhook_context.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from handlers.user import finalize_confirmed_payment
from services.webhook_context import get_bot, get_config, get_db, get_reminder_service
from services.schedule_channel import publish_schedule_channel_bundle
from services.yookassa_payments import payments

logger = logging.getLogger(__name__)

app = FastAPI(title="Studio bot — webhooks (YooKassa + Yclients)")

# Опциональная защита webhook простым токеном.
# Если переменная задана — ожидаем заголовок X-Webhook-Token: <token>
_WEBHOOK_TOKEN = (os.getenv("WEBHOOK_TOKEN", "").strip() or os.getenv("WEBHOOK_SECRET", "").strip())
# Отдельный токен для Yclients (если пусто — пробуем общий WEBHOOK_TOKEN).
_YCLIENTS_WEBHOOK_TOKEN = (
    os.getenv("YCLIENTS_WEBHOOK_TOKEN", "").strip()
    or os.getenv("YCLIENTS_WEBHOOK_SECRET", "").strip()
    or _WEBHOOK_TOKEN
)

_DT_SQL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$")

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
async def _log_webhook_request(request: Request, call_next):
    """
    Короткая отметка о запросах к webhook-маршрутам.
    Тело не читаем здесь — его парсит обработчик POST; иначе лишняя работа и риск
    конфликтов при чтении потока на некоторых стеках.
    """
    path = request.url.path
    is_webhookish = path in ("/yookassa-webhook", "/") or path.startswith("/yookassa-webhook")
    if is_webhookish:
        logger.info("WEBHOOK %s %s", request.method, path)
        if logger.isEnabledFor(logging.DEBUG):
            for hk in (
                "x-original-uri",
                "x-forwarded-uri",
                "x-rewrite-url",
                "x-forwarded-proto",
                "x-forwarded-host",
            ):
                hv = request.headers.get(hk)
                if hv:
                    logger.debug("WEBHOOK HDR %s: %s", hk, hv)

    resp = await call_next(request)

    if is_webhookish and logger.isEnabledFor(logging.DEBUG):
        logger.debug("WEBHOOK RESP %s", getattr(resp, "status_code", "?"))
    return resp


@app.get("/yookassa-webhook")
async def yookassa_webhook_ping() -> dict[str, str]:
    """Проверка, что URL снаружи открывается (ЮKassa шлёт только POST)."""
    return {
        "status": "ok",
        "hint": "Для ЮKassa нужен POST https://ваш-домен/yookassa-webhook",
    }


@app.get("/yclients-webhook")
async def yclients_webhook_ping() -> dict[str, str]:
    return {
        "status": "ok",
        "hint": "Для Yclients нужен POST https://ваш-домен/yclients-webhook",
    }


def _hash_event(body: dict) -> str:
    """
    Фолбэк-идентификатор события для идемпотентности.
    Не криптографическая защита, а стабильный ключ на одинаковое тело.
    """
    try:
        raw = repr(body).encode("utf-8", errors="ignore")
    except Exception:
        raw = b"?"
    return hashlib.sha1(raw).hexdigest()[:24]


def _walk(obj, *, depth: int = 0, max_depth: int = 5):
    if depth > max_depth:
        return
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v, depth=depth + 1, max_depth=max_depth)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v, depth=depth + 1, max_depth=max_depth)


def _first_int_in_body(body: dict, keys: tuple[str, ...]) -> int | None:
    for d in _walk(body):
        if not isinstance(d, dict):
            continue
        for k in keys:
            if k in d:
                v = d.get(k)
                if isinstance(v, dict):
                    v = v.get("id")
                try:
                    n = int(v)  # type: ignore[arg-type]
                    if n > 0:
                        return n
                except Exception:
                    continue
    return None


def _first_str_in_body(body: dict, keys: tuple[str, ...]) -> str | None:
    for d in _walk(body):
        if not isinstance(d, dict):
            continue
        for k in keys:
            if k in d:
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return None


def _parse_dt_and_length(body: dict) -> tuple[str | None, int | None]:
    dt = _first_str_in_body(body, ("datetime", "date_time", "start_datetime", "start_time"))
    if dt and _DT_SQL_RE.match(dt):
        pass
    else:
        dt = None
    seclen: int | None = None
    raw_len = None
    for k in ("seance_length", "length", "duration", "duration_sec", "duration_seconds"):
        raw_len = _first_int_in_body(body, (k,))
        if raw_len:
            break
    if raw_len:
        # В Yclients book_times seance_length — секунды; здесь принимаем как секунды.
        seclen = int(raw_len)
        if seclen < 60:
            seclen = None
    return dt, seclen


def _event_kind(body: dict) -> str:
    """
    cancel | update | ignore
    Пытаемся по строковым полям event/type/status.
    """
    s = " ".join(
        x
        for x in (
            str(body.get("event") or ""),
            str(body.get("type") or ""),
            str(body.get("action") or ""),
            str(body.get("status") or ""),
        )
        if x
    ).lower()
    if any(w in s for w in ("delete", "removed", "cancel", "canceled", "cancelled", "decline", "reject")):
        return "cancel"
    if any(w in s for w in ("update", "changed", "edit", "reschedule", "move")):
        return "update"
    # Если явного action нет — но пришли datetime/длительность, это тоже update.
    dt, seclen = _parse_dt_and_length(body)
    if dt or seclen:
        return "update"
    return "ignore"


@app.post("/yclients-webhook")
async def yclients_webhook(request: Request) -> JSONResponse:
    if _YCLIENTS_WEBHOOK_TOKEN:
        got = (request.headers.get("x-webhook-token") or "").strip()
        got2 = (request.headers.get("x-yclients-token") or "").strip()
        if got != _YCLIENTS_WEBHOOK_TOKEN and got2 != _YCLIENTS_WEBHOOK_TOKEN:
            logger.warning("YCLIENTS WEBHOOK TOKEN MISMATCH")
            return JSONResponse({"ok": False}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    bot = get_bot()
    db = get_db()
    cfg = get_config()
    reminder = get_reminder_service()
    if not bot or not db or not cfg or not reminder:
        logger.error("Yclients webhook: context is not initialized")
        return JSONResponse({"ok": False}, status_code=503)

    record_id = _first_int_in_body(body, ("record_id", "recordId", "id"))
    if not record_id:
        logger.info("Yclients webhook: no record_id in body (ignored)")
        return JSONResponse({"ok": True})

    event_id = (
        _first_str_in_body(body, ("event_id", "eventId", "uuid", "guid"))
        or f"rid:{record_id}:{_hash_event(body)}"
    )
    try:
        first = await db.mark_yclients_event_processed(event_id)
    except Exception:
        logger.exception("Yclients webhook: failed to mark processed event_id=%s", event_id)
        first = True
    if not first:
        return JSONResponse({"ok": True})

    booking = await db.get_booking_by_yclients_record_id(int(record_id))
    if not booking:
        logger.info("Yclients webhook: booking not found for record_id=%s", record_id)
        return JSONResponse({"ok": True})

    kind = _event_kind(body)
    uid = int(booking.get("user_id") or 0)
    bid = int(booking.get("id") or 0)

    if kind == "cancel":
        try:
            await reminder.remove_for_booking(bid)
        except Exception:
            pass
        try:
            await db.cancel_booking(bid)
        except Exception:
            logger.exception("Yclients webhook: cancel_booking failed id=%s", bid)
        try:
            await publish_schedule_channel_bundle(bot, db, cfg)
        except Exception:
            pass
        if uid:
            try:
                await bot.send_message(
                    uid,
                    "❌ Ваша запись была отменена в CRM.\n"
                    f"Запись #{bid}: {booking.get('day', '—')} {booking.get('start_time', '—')} — {booking.get('end_time', '—')}",
                )
            except Exception:
                pass
        return JSONResponse({"ok": True})

    if kind == "update":
        dt_sql, seclen = _parse_dt_and_length(body)
        if dt_sql and seclen:
            try:
                dt0 = datetime.strptime(dt_sql, "%Y-%m-%d %H:%M:%S")
                dt1 = dt0 + timedelta(seconds=int(seclen))
                day = dt0.strftime("%Y-%m-%d")
                st = dt0.strftime("%H:%M")
                et = dt1.strftime("%H:%M")
            except Exception:
                day = st = et = None  # type: ignore[assignment]
        else:
            day = st = et = None  # type: ignore[assignment]

        if day and st and et:
            updated = await db.update_booking_times_by_yclients_record_id(
                int(record_id), day=day, start_time=st, end_time=et
            )
            if updated:
                try:
                    await reminder.remove_for_booking(bid)
                    await reminder.schedule_for_booking(updated)
                except Exception:
                    pass
                try:
                    await publish_schedule_channel_bundle(bot, db, cfg)
                except Exception:
                    pass
                if uid:
                    try:
                        await bot.send_message(
                            uid,
                            "⏱ Ваша запись была изменена в CRM.\n"
                            f"Запись #{bid}: {day} {st} — {et}",
                        )
                    except Exception:
                        pass
        return JSONResponse({"ok": True})

    return JSONResponse({"ok": True})


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
    if _WEBHOOK_TOKEN:
        got = (request.headers.get("x-webhook-token") or "").strip()
        if got != _WEBHOOK_TOKEN:
            logger.warning("WEBHOOK TOKEN MISMATCH")
            return JSONResponse({"ok": False}, status_code=403)
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

    # Если metadata/RAM не дали booking_id или user_id — пробуем SQLite (переживает рестарты)
    if not booking_id or not user_id:
        try:
            link = await db.get_yookassa_payment_link(pid)
        except Exception:
            link = None
        if link:
            if not booking_id:
                booking_id = int(link.get("booking_id") or 0)
            if not user_id:
                user_id = int(link.get("user_id") or 0)
            logger.warning("USER FOUND (sqlite)") if user_id else logger.warning("USER NOT FOUND (sqlite)")

    if not booking_id:
        logger.warning("YooKassa webhook: missing booking_id for payment %s", pid)
        payments.pop(pid, None)
        return JSONResponse({"ok": True})

    # Идемпотентность: помечаем как обработанный только когда знаем booking_id
    try:
        first = await db.mark_yookassa_payment_processed(pid)
    except Exception:
        logger.exception("Failed to mark payment processed payment_id=%s", pid)
        first = True
    if not first:
        logger.warning("Duplicate webhook ignored payment_id=%s", pid)
        return JSONResponse({"ok": True})

    try:
        ok, _msg = await finalize_confirmed_payment(
            bot,
            db,
            cfg,
            reminder,
            booking_id,
            success_entry_prefix_html="<b>✅ Оплата прошла успешно, запись подтверждена</b>",
        )
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
    try:
        await db.delete_yookassa_payment_link(pid)
    except Exception:
        pass
    return JSONResponse({"ok": True})


# Алиас: если хостинг/прокси режет path и YooKassa POST фактически приходит на '/',
# мы всё равно обработаем событие.
@app.post("/")
async def yookassa_webhook_root_alias(request: Request) -> JSONResponse:
    return await yookassa_webhook(request)
