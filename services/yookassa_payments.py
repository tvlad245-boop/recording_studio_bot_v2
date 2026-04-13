"""
ЮKassa: создание платежей и хранение связи payment_id → user_id + slot (dict).
"""

from __future__ import annotations

import logging
import uuid
from html import escape as html_escape
from typing import Any, TYPE_CHECKING

from config import Config

logger = logging.getLogger(__name__)

# payment_id (str) → {"user_id": int, "slot": dict}
payments: dict[str, dict[str, Any]] = {}


def pop_yookassa_payments_for_booking(booking_id: int) -> None:
    """Удаляет записи в RAM по booking_id (после отмены брошенной оплаты)."""
    bid = int(booking_id)
    for pid, meta in list(payments.items()):
        slot = meta.get("slot") or {}
        try:
            sb = int(slot.get("booking_id") or 0)
        except (TypeError, ValueError):
            sb = 0
        if sb == bid:
            payments.pop(pid, None)

if TYPE_CHECKING:
    from database.db import Database


def is_yookassa_configured(config: Config) -> bool:
    return bool(config.yookassa_shop_id and config.yookassa_secret_key)


def payment_destination_block_html(config: Config) -> str:
    """Блок «куда платить» для экрана оплаты: ЮKassa или реквизиты карты из .env."""
    if is_yookassa_configured(config):
        return (
            "<b>Оплата онлайн:</b> после ввода контактов нажмите «Перейти к оплате» — "
            "откроется страница ЮKassa (банковская карта)."
        )
    pay = (config.payment_details or "").strip().replace("\\n", "\n")
    return f"<b>Куда отправить:</b>\n{html_escape(pay)}"


async def create_payment(
    amount: int,
    description: str,
    user_id: int,
    slot: dict[str, Any],
    *,
    config: Config,
    db: "Database | None" = None,
) -> str:
    """
    Создаёт платёж в ЮKassa, сохраняет payments[payment_id] = {user_id, slot}, возвращает confirmation_url.
    """
    if not is_yookassa_configured(config):
        raise RuntimeError("YooKassa is not configured (SHOP_ID / SECRET_KEY)")

    try:
        from yookassa import Configuration, Payment
    except ImportError as e:
        raise RuntimeError("Install package: yookassa") from e

    Configuration.account_id = config.yookassa_shop_id
    Configuration.secret_key = config.yookassa_secret_key

    value = f"{int(amount)}.00"
    desc = (description or "Оплата").strip()[:128]
    idempotency_key = str(uuid.uuid4())

    # Метаданные приходят обратно в webhook — если бот перезапустили, словарь payments пуст,
    # но по metadata всё равно можно подтвердить заказ.
    meta: dict[str, str] = {"user_id": str(user_id)}
    bid = slot.get("booking_id")
    if bid is not None:
        meta["booking_id"] = str(int(bid))

    payment = Payment.create(
        {
            "amount": {"value": value, "currency": "RUB"},
            "confirmation": {
                "type": "redirect",
                "return_url": config.yookassa_return_url,
            },
            "capture": True,
            "description": desc,
            "metadata": meta,
        },
        idempotency_key,
    )

    pid = getattr(payment, "id", None)
    if not pid and isinstance(payment, dict):
        pid = payment.get("id")
    if not pid:
        raise RuntimeError("YooKassa: empty payment id")

    confirmation = getattr(payment, "confirmation", None)
    url = getattr(confirmation, "confirmation_url", None) if confirmation is not None else None
    if not url and isinstance(payment, dict):
        conf = payment.get("confirmation") or {}
        url = conf.get("confirmation_url")

    if not url:
        raise RuntimeError("YooKassa: no confirmation_url")

    payments[str(pid)] = {"user_id": int(user_id), "slot": dict(slot)}
    if db is not None:
        bid = int(slot.get("booking_id") or 0)
        if bid:
            await db.upsert_yookassa_payment_link(str(pid), bid, int(user_id))
    logger.info("YooKassa payment created id=%s user_id=%s", pid, user_id)
    return str(url)
