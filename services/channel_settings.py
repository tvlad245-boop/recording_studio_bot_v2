"""Каналы из bot_settings с fallback на Config (.env)."""

from __future__ import annotations

from config import Config, payments_inbox_chat_id


def _parse_channel_int(raw: str) -> int | None:
    """ID канала/чата: убираем любые пробелы (часто копируют из Telegram с лишними символами)."""
    s = "".join((raw or "").strip().split())
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def effective_subscription_channel_id(settings: dict[str, str], cfg: Config) -> int:
    v = _parse_channel_int(settings.get("subscription_channel_id") or "")
    if v is not None:
        return v
    return int(cfg.channel_id)


def effective_subscription_channel_link(settings: dict[str, str], cfg: Config) -> str:
    raw = (settings.get("subscription_channel_link") or "").strip()
    if raw:
        return raw
    return (cfg.channel_link or "").strip() or "https://t.me/"


def effective_schedule_channel_id(settings: dict[str, str], cfg: Config) -> int:
    v = _parse_channel_int(settings.get("schedule_channel_id") or "")
    if v is not None:
        return v
    return int(cfg.schedule_channel_id)


def effective_payments_inbox_chat_id(settings: dict[str, str], cfg: Config) -> int:
    v = _parse_channel_int(settings.get("payments_inbox_chat_id") or "")
    if v is not None:
        return v if v != 0 else int(cfg.admin_id)
    return int(payments_inbox_chat_id(cfg))
