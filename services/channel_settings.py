"""Каналы из bot_settings с fallback на Config (.env)."""

from __future__ import annotations

from config import Config, payments_inbox_chat_id


def effective_subscription_channel_id(settings: dict[str, str], cfg: Config) -> int:
    raw = (settings.get("subscription_channel_id") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return int(cfg.channel_id)


def effective_subscription_channel_link(settings: dict[str, str], cfg: Config) -> str:
    raw = (settings.get("subscription_channel_link") or "").strip()
    if raw:
        return raw
    return (cfg.channel_link or "").strip() or "https://t.me/"


def effective_schedule_channel_id(settings: dict[str, str], cfg: Config) -> int:
    raw = (settings.get("schedule_channel_id") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return int(cfg.schedule_channel_id)


def effective_payments_inbox_chat_id(settings: dict[str, str], cfg: Config) -> int:
    raw = (settings.get("payments_inbox_chat_id") or "").strip()
    if raw:
        try:
            v = int(raw)
            return v if v != 0 else int(cfg.admin_id)
        except ValueError:
            pass
    return int(payments_inbox_chat_id(cfg))
