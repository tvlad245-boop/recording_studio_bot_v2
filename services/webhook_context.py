"""
Общий контекст для HTTP webhook (ЮKassa) и polling-бота: один экземпляр Bot, БД, конфиг, напоминания.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiogram import Bot

    from config import Config
    from database.db import Database
    from services.reminders import ReminderService

_bot: object | None = None
_db: object | None = None
_cfg: object | None = None
_reminder: object | None = None


def set_payment_webhook_context(
    *,
    bot: "Bot",
    db: "Database",
    cfg: "Config",
    reminder_service: "ReminderService",
) -> None:
    global _bot, _db, _cfg, _reminder
    _bot = bot
    _db = db
    _cfg = cfg
    _reminder = reminder_service


def get_bot() -> "Bot | None":
    return _bot  # type: ignore[return-value]


def get_db() -> "Database | None":
    return _db  # type: ignore[return-value]


def get_config() -> "Config | None":
    return _cfg  # type: ignore[return-value]


def get_reminder_service() -> "ReminderService | None":
    return _reminder  # type: ignore[return-value]
