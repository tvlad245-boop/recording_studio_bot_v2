"""
Публикация в канал расписания: неделя + задачи текстовика + задачи битмейкера.
Логика вынесена из handlers/user.py для изоляции ошибок и проще сопровождения.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from html import escape as html_escape
from typing import Any

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import Config
from database.db import Database
from services.channel_settings import effective_schedule_channel_id
from services.content_settings import (
    effective_maker_username,
    format_maker_username as _format_maker_username,
)

logger = logging.getLogger(__name__)

KEY_WEEK = "schedule_week_7d"
KEY_LYRICS = "tasks_lyrics"
KEY_BEAT = "tasks_beat"

_TELEGRAM_TEXT_LIMIT = 4096
_SAFE_LIMIT = 3800


def _format_tg_username(u: str | None) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u if u.startswith("@") else f"@{u}"


def _slot_cell(r: dict[str, Any] | None) -> str:
    if not r:
        return ""
    st = Database.normalize_time_str(r["start_time"])
    et = Database.normalize_time_str(r["end_time"])
    if r["is_active"]:
        mark = "🟢"
    else:
        mark = "🟠" if int(r.get("requires_engineer") or 0) == 1 else "🔴"
    return f"{mark} {st}-{et}"


async def _build_week_schedule_for_ndays(db: Database, num_days: int) -> str:
    """Внутренний билдер без рекурсии."""
    num_days = max(1, min(7, num_days))
    days = [(date.today() + timedelta(days=i)).isoformat() for i in range(num_days)]
    title = (
        "<b>📅 Расписание (7 дней)</b>"
        if num_days == 7
        else f"<b>📅 Расписание ({num_days} дн.)</b>"
    )
    lines: list[str] = [title, ""]
    for day in days:
        schedule = await db.get_day_schedule(day)
        lines.append(f"<b>{day}</b>")
        left = [
            r
            for r in schedule
            if int(Database.time_sort_key(Database._coerce_cell_str(r["start_time"]))[0]) < 12
        ]
        right = [
            r
            for r in schedule
            if int(Database.time_sort_key(Database._coerce_cell_str(r["start_time"]))[0]) >= 12
        ]
        n = max(len(left), len(right))
        table_lines: list[str] = []
        for i in range(n):
            l = _slot_cell(left[i]) if i < len(left) else ""
            rr = _slot_cell(right[i]) if i < len(right) else ""
            table_lines.append(f"{l:<12}    {rr}")
        legend = (
            "<i>🟢 свободно · 🟠 занято (со звукорежиссёром) · 🔴 занято (без звукорежиссёра)</i>"
        )
        lines.append(legend)
        lines.append("<pre>" + "\n".join(html_escape(x) for x in table_lines).strip() + "</pre>")
        lines.append("")
    return "\n".join(lines).strip()


async def build_week_schedule_html(db: Database) -> str:
    """Текст блока расписания; при переполнении лимита Telegram уменьшаем число дней."""
    try:
        for num_days in range(7, 0, -1):
            text = await _build_week_schedule_for_ndays(db, num_days)
            if len(text) <= _SAFE_LIMIT:
                return text
        return await _build_week_schedule_for_ndays(db, 1)[: _SAFE_LIMIT - 40] + "…"
    except Exception:
        logger.exception("build_week_schedule_html failed")
        return (
            "<b>📅 Расписание</b>\n\n"
            "<i>Временно не удалось загрузить таблицу слотов. Попробуйте позже.</i>"
        )


async def build_tasks_channel_block(
    db: Database,
    cfg: Config,
    kind: str,
    title: str,
) -> tuple[str, InlineKeyboardMarkup | None]:
    """Текст и кнопки «Выполнено» для текстовика или битмейкера."""
    try:
        orders = await db.get_active_service_orders(kind)
        maker_u = await effective_maker_username(db, cfg, kind=kind)
    except Exception:
        logger.exception("build_tasks_channel_block: fetch orders/maker failed kind=%s", kind)
        return (
            f"<b>{title}</b>\n\n<i>Не удалось загрузить заявки.</i>",
            None,
        )

    out = [f"<b>{title}</b>", "", f"<b>Исполнитель:</b> {html_escape(_format_maker_username(maker_u))}", ""]
    if not orders:
        out.append("Нет заявок в работе и на проверке оплаты.")
        return "\n".join(out), None

    kb = InlineKeyboardBuilder()
    n_done = 0
    for o in orders:
        client_u = _format_tg_username(o.get("tg_username"))
        notes = (o.get("notes") or "").strip().replace("\n", " ")
        if len(notes) > 250:
            notes = notes[:247] + "..."
        st = (o.get("status") or "").strip()
        status_line = "\n<i>⏳ Ожидает подтверждения оплаты</i>" if st == "pending_payment" else ""
        out.append(
            f"<b>#{o['id']}</b> — {html_escape(str(o.get('services', '—')))} — "
            f"<b>{o.get('total_price', 0)} руб</b>{status_line}\n"
            f"<b>Клиент:</b> {html_escape(str(o.get('user_name', '')))}\n"
            f"<b>Банк:</b> {html_escape(str(o.get('phone', '')))}\n"
            f"<b>Telegram клиента:</b> {html_escape(client_u) if client_u else '—'}\n"
            f"{html_escape(notes) if notes else '—'}"
        )
        out.append("")
        if st == "active":
            kb.button(
                text=f"✅ #{o['id']}",
                callback_data=f"task_done:{kind}:{int(o['id'])}",
            )
            n_done += 1

    body = "\n".join(out).strip()
    if len(body) > _SAFE_LIMIT:
        body = body[: _SAFE_LIMIT - 40] + "…"

    if n_done == 0:
        return body, None
    kb.adjust(2)
    return body, kb.as_markup()


def _markup_for_edit(reply_markup: InlineKeyboardMarkup | None) -> InlineKeyboardMarkup:
    """При edit пустая клавиатура снимает старые кнопки; None у Telegram = «не менять»."""
    if reply_markup is not None:
        return reply_markup
    return InlineKeyboardMarkup(inline_keyboard=[])


def _clamp_text(text: str) -> str:
    t = (text or "").strip()
    if not t:
        t = "—"
    if len(t) > _TELEGRAM_TEXT_LIMIT:
        t = t[: _TELEGRAM_TEXT_LIMIT - 20] + "…"
    return t


async def upsert_channel_message(
    bot: Bot,
    db: Database,
    *,
    key: str,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    """
    Одно закреплённое сообщение по ключу: правка по message_id или новая отправка.
    """
    if not chat_id:
        return
    chat_id = int(chat_id)
    text = _clamp_text(text)
    edit_markup = _markup_for_edit(reply_markup)

    stored = await db.get_bot_message(key)
    if stored and int(stored.get("chat_id", 0)) != chat_id:
        await db.delete_bot_message(key)
        stored = None

    if stored and stored.get("message_id"):
        mid = int(stored["message_id"])
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=mid,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=edit_markup,
            )
            return
        except TelegramBadRequest as e:
            err = str(e).lower()
            if "message is not modified" in err:
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=mid,
                        reply_markup=edit_markup,
                    )
                except Exception:
                    pass
                return
            logger.warning("edit_message_text failed key=%s: %s", key, e)
        except Exception as e:
            logger.warning("edit_message_text failed key=%s: %s", key, e)

        await db.delete_bot_message(key)
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass

    try:
        msg = await bot.send_message(
            chat_id,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except Exception:
        logger.exception("send_message failed key=%s chat_id=%s", key, chat_id)
        raise
    await db.upsert_bot_message(key, chat_id, int(msg.message_id))


async def publish_schedule_channel_bundle(bot: Bot, db: Database, cfg: Config) -> None:
    """
    Три независимых сообщения в канале: неделя, задачи lyrics, задачи beat.
    Ошибка одного блока не отменяет остальные.
    """
    s = await db.get_all_settings()
    sch_id = effective_schedule_channel_id(s, cfg)
    if not sch_id:
        return

    # 1) Неделя
    try:
        week_text = await build_week_schedule_html(db)
        await upsert_channel_message(
            bot, db, key=KEY_WEEK, chat_id=sch_id, text=week_text, reply_markup=None
        )
    except Exception:
        logger.exception("schedule channel: week block failed")

    # 2) Текстовик
    try:
        lyrics_text, lyrics_markup = await build_tasks_channel_block(
            db, cfg, "lyrics", "📝 Задачи текстовика"
        )
        await upsert_channel_message(
            bot,
            db,
            key=KEY_LYRICS,
            chat_id=sch_id,
            text=lyrics_text,
            reply_markup=lyrics_markup,
        )
    except Exception:
        logger.exception("schedule channel: lyrics tasks failed")

    # 3) Битмейкер
    try:
        beat_text, beat_markup = await build_tasks_channel_block(
            db, cfg, "beat", "🎚️ Задачи битмейкера"
        )
        await upsert_channel_message(
            bot,
            db,
            key=KEY_BEAT,
            chat_id=sch_id,
            text=beat_text,
            reply_markup=beat_markup,
        )
    except Exception:
        logger.exception("schedule channel: beat tasks failed")
