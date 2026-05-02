from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from calendar import monthrange
from datetime import date as dt_date
from collections.abc import Iterable
from typing import Any
from html import escape as html_escape

from aiogram import Bot, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import Config
from database.db import Database
from services.channel_settings import (
    effective_payments_inbox_chat_id,
    effective_schedule_channel_id,
    effective_subscription_channel_id,
    effective_subscription_channel_link,
)
from services.content_settings import (
    equipment_caption_html,
    equipment_photo_paths,
    effective_maker_username,
    format_maker_username as _format_maker_username,
    post_payment_contact_block_html,
    append_manager_contact_html,
    cancel_refund_warning_html,
    cancel_request_sent_body_html,
    manager_contact_html,
    studio_address_html,
    studio_directions_video_file_id,
    tariff_day_start_list,
    tariff_night_start_list,
    ui_photo_main_menu,
    ui_photo_payment,
    ui_photo_prices,
    ui_photo_tariff_category,
    ui_photo_tariff_day,
    ui_photo_tariff_night,
)
from keyboards import (
    back_to_menu_kb,
    booking_products_kb,
    equipment_carousel_kb,
    main_menu_kb,
    cancel_confirm_kb,
    month_calendar_kb,
    my_bookings_kb,
    now_month,
    paid_kb,
    payment_method_kb,
    reschedule_confirm_kb,
    slots_pick_kb,
    slots_rs_pick_kb,
    yclients_hours_kb,
    yclients_start_kb,
    studio_mode_kb,
    subscription_kb,
    tariff_category_kb,
    tariff_day_start_kb,
    tariff_hours_kb,
    tariff_night_start_kb,
)
from services.effective_pricing import EffectivePricing
from services.reminders import ReminderService
from services.schedule_channel import publish_schedule_channel_bundle as _publish_weekly_and_tasks
from services.subscription import is_subscribed
from services.yookassa_payments import (
    create_payment,
    is_yookassa_configured,
    payment_destination_block_html,
    pop_yookassa_payments_for_booking,
)
from services import yclients_studio as yc
from services.yclients_client import YclientsError
from states import BookingStates


router = Router()
logger = logging.getLogger(__name__)


async def _studio_day_slots(
    data: dict[str, Any], config: Config, db: Database, day: str
) -> list[dict[str, Any]]:
    if data.get("yclients_studio"):
        alt = yc.slot_rows_for_day(data, config, day)
        if alt is not None:
            return alt
    return await db.get_all_slots_for_day(day)

def _truncate_html(text: str, limit: int) -> str:
    """Обрезка HTML-текста под лимит Telegram для обычных сообщений / подписей."""
    t = (text or "").strip()
    if len(t) <= limit:
        return t
    cut = max(1, limit - 10)
    return t[:cut].rstrip() + "…"


def _yookassa_pay_url_kb(url: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Перейти к оплате", url=url)
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


async def _send_studio_directions_to_user(bot: Bot, db: Database, user_id: int) -> None:
    """После подтверждения оплаты — отдельное видео с подписью (адрес в подписи, если задан)."""
    vid = await studio_directions_video_file_id(db)
    if not vid:
        return
    addr = await studio_address_html(db)
    cap = "<b>Как пройти до студии</b>"
    if addr:
        cap = f"{cap}\n\n{addr}"
    cap = _truncate_html(cap, 1024)
    try:
        await bot.send_video(
            user_id,
            video=vid,
            caption=cap,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


def _fit_message_text_for_edit(message: Message, text: str) -> str:
    """
    Telegram limits:
    - message text: 4096
    - photo caption: 1024
    If we exceed, Telegram rejects edit_* and we end up with only markup changes.
    """
    t = (text or "").strip()
    if getattr(message, "photo", None):
        limit = 1024
    else:
        limit = 4096
    if len(t) <= limit:
        return t
    # Keep some headroom for HTML entities
    cut = max(1, limit - 10)
    return t[:cut].rstrip() + "…"


async def _bulk_delete_chat_messages(bot, chat_id: int, message_ids: Iterable[int]) -> None:
    """
    Удаляет сообщения пакетами (до 100 за запрос через deleteMessages).
    Если пакетный метод недоступен для чата — параллельно удаляет по одному.
    """
    ids = sorted({int(x) for x in message_ids if x})
    if not ids:
        return
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        try:
            await bot.delete_messages(chat_id, chunk)
        except Exception:
            await asyncio.gather(
                *[bot.delete_message(chat_id, mid) for mid in chunk],
                return_exceptions=True,
            )


def _booking_hours_count(b: dict[str, Any]) -> int:
    raw = (b.get("booked_slot_ids") or "").strip()
    if raw:
        return len([x for x in raw.split(",") if x.strip()])
    return 1


def _format_tg_username(u: str | None) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u if u.startswith("@") else f"@{u}"


async def save_booking_pending_ui_cleanup(
    db: Database, booking_id: int, *, chat_id: int, data: dict[str, Any]
) -> None:
    root_mid = data.get("payment_root_message_id")
    payload = json.dumps(
        {
            "chat_id": chat_id,
            "root": int(root_mid) if root_mid is not None else None,
            "extra": [int(x) for x in (data.get("cleanup_ids") or [])],
        },
        ensure_ascii=False,
    )
    await db.update_booking_client_cleanup(booking_id, payload)


async def delete_booking_pending_ui_messages(bot: Bot, booking: dict[str, Any], db: Database) -> None:
    """Удаляет у клиента экран ожидания и сообщение с контактами (сохранённые при «Я оплатил»)."""
    raw = booking.get("client_cleanup_json")
    if not raw:
        return
    try:
        p = json.loads(raw)
        cid = int(p["chat_id"])
        act_mid: int | None = None
        act_chat: int | None = None
        row = await db.get_user_activity_message(int(booking["user_id"]))
        if row:
            act_mid = int(row["message_id"])
            act_chat = int(row["chat_id"])
        mids: list[int] = []
        if p.get("root") is not None:
            mids.append(int(p["root"]))
        mids.extend(int(x) for x in (p.get("extra") or []))
        for mid in mids:
            if act_mid is not None and act_chat is not None and mid == act_mid and cid == act_chat:
                continue
            try:
                await bot.delete_message(cid, mid)
            except Exception:
                pass
    except Exception:
        pass


async def delete_pending_ui_and_send_main_menu(
    bot: Bot,
    db: Database,
    config: Config,
    *,
    booking_snapshot: dict[str, Any],
    announcement_html: str,
) -> None:
    """После решения по заявке: убрать вспомогательные сообщения и показать текст в «липком» главном окне."""
    await delete_booking_pending_ui_messages(bot, booking_snapshot, db)
    uid = int(booking_snapshot["user_id"])
    ann = (announcement_html or "").strip()
    act = await db.get_user_activity_message(uid)
    if act:
        if ann:
            await db.set_user_activity_notice(uid, ann)
        await _render_user_activity_message(bot, db, config, uid)
        return
    if ann:
        try:
            await bot.send_message(uid, _truncate_html(ann, 4096), parse_mode=ParseMode.HTML)
        except Exception:
            pass
    s = await db.get_all_settings()
    photo = _file(ui_photo_main_menu(s, config))
    menu_caption = "<b>🏠 Главное меню</b>\nВыберите действие ниже."
    if photo:
        try:
            await bot.send_photo(
                uid,
                photo=photo,
                caption=_truncate_html(menu_caption, 1024),
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(),
            )
        except Exception:
            await bot.send_message(
                uid,
                _truncate_html(menu_caption, 4096),
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(),
            )
    else:
        await bot.send_message(
            uid,
            _truncate_html(menu_caption, 4096),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(),
        )


async def finalize_confirmed_payment(
    bot: Bot,
    db: Database,
    cfg: Config,
    reminder_service: ReminderService,
    booking_id: int,
    *,
    success_entry_prefix_html: str | None = None,
) -> tuple[bool, str]:
    """Оплата подтверждена: pending_payment или awaiting_yookassa → active; напоминания, канал, сводка."""
    row = await db.confirm_booking_payment(booking_id)
    if not row:
        return False, "Заявка не найдена или уже обработана"
    await delete_booking_pending_ui_messages(bot, row, db)
    kind = row.get("booking_kind") or "studio"
    if kind in (None, "studio"):
        await reminder_service.schedule_for_booking(row)
    # Канал расписания — сразу после фиксации данных в БД, до сводки/видео:
    # иначе ошибка в _upsert_user_success_summary или directions могла не дать обновить чат.
    try:
        await _publish_weekly_and_tasks(bot, db, cfg)
    except Exception:
        pass
    uid = int(row["user_id"])
    chat_id = uid
    if kind in ("lyrics", "beat"):
        contact_html = await post_payment_contact_block_html(db, cfg, kind=kind)
        entry = (
            f"<b>Заявка #{booking_id}</b>\n"
            f"{html_escape(str(row.get('services', '—')))}\n"
            f"<b>Сумма:</b> {row.get('total_price', 0)} руб\n"
            f"{contact_html}"
        )
        mgr = await manager_contact_html(db)
        if mgr:
            entry += f"\n\n{mgr}"
    else:
        entry = (
            f"<b>Запись #{booking_id}</b>\n"
            f"{html_escape(str(row.get('services', '—')))}\n"
            f"<b>Дата:</b> {html_escape(str(row.get('day', '—')))}\n"
            f"<b>Время:</b> {html_escape(str(row.get('start_time', '—')))} — "
            f"{html_escape(str(row.get('end_time', '—')))}\n"
            f"<b>Сумма:</b> {row.get('total_price', 0)} руб"
        )
        addr = await studio_address_html(db)
        if addr:
            entry += f"\n<b>Адрес студии:</b> {addr}"
        mgr = await manager_contact_html(db)
        if mgr:
            entry += f"\n\n{mgr}"
    if success_entry_prefix_html:
        entry = f"{success_entry_prefix_html.strip()}\n\n{entry}"
    await _upsert_user_success_summary(
        bot,
        db,
        user_id=uid,
        chat_id=chat_id,
        config=cfg,
        new_entry_html=entry,
    )
    if kind in (None, "studio"):
        await _send_studio_directions_to_user(bot, db, uid)
    return True, "ok"


def _payment_review_kb(booking_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="✅ Подтвердить оплату", callback_data=f"pay:ok:{booking_id}"
            ),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"pay:no:{booking_id}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _cancellation_review_kb(booking_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="✅ Подтвердить отмену", callback_data=f"cnc:ok:{booking_id}"
            ),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"cnc:no:{booking_id}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _reschedule_review_kb(booking_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="✅ Подтвердить перенос", callback_data=f"rsc:ok:{booking_id}"
            ),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"rsc:no:{booking_id}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _file(path: str) -> FSInputFile | None:
    p = (path or "").strip()
    if not p:
        return None
    if not os.path.isfile(p):
        return None
    return FSInputFile(p)


async def _send_payment_screen_message(
    bot,
    *,
    chat_id: int,
    text: str,
    reply_markup,
    photo_path: str,
) -> tuple[int, bool]:
    """Одно сообщение с блоком оплаты: фото + подпись (до 1024) или только текст (до 4096)."""
    photo = _file(photo_path)
    if photo:
        cap = _truncate_html(text, 1024)
        m = await bot.send_photo(
            chat_id,
            photo=photo,
            caption=cap,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )
        return int(m.message_id), True
    body = _truncate_html(text, 4096)
    m = await bot.send_message(
        chat_id,
        body,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )
    return int(m.message_id), False


async def _edit_payment_screen_message(
    bot,
    *,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup,
    is_photo: bool,
) -> None:
    t = _truncate_html(text, 1024 if is_photo else 4096)
    if is_photo:
        await bot.edit_message_caption(
            chat_id=chat_id,
            message_id=message_id,
            caption=t,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
    else:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=t,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )


async def _edit(message: Message, text: str, reply_markup=None) -> None:
    try:
        if getattr(message, "photo", None):
            await message.edit_caption(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        # Не отправляем новые сообщения при навигации
        try:
            await message.edit_reply_markup(reply_markup=reply_markup)
        except Exception:
            pass


async def _edit_screen(
    callback: CallbackQuery,
    text: str,
    reply_markup,
    *,
    photo_path: str = "",
) -> None:
    """
    Показ экрана с картинкой из .env: при уже фото-сообщении — edit_media;
    при текстовом — удаление и send_photo (иначе нельзя «добавить» фото к тексту).
    Пустой путь или отсутствующий файл — обычный _edit (текст/подпись).
    """
    msg = callback.message
    if not (photo_path or "").strip():
        await _edit(msg, text, reply_markup)
        return
    photo = _file(photo_path)
    if not photo:
        await _edit(msg, text, reply_markup)
        return
    cap = _truncate_html(text, 1024)
    try:
        if getattr(msg, "photo", None):
            await msg.edit_media(
                InputMediaPhoto(media=photo, caption=cap, parse_mode=ParseMode.HTML),
                reply_markup=reply_markup,
            )
            return
    except Exception:
        pass
    try:
        await msg.delete()
    except Exception:
        pass
    await callback.bot.send_photo(
        chat_id=msg.chat.id,
        photo=photo,
        caption=cap,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )


async def _present_my_bookings_message(
    callback: CallbackQuery,
    text: str,
    reply_markup,
) -> None:
    """
    Текст списка заявок + клавиатура. Если edit не удался (лимит подписи 1024, удалённое сообщение и т.д.),
    отправляет новое сообщение — чтобы не оставались одни кнопки «Отменить» без текста.
    """
    msg = callback.message
    if len(text) > 4096:
        text = text[:4090] + "…"
    try:
        if len(text) > 1024:
            await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            return
        if getattr(msg, "photo", None):
            await msg.edit_caption(caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await msg.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


async def _edit_by_message_id(
    bot,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup=None,
) -> None:
    try:
        await bot.edit_message_caption(
            chat_id=chat_id,
            message_id=message_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=chat_id, message_id=message_id, reply_markup=reply_markup
                )
            except Exception:
                pass


def _parse_tg_username_line(line: str, fallback: str | None) -> str:
    s = (line or "").strip()
    if not s or s in ("—", "-", "–", "нет", "Нет", "нету"):
        return (fallback or "").strip()
    return s.lstrip("@").strip()


async def _present_main_menu_on_message(
    bot,
    *,
    chat_id: int,
    message_id: int,
    config: Config,
    db: Database,
    extra_top_html: str | None = None,
) -> None:
    """Главное меню с картинкой MAIN_MENU_PHOTO_PATH (как после /start), в т.ч. после оплаты или раздела «Оборудование»."""
    if extra_top_html:
        text = f"{extra_top_html}\n\n<b>🏠 Главное меню</b>"
    else:
        text = "<b>🏠 Главное меню</b>"
    cap = _truncate_html(text, 1024)
    s = await db.get_all_settings()
    main_photo = _file(ui_photo_main_menu(s, config))
    if main_photo:
        try:
            await bot.edit_message_media(
                chat_id=chat_id,
                message_id=message_id,
                media=InputMediaPhoto(media=main_photo, caption=cap, parse_mode=ParseMode.HTML),
                reply_markup=main_menu_kb(),
            )
            return
        except TelegramBadRequest:
            try:
                await bot.delete_message(chat_id, message_id)
            except Exception:
                pass
            await bot.send_photo(
                chat_id=chat_id,
                photo=main_photo,
                caption=cap,
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(),
            )
            return
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=_truncate_html(text, 4096),
            reply_markup=main_menu_kb(),
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest:
        try:
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=message_id,
                caption=cap,
                reply_markup=main_menu_kb(),
                parse_mode=ParseMode.HTML,
            )
        except TelegramBadRequest:
            pass


def _slots_caption(day: str, n_selected: int) -> str:
    return (
        f"<b>📌 Дата:</b> {day}\n\n"
        "Слева — время с полуночи до 11:00, справа — с 12:00 до конца дня.\n"
        "Сверху вниз строка за строкой.\n\n"
        "<b>✅</b> — час свободен  ·  <b>❌</b> — занят\n"
        "<b>☑</b> — выбран (нажмите снова, чтобы снять)\n\n"
        f"<b>Выбрано часов:</b> {n_selected}\n\n"
        "Отметьте один или несколько <b>подряд идущих</b> свободных часов, затем «Далее»."
    )


def _yc_start_caption(day: str) -> str:
    return (
        f"<b>📌 Дата:</b> {day}\n\n"
        "<b>Шаг 1.</b> Выберите <b>время начала</b> (сетка по услуге <b>1 ч</b> в Yclients).\n"
        "Дальше вы выберете длительность <b>1–4 ч</b> — каждый вариант отдельная услуга в CRM, "
        "<b>сумма из прайса Yclients</b>.\n\n"
        "<i>Слева — с полуночи до 11:59, справа — с полудня.</i>"
    )


def _yc_hours_caption(day: str, start_slot_label: str) -> str:
    return (
        f"<b>📌 Дата:</b> {day}\n"
        f"<b>Старт:</b> {start_slot_label}\n\n"
        "<b>Шаг 2.</b> Сколько часов бронируем?"
    )


def _prices_text(cfg: Config, pricing: EffectivePricing) -> str:
    """Прайс с учётом включённых/выключенных услуг в админке."""
    parts: list[str] = ["<b>💳 Прайс студии</b>"]
    has_studio = pricing.service_no_engineer_enabled or pricing.service_with_engineer_enabled

    if has_studio:
        parts.append("")
        parts.append("<b>Почасовая запись</b>")
        if pricing.service_no_engineer_enabled:
            parts.append(f"🎤 1 час без звукорежиссёра — <b>{pricing.price_no_engineer} руб</b>")
        if pricing.service_with_engineer_enabled:
            parts.append(f"🎛️ 1 час с звукорежиссёром — <b>{pricing.price_with_engineer} руб</b>")

    if pricing.service_no_engineer_enabled:
        parts.extend(
            [
                "",
                "<b>Тарифы — без звукорежиссёра</b>",
                "<i>Ночь (с 00:00):</i>",
                f"🌙 6 ч — <b>{pricing.tariff_night_6h} руб</b> · 8 ч — <b>{pricing.tariff_night_8h} руб</b>",
                f"🌙 10 ч — <b>{pricing.tariff_night_10h} руб</b> · 12 ч — <b>{pricing.tariff_night_12h} руб</b>",
                "<i>День (09:00 / 12:00):</i>",
                f"☀️ 6 ч — <b>{pricing.tariff_day_6h} руб</b> · 8 ч — <b>{pricing.tariff_day_8h} руб</b>",
                f"☀️ 10 ч — <b>{pricing.tariff_day_10h} руб</b> · 12 ч — <b>{pricing.tariff_day_12h} руб</b>",
            ]
        )

    if pricing.service_with_engineer_enabled:
        parts.extend(
            [
                "",
                "<b>Тарифы — со звукорежиссёром</b>",
                "<i>Ночь (с 00:00):</i>",
                f"🌙 6 ч — <b>{pricing.tariff_night_6h_engineer} руб</b> · 8 ч — <b>{pricing.tariff_night_8h_engineer} руб</b>",
                f"🌙 10 ч — <b>{pricing.tariff_night_10h_engineer} руб</b> · 12 ч — <b>{pricing.tariff_night_12h_engineer} руб</b>",
                "<i>День (09:00 / 12:00):</i>",
                f"☀️ 6 ч — <b>{pricing.tariff_day_6h_engineer} руб</b> · 8 ч — <b>{pricing.tariff_day_8h_engineer} руб</b>",
                f"☀️ 10 ч — <b>{pricing.tariff_day_10h_engineer} руб</b> · 12 ч — <b>{pricing.tariff_day_12h_engineer} руб</b>",
            ]
        )

    extra: list[str] = []
    if pricing.service_lyrics_enabled:
        extra.append(f"📝 Текст для вашей песни — <b>{pricing.price_lyrics} руб</b>")
    if pricing.service_beat_enabled:
        extra.append(f"🎚️ Бит для песни — <b>{pricing.price_beat} руб</b>")
    if extra:
        parts.extend(["", "<b>Услуги</b>", *extra])

    if not has_studio and not extra:
        parts.append("")
        parts.append("<i>Сейчас нет доступных позиций в прайсе — уточните у администратора.</i>")

    return "\n".join(parts).strip()


_ACTIVITY_SEP = "\n\n───────────────\n\n"
_ACTIVITY_HEADER = "<b>✅ Оформленные заявки</b>\n\n"
_ACTIVITY_FOOTER = "\n\n<b>🏠 Главное меню</b> — выберите действие ниже."
_MAX_ACTIVITY_BODY_STORE = 10000


def _activity_caption_pair(notice_html: str, body_html: str) -> tuple[str, str]:
    notice = (notice_html or "").strip()
    body = (body_html or "").strip()
    if not body:
        body = "<i>Нет активных заявок в этом списке.</i>"
    core = _ACTIVITY_HEADER + body + _ACTIVITY_FOOTER
    full = f"{notice}\n\n{core}" if notice else core
    return _truncate_html(full, 1024), _truncate_html(full, 4096)


async def _render_user_activity_message(bot, db: Database, config: Config, user_id: int) -> None:
    row = await db.get_user_activity_message(user_id)
    if not row:
        return
    await _sync_user_activity_body_html(
        bot, db, config, user_id=user_id, inner_body_html=str(row.get("body_html") or "")
    )


async def _upsert_user_success_summary(
    bot,
    db: Database,
    *,
    user_id: int,
    chat_id: int,
    config: Config,
    new_entry_html: str,
) -> None:
    """
    Одно сообщение на пользователя с накоплением заявок.
    Если задано фото главного меню — всегда оно (и при обновлении подменяем медиа, в т.ч. после экрана оплаты с фото).
    Иначе — текст до 4096 символов.
    """
    row = await db.get_user_activity_message(user_id)
    old = (row.get("body_html") or "").strip() if row else ""
    body = old + _ACTIVITY_SEP + new_entry_html if old else new_entry_html
    if len(body) > _MAX_ACTIVITY_BODY_STORE:
        body = "…\n\n" + body[-_MAX_ACTIVITY_BODY_STORE:]

    caption_display, text_display = _activity_caption_pair("", body)

    prev_mid = int(row["message_id"]) if row else None
    prev_chat = int(row["chat_id"]) if row else chat_id

    s = await db.get_all_settings()
    main_photo = _file(ui_photo_main_menu(s, config))

    if main_photo:
        media = InputMediaPhoto(
            media=main_photo,
            caption=caption_display,
            parse_mode=ParseMode.HTML,
        )
        if prev_mid:
            try:
                await bot.edit_message_media(
                    chat_id=prev_chat,
                    message_id=prev_mid,
                    media=media,
                    reply_markup=main_menu_kb(),
                )
                await db.upsert_user_activity_message(
                    user_id, prev_chat, prev_mid, body, notice_html=""
                )
                return
            except TelegramBadRequest as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    try:
                        await bot.edit_message_caption(
                            chat_id=prev_chat,
                            message_id=prev_mid,
                            caption=caption_display,
                            reply_markup=main_menu_kb(),
                            parse_mode=ParseMode.HTML,
                        )
                    except TelegramBadRequest:
                        pass
                    await db.upsert_user_activity_message(
                        user_id, prev_chat, prev_mid, body, notice_html=""
                    )
                    return
                # нельзя сменить медиа (например, прошлое сообщение было текстом) — удаляем и шлём фото
            except Exception:
                pass
            try:
                await bot.delete_message(prev_chat, prev_mid)
            except Exception:
                pass
        msg = await bot.send_photo(
            chat_id,
            photo=main_photo,
            caption=caption_display,
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(),
        )
        await db.upsert_user_activity_message(
            user_id, chat_id, int(msg.message_id), body, notice_html=""
        )
        return

    if prev_mid:
        try:
            await bot.edit_message_text(
                chat_id=prev_chat,
                message_id=prev_mid,
                text=text_display,
                reply_markup=main_menu_kb(),
                parse_mode=ParseMode.HTML,
            )
            await db.upsert_user_activity_message(
                user_id, prev_chat, prev_mid, body, notice_html=""
            )
            return
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await db.upsert_user_activity_message(
                    user_id, prev_chat, prev_mid, body, notice_html=""
                )
                return
        except Exception:
            pass
        try:
            await bot.delete_message(prev_chat, prev_mid)
        except Exception:
            pass

    msg = await bot.send_message(
        chat_id,
        text_display,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(),
    )
    await db.upsert_user_activity_message(
        user_id, chat_id, int(msg.message_id), body, notice_html=""
    )


async def _sync_user_activity_body_html(
    bot: Bot,
    db: Database,
    config: Config,
    *,
    user_id: int,
    inner_body_html: str,
) -> None:
    """Обновляет сообщение «Оформленные заявки» с новым телом (без добавления строки)."""
    row = await db.get_user_activity_message(user_id)
    if not row:
        return
    body = (inner_body_html or "").strip()
    if not body:
        body = "<i>Нет активных заявок в этом списке.</i>"
    if len(body) > _MAX_ACTIVITY_BODY_STORE:
        body = "…\n\n" + body[-_MAX_ACTIVITY_BODY_STORE:]

    notice = (row.get("notice_html") or "").strip()
    caption_display, text_display = _activity_caption_pair(notice, body)

    prev_mid = int(row["message_id"])
    prev_chat = int(row["chat_id"])
    chat_id = prev_chat

    s = await db.get_all_settings()
    main_photo = _file(ui_photo_main_menu(s, config))

    if main_photo:
        media = InputMediaPhoto(
            media=main_photo,
            caption=caption_display,
            parse_mode=ParseMode.HTML,
        )
        try:
            await bot.edit_message_media(
                chat_id=prev_chat,
                message_id=prev_mid,
                media=media,
                reply_markup=main_menu_kb(),
            )
            await db.upsert_user_activity_message(user_id, prev_chat, prev_mid, body)
            return
        except TelegramBadRequest as e:
            err = str(e).lower()
            if "message is not modified" in err:
                try:
                    await bot.edit_message_caption(
                        chat_id=prev_chat,
                        message_id=prev_mid,
                        caption=caption_display,
                        reply_markup=main_menu_kb(),
                        parse_mode=ParseMode.HTML,
                    )
                except TelegramBadRequest:
                    pass
                await db.upsert_user_activity_message(user_id, prev_chat, prev_mid, body)
                return
        except Exception:
            pass
        try:
            await bot.delete_message(prev_chat, prev_mid)
        except Exception:
            pass
        msg = await bot.send_photo(
            chat_id,
            photo=main_photo,
            caption=caption_display,
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(),
        )
        await db.upsert_user_activity_message(user_id, chat_id, int(msg.message_id), body)
        return

    try:
        await bot.edit_message_text(
            chat_id=prev_chat,
            message_id=prev_mid,
            text=text_display,
            reply_markup=main_menu_kb(),
            parse_mode=ParseMode.HTML,
        )
        await db.upsert_user_activity_message(user_id, prev_chat, prev_mid, body)
        return
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            await db.upsert_user_activity_message(user_id, prev_chat, prev_mid, body)
            return
    except Exception:
        pass
    try:
        await bot.delete_message(prev_chat, prev_mid)
    except Exception:
        pass

    msg = await bot.send_message(
        chat_id,
        text_display,
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(),
    )
    await db.upsert_user_activity_message(user_id, chat_id, int(msg.message_id), body)


async def remove_booking_from_user_activity(
    bot: Bot,
    db: Database,
    config: Config,
    *,
    user_id: int,
    booking_id: int,
) -> None:
    """Убирает блок заявки студии или текст/бит из сводки «Оформленные заявки»."""
    row = await db.get_user_activity_message(user_id)
    if not row:
        return
    old = (row.get("body_html") or "").strip()
    markers = (f"<b>Запись #{booking_id}</b>", f"<b>Заявка #{booking_id}</b>")
    parts = old.split(_ACTIVITY_SEP)
    kept = [p for p in parts if not any(m in p for m in markers)]
    new_body = _ACTIVITY_SEP.join(kept).strip()
    if new_body == old:
        return
    await _sync_user_activity_body_html(
        bot, db, config, user_id=user_id, inner_body_html=new_body
    )


async def remove_service_order_from_user_activity(
    bot: Bot,
    db: Database,
    config: Config,
    *,
    user_id: int,
    booking_id: int,
) -> None:
    await remove_booking_from_user_activity(
        bot, db, config, user_id=user_id, booking_id=booking_id
    )


async def _post_schedule_to_channel(bot, db: Database, cfg: Config, day: str) -> None:
    s = await db.get_all_settings()
    sch_id = effective_schedule_channel_id(s, cfg)
    if not sch_id:
        return
    if not day or day == "service":
        return
    schedule = await db.get_day_schedule(day)
    lines = [f"<b>📅 Расписание на {day}</b>"]
    for row in schedule:
        if row["is_active"]:
            lines.append(f"🟢 {row['start_time']} - {row['end_time']} свободно")
        else:
            lines.append(
                f"🔴 {row['start_time']} - {row['end_time']} занято "
                f"({row.get('user_name','')} {row.get('phone','')})"
            )
    await bot.send_message(sch_id, "\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(F.text == "/start")
async def start(message: Message, state: FSMContext, config: Config, db: Database) -> None:
    await state.clear()
    await db.delete_user_activity_message(message.from_user.id)
    caption = "<b>🎧 Студия звукозаписи</b>\nВыберите действие в меню ниже."
    s = await db.get_all_settings()
    photo = _file(ui_photo_main_menu(s, config))
    if photo:
        await message.answer_photo(photo=photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())
    else:
        await message.answer(caption, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())


@router.callback_query(F.data == "menu:home")
async def menu_home(callback: CallbackQuery, state: FSMContext, config: Config, db: Database) -> None:
    data = await state.get_data()
    prompt_id = data.get("brief_prompt_message_id")
    root_id = data.get("payment_root_message_id") or data.get("brief_root_message_id")
    chat_id = callback.message.chat.id
    uid = callback.from_user.id

    if prompt_id:
        try:
            await callback.bot.delete_message(chat_id, prompt_id)
        except Exception:
            pass

    # Ушли в меню, не оплатив ЮKassa — отменяем «висящую» бронь и освобождаем слоты.
    cur_st = await state.get_state()
    if cur_st == BookingStates.awaiting_payment_confirm.state:
        bid = data.get("pending_booking_id")
        if bid is not None:
            b = await db.get_booking_by_id(int(bid))
            if (
                b
                and int(b["user_id"]) == uid
                and (b.get("status") or "") == "awaiting_yookassa"
            ):
                try:
                    await delete_booking_pending_ui_messages(callback.bot, dict(b), db)
                except Exception:
                    pass
                try:
                    await db.cancel_booking(int(bid))
                except Exception:
                    logger.exception("cancel awaiting_yookassa on menu_home bid=%s", bid)
                try:
                    await _publish_weekly_and_tasks(callback.bot, db, config)
                except Exception:
                    pass

    await state.clear()

    act = await db.get_user_activity_message(uid)
    if act:
        await _render_user_activity_message(callback.bot, db, config, uid)
        try:
            await callback.answer()
        except Exception:
            pass
        return

    target_mid = int(root_id) if root_id else int(callback.message.message_id)
    await _present_main_menu_on_message(
        callback.bot,
        chat_id=chat_id,
        message_id=target_mid,
        config=config,
        db=db,
    )
    try:
        await callback.answer()
    except Exception:
        pass


@router.callback_query(F.data == "menu:prices")
async def menu_prices(
    callback: CallbackQuery, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    s = await db.get_all_settings()
    prices_photo = _file(ui_photo_prices(s, config))
    if getattr(callback.message, "photo", None) and prices_photo:
        try:
            await callback.message.edit_media(
                InputMediaPhoto(media=prices_photo, caption=_prices_text(config, pricing), parse_mode=ParseMode.HTML),
                reply_markup=back_to_menu_kb(),
            )
        except Exception:
            # Если caption не меняется — покажем алерт, иначе пользователь видит “пустое фото”
            try:
                await callback.message.edit_caption(
                    _prices_text(config, pricing),
                    reply_markup=back_to_menu_kb(),
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                await callback.answer("Не удалось обновить прайсы. Нажмите /start и попробуйте снова.", show_alert=True)
                return
    else:
        await _edit(callback.message, _prices_text(config, pricing), reply_markup=back_to_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:equipment")
async def menu_equipment(callback: CallbackQuery, config: Config, db: Database) -> None:
    settings = await db.get_all_settings()
    caption = await equipment_caption_html(db, config)
    paths = equipment_photo_paths(settings, config)
    photos = [p for p in paths if _file(p)]
    total = len(photos)

    # В одном сообщении можно менять ТОЛЬКО media/подпись. Альбом (media_group) всегда создаёт новые сообщения.
    if not getattr(callback.message, "photo", None):
        await _edit(
            callback.message,
            caption + "\n\n<i>⚠️ Для показа фото нужно, чтобы главное меню было с фото (MAIN_MENU_PHOTO_PATH).</i>",
            reply_markup=equipment_carousel_kb(0, max(total, 1)),
        )
        await callback.answer()
        return

    if total == 0:
        await callback.message.edit_caption(
            caption + "\n\n<i>⚠️ Фото не настроены.</i>",
            reply_markup=equipment_carousel_kb(0, 1),
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()
        return

    first_photo = _file(photos[0])
    cap = _truncate_html(caption, 1024)
    # Подпись обязательно в том же вызове, что и media — иначе Telegram сбрасывает caption.
    await callback.message.edit_media(
        media=InputMediaPhoto(
            media=first_photo,
            caption=cap,
            parse_mode=ParseMode.HTML,
        ),
        reply_markup=equipment_carousel_kb(0, total),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("equip:"))
async def equipment_nav(callback: CallbackQuery, config: Config, db: Database) -> None:
    if not getattr(callback.message, "photo", None):
        await callback.answer()
        return
    try:
        idx = int(callback.data.split(":", 1)[1])
    except Exception:
        await callback.answer()
        return

    settings = await db.get_all_settings()
    paths = equipment_photo_paths(settings, config)
    photos = [p for p in paths if _file(p)]
    total = len(photos)
    if total == 0:
        await callback.answer("Фото не настроены", show_alert=True)
        return
    idx = idx % total
    photo = _file(photos[idx])
    cap = _truncate_html(await equipment_caption_html(db, config), 1024)
    await callback.message.edit_media(
        media=InputMediaPhoto(
            media=photo,
            caption=cap,
            parse_mode=ParseMode.HTML,
        ),
        reply_markup=equipment_carousel_kb(idx, total),
    )
    await callback.answer()


@router.callback_query(F.data == "book:start")
async def booking_start(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    await state.clear()
    user_id = callback.from_user.id
    try:
        dropped = await db.cancel_all_awaiting_yookassa_for_user(user_id)
        if dropped:
            for bid in dropped:
                pop_yookassa_payments_for_booking(bid)
            try:
                await _publish_weekly_and_tasks(callback.bot, db, config)
            except Exception:
                logger.exception(
                    "_publish_weekly_and_tasks after cancel awaiting_yookassa uid=%s bids=%s",
                    user_id,
                    dropped,
                )
    except Exception:
        logger.exception("cancel_all_awaiting_yookassa_for_user uid=%s", user_id)
    await state.update_data(root_message_id=callback.message.message_id)
    await state.update_data(tg_username=callback.from_user.username or "")

    s = await db.get_all_settings()
    sub_ch = effective_subscription_channel_id(s, config)
    sub_link = effective_subscription_channel_link(s, config)
    if not await is_subscribed(callback.bot, sub_ch, user_id):
        await _edit(
            callback.message,
            "Для записи необходимо подписаться на канал",
            reply_markup=subscription_kb(sub_link),
        )
        await callback.answer()
        return

    await state.set_state(BookingStates.choosing_product)
    await _edit(callback.message, "<b>Выберите услугу</b>", reply_markup=booking_products_kb(pricing=pricing))
    await callback.answer()


@router.callback_query(F.data.startswith("prod:"), BookingStates.choosing_product)
async def pick_product(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    code = callback.data.split(":", 1)[1]
    if code == "lyrics" and not pricing.service_lyrics_enabled:
        await callback.answer("Услуга временно недоступна.", show_alert=True)
        return
    if code == "beat" and not pricing.service_beat_enabled:
        await callback.answer("Услуга временно недоступна.", show_alert=True)
        return
    if code == "no_engineer" and not pricing.service_no_engineer_enabled:
        await callback.answer("Услуга временно недоступна.", show_alert=True)
        return
    if code == "with_engineer" and not pricing.service_with_engineer_enabled:
        await callback.answer("Услуга временно недоступна.", show_alert=True)
        return
    await state.update_data(product=code)

    if code in ("no_engineer", "with_engineer"):
        if await db.user_has_active_studio_booking(callback.from_user.id):
            await callback.answer(
                "У вас уже есть активная запись на студию. Откройте «📅 Моя запись» или отмените её, "
                "чтобы выбрать новую дату. Заявки на текст и бит доступны отдельно.",
                show_alert=True,
            )
            return
        await show_studio_mode(callback, state, config, db)
        await callback.answer()
        return

    # Текст/бит: одно сообщение — описание и экран оплаты правятся через edit.
    price = pricing.service_price(code)
    await state.update_data(pay_online=False)
    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass
    s = await db.get_all_settings()
    pay_ph = ui_photo_payment(s, config)
    if is_yookassa_configured(config):
        await state.set_state(BookingStates.choosing_pay_method)
        screen = (
            "<b>📝 Заказ: текст или бит</b>\n\n"
            f"<b>Стоимость:</b> {price} руб\n\n"
            "<b>Как будете оплачивать?</b>\n"
            "• <b>Онлайн (ЮKassa)</b> — после описания заказа откроется ссылка на оплату; "
            "фамилию и банк указывать не нужно.\n"
            "• <b>Перевод на карту</b> — в конце сообщения понадобятся имя, фамилия, банк и @username.\n\n"
            "Выберите вариант ниже."
        )
        kb = payment_method_kb()
    else:
        await state.set_state(BookingStates.entering_brief)
        screen = (
            "<b>📝 Опишите заказ</b>\n\n"
            "Отправьте <b>несколько строк</b> в таком порядке:\n"
            "• сначала — <b>пожелания</b> к заказу (можно несколько строк);\n"
            "• затем <b>4 отдельные строки</b>:\n"
            "  — имя\n"
            "  — фамилия\n"
            "  — банк (с какого будет оплата)\n"
            "  — ваш @username в Telegram (или «—» если без username)\n\n"
            f"<b>Стоимость:</b> {price} руб"
        )
        kb = back_to_menu_kb()
    sent_id, is_photo = await _send_payment_screen_message(
        callback.bot,
        chat_id=chat_id,
        text=screen,
        reply_markup=kb,
        photo_path=pay_ph,
    )
    await state.update_data(
        payment_root_message_id=sent_id,
        payment_root_is_photo=is_photo,
        cleanup_ids=[],
    )


@router.callback_query(F.data == "sub:check")
async def sub_check(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
) -> None:
    s = await db.get_all_settings()
    sub_ch = effective_subscription_channel_id(s, config)
    if await is_subscribed(callback.bot, sub_ch, callback.from_user.id):
        data = await state.get_data()
        prod = data.get("product")
        if prod is None:
            await callback.answer("Подписка подтверждена", show_alert=True)
            await state.update_data(tg_username=callback.from_user.username or "")
            await state.set_state(BookingStates.choosing_product)
            await _edit(
                callback.message,
                "<b>Выберите услугу</b>",
                reply_markup=booking_products_kb(pricing=pricing),
            )
            return
        if prod == "no_engineer" and not pricing.service_no_engineer_enabled:
            await callback.answer("Запись без звукорежиссёра сейчас недоступна.", show_alert=True)
            return
        if prod == "with_engineer" and not pricing.service_with_engineer_enabled:
            await callback.answer("Запись со звукорежиссёром сейчас недоступна.", show_alert=True)
            return
        if prod in ("no_engineer", "with_engineer"):
            if await db.user_has_active_studio_booking(callback.from_user.id):
                await callback.answer(
                    "У вас уже есть активная запись на студию. Сначала отмените её в «Моя запись».",
                    show_alert=True,
                )
                return
        await callback.answer("Подписка подтверждена", show_alert=True)
        if prod in ("no_engineer", "with_engineer"):
            await show_studio_mode(callback, state, config, db)
        else:
            await show_calendar(callback, state, db, config)
        return
    await callback.answer("Подписка не найдена", show_alert=True)


async def show_studio_mode(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    """Почасовая запись или тарифы (после выбора записи с/без звукорежиссёра)."""
    await state.set_state(BookingStates.choosing_studio_mode)
    await state.update_data(booking_mode=None, tariff_label=None)
    s = await db.get_all_settings()
    day_lbl = html_escape(", ".join(tariff_day_start_list(s)))
    night_lbl = html_escape(", ".join(tariff_night_start_list(s)))
    await _edit_screen(
        callback,
        "<b>Запись на студию</b>\n\n"
        "Выберите режим:\n"
        "• <b>Почасовая запись</b> — сами отмечаете нужные часы.\n"
        "• <b>Тарифы</b> — пакеты на 6 / 8 / 10 / 12 ч: "
        f"<b>ночная</b> (старт: {night_lbl}) или <b>дневная</b> (старт: {day_lbl}).",
        studio_mode_kb(),
        photo_path=ui_photo_main_menu(s, config),
    )


async def show_calendar(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    year: int | None = None,
    month: int | None = None,
) -> None:
    if year is None or month is None:
        year, month = now_month()
    data = await state.get_data()
    use_yc = yc.yclients_studio_enabled(config) and (data.get("product") in ("no_engineer", "with_engineer"))
    if use_yc:
        try:
            yc_days = set(await yc.available_days_in_window(config))
        except YclientsError as e:
            logger.warning("Yclients show_calendar: %s", e)
            await callback.answer(
                "Не удалось загрузить расписание Yclients. Попробуйте позже или отключите YCLIENTS_STUDIO.",
                show_alert=True,
            )
            return
        available_days = yc_days
    else:
        available_days = set(await db.get_available_days())
    await state.set_state(BookingStates.choosing_date)
    await state.update_data(
        cal_year=year,
        cal_month=month,
        booking_mode="hourly",
        yclients_studio=False,
        yclients_seances=[],
        yclients_hour_pack=None,
        yclients_start_idx=None,
    )

    closed_admin = await db.get_closed_days_in_month(year, month)
    blocked: set[str] = set(closed_admin)
    if not use_yc and data.get("product") == "with_engineer":
        available_days = await db.filter_days_for_engineer_booking(available_days)
        blocked |= await db.get_engineer_unavailable_days_in_month(year, month)
    if use_yc and data.get("product") == "with_engineer":
        blocked |= await db.get_engineer_unavailable_days_in_month(year, month)
    await _edit(
        callback.message,
        "<b>📅 Выберите дату записи</b>\n\n"
        "Число — есть свободные слоты\n"
        "❌ — недоступно (прошлые даты, нет звукорежиссёра в этот день, день закрыт студией)\n\n"
        "<i>Доступны даты с сегодня до конца следующего календарного месяца.</i>",
        reply_markup=month_calendar_kb(year, month, allowed_days=available_days, blocked_days=blocked),
    )


async def show_tariff_calendar(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    *,
    year: int | None = None,
    month: int | None = None,
) -> None:
    data = await state.get_data()
    start_hhmm = str(data.get("tariff_start") or "00:00")
    hours = int(data.get("tariff_hours") or 0)
    product = data.get("product")
    if product not in ("no_engineer", "with_engineer") or hours < 1:
        return
    require_engineer = product == "with_engineer"
    allowed = await db.get_days_with_free_tariff_block(
        start_hhmm=start_hhmm,
        hour_count=hours,
        require_engineer=require_engineer,
    )
    if year is None or month is None:
        year, month = now_month()
    await state.update_data(cal_year=year, cal_month=month)
    st_label, end_label = Database.tariff_time_range_label(start_hhmm, hours)
    caption = (
        f"<b>Ваше время: {st_label} — {end_label}</b>\n\n"
        "<b>📅 Выберите дату</b>\n\n"
        "Число — весь интервал свободен для брони\n"
        "❌ — нельзя выбрать (прошлые дни, занято, день закрыт студией или нет звукорежиссёра)\n\n"
        "<i>После выбора даты блокируется весь указанный отрезок.</i>"
    )
    closed_admin = await db.get_closed_days_in_month(year, month)
    engineer_blocked: set[str] = (
        await db.get_engineer_unavailable_days_in_month(year, month) if require_engineer else set()
    )
    mk = month_calendar_kb(
        year,
        month,
        allowed_days=allowed,
        blocked_days=closed_admin | engineer_blocked,
        prefix="tdate",
        nav_prefix="tcal",
    )
    rows = list(mk.inline_keyboard)
    rows.append([InlineKeyboardButton(text="⬅ К выбору часов", callback_data="trf:cal:back")])
    kind = data.get("tariff_kind")
    s = await db.get_all_settings()
    tariff_photo = (
        ui_photo_tariff_night(s, config)
        if kind == "night"
        else ui_photo_tariff_day(s, config)
    )
    await _edit_screen(
        callback,
        caption,
        InlineKeyboardMarkup(inline_keyboard=rows),
        photo_path=tariff_photo,
    )


@router.callback_query(F.data == "book:calendar")
async def back_to_calendar(callback: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    data = await state.get_data()
    await show_calendar(
        callback, state, db, config, year=data.get("cal_year"), month=data.get("cal_month")
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cal:"))
async def calendar_nav(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    payload = callback.data.split(":", maxsplit=1)[1]
    if payload == "today":
        y, m = now_month()
    else:
        y_str, m_str = payload.split("-")
        y, m = int(y_str), int(m_str)
    await show_calendar(callback, state, db, config, year=y, month=m)
    await callback.answer()


@router.callback_query(F.data == "book:pick_product")
async def back_to_pick_product(callback: CallbackQuery, state: FSMContext, pricing: EffectivePricing) -> None:
    await state.set_state(BookingStates.choosing_product)
    await _edit(callback.message, "<b>Выберите услугу</b>", reply_markup=booking_products_kb(pricing=pricing))
    await callback.answer()


@router.callback_query(F.data == "stm:hourly", BookingStates.choosing_studio_mode)
async def studio_mode_hourly(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    await state.update_data(booking_mode="hourly", tariff_label=None)
    await show_calendar(callback, state, db, config)
    await callback.answer()


@router.callback_query(F.data == "stm:tariff", BookingStates.choosing_studio_mode)
async def studio_mode_tariff(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    await state.set_state(BookingStates.choosing_tariff)
    await state.update_data(
        tariff_kind=None,
        tariff_hours=None,
        tariff_day_start=None,
        tariff_night_start=None,
        tariff_start=None,
        tariff_label=None,
    )
    s = await db.get_all_settings()
    await _edit_screen(
        callback,
        "<b>📦 Тарифы</b>\n\nВыберите: ночная или дневная запись.",
        tariff_category_kb(),
        photo_path=ui_photo_tariff_category(s, config),
    )
    await callback.answer()


@router.callback_query(F.data == "stm:back", BookingStates.choosing_tariff)
async def stm_back_to_studio_mode(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    await show_studio_mode(callback, state, config, db)
    await callback.answer()


@router.callback_query(F.data == "trf:c:night", BookingStates.choosing_tariff)
async def tariff_cat_night(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
) -> None:
    await state.update_data(tariff_kind="night", tariff_night_start=None)
    data = await state.get_data()
    we = data.get("product") == "with_engineer"
    s = await db.get_all_settings()
    night_times = tariff_night_start_list(s)
    if len(night_times) == 1:
        st0 = night_times[0]
        await state.update_data(tariff_night_start=st0)
        await _edit_screen(
            callback,
            "<b>🌙 Ночная запись</b>\n\n"
            f"Старт в <b>{html_escape(st0)}</b>. Например: 6 ч — шесть часов подряд с этого времени.\n\n"
            "Выберите длительность:",
            tariff_hours_kb(night=True, pricing=pricing, with_engineer=we, night_back_callback="trf:c:back"),
            photo_path=ui_photo_tariff_night(s, config),
        )
    else:
        await _edit_screen(
            callback,
            "<b>🌙 Ночная запись</b>\n\nС какого времени начинается сессия?",
            tariff_night_start_kb(night_times),
            photo_path=ui_photo_tariff_night(s, config),
        )
    await callback.answer()


@router.callback_query(F.data == "trf:c:day", BookingStates.choosing_tariff)
async def tariff_cat_day(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    await state.update_data(tariff_kind="day")
    s = await db.get_all_settings()
    day_times = tariff_day_start_list(s)
    await _edit_screen(
        callback,
        "<b>☀️ Дневная запись</b>\n\nС какого времени начинается сессия?",
        tariff_day_start_kb(day_times),
        photo_path=ui_photo_tariff_day(s, config),
    )
    await callback.answer()


@router.callback_query(F.data == "trf:c:back", BookingStates.choosing_tariff)
async def tariff_cat_back(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    s = await db.get_all_settings()
    await _edit_screen(
        callback,
        "<b>📦 Тарифы</b>\n\nВыберите: ночная или дневная запись.",
        tariff_category_kb(),
        photo_path=ui_photo_tariff_category(s, config),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("trf:s:"), BookingStates.choosing_tariff)
async def tariff_day_start_pick(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
) -> None:
    part = callback.data.split(":")[2]
    s = await db.get_all_settings()
    day_times = tariff_day_start_list(s)
    start: str | None = None
    if part == "09":
        start = "09:00"
    elif part == "12":
        start = "12:00"
    else:
        try:
            idx = int(part)
            if 0 <= idx < len(day_times):
                start = day_times[idx]
        except ValueError:
            pass
    if not start:
        await callback.answer()
        return
    await state.update_data(tariff_day_start=start)
    data = await state.get_data()
    we = data.get("product") == "with_engineer"
    await _edit_screen(
        callback,
        f"<b>☀️ Дневная запись</b> (начало в {html_escape(start)})\n\nВыберите длительность:",
        tariff_hours_kb(night=False, pricing=pricing, with_engineer=we),
        photo_path=ui_photo_tariff_day(s, config),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("trf:n:"), BookingStates.choosing_tariff)
async def tariff_night_start_pick(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
) -> None:
    part = callback.data.split(":")[2]
    try:
        idx = int(part)
    except ValueError:
        await callback.answer()
        return
    s = await db.get_all_settings()
    nts = tariff_night_start_list(s)
    if idx < 0 or idx >= len(nts):
        await callback.answer()
        return
    start = nts[idx]
    await state.update_data(tariff_night_start=start)
    data = await state.get_data()
    we = data.get("product") == "with_engineer"
    night_back = "trf:nback" if len(nts) > 1 else "trf:c:back"
    await _edit_screen(
        callback,
        f"<b>🌙 Ночная запись</b> (начало в {html_escape(start)})\n\nВыберите длительность:",
        tariff_hours_kb(
            night=True, pricing=pricing, with_engineer=we, night_back_callback=night_back
        ),
        photo_path=ui_photo_tariff_night(s, config),
    )
    await callback.answer()


@router.callback_query(F.data == "trf:nback", BookingStates.choosing_tariff)
async def tariff_night_start_back(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    s = await db.get_all_settings()
    nts = tariff_night_start_list(s)
    if len(nts) <= 1:
        await _edit_screen(
            callback,
            "<b>📦 Тарифы</b>\n\nВыберите: ночная или дневная запись.",
            tariff_category_kb(),
            photo_path=ui_photo_tariff_category(s, config),
        )
        await callback.answer()
        return
    await state.update_data(tariff_night_start=None)
    await _edit_screen(
        callback,
        "<b>🌙 Ночная запись</b>\n\nС какого времени начинается сессия?",
        tariff_night_start_kb(nts),
        photo_path=ui_photo_tariff_night(s, config),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("trf:h:"), BookingStates.choosing_tariff)
async def tariff_pick_hours(callback: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    payload = callback.data.split(":")[2]
    if payload == "back":
        data = await state.get_data()
        s = await db.get_all_settings()
        if data.get("tariff_kind") == "day":
            await _edit_screen(
                callback,
                "<b>☀️ Дневная запись</b>\n\nС какого времени начинается сессия?",
                tariff_day_start_kb(tariff_day_start_list(s)),
                photo_path=ui_photo_tariff_day(s, config),
            )
        elif data.get("tariff_kind") == "night":
            nts = tariff_night_start_list(s)
            if len(nts) > 1:
                await _edit_screen(
                    callback,
                    "<b>🌙 Ночная запись</b>\n\nС какого времени начинается сессия?",
                    tariff_night_start_kb(nts),
                    photo_path=ui_photo_tariff_night(s, config),
                )
            else:
                await _edit_screen(
                    callback,
                    "<b>📦 Тарифы</b>\n\nВыберите: ночная или дневная запись.",
                    tariff_category_kb(),
                    photo_path=ui_photo_tariff_category(s, config),
                )
        await callback.answer()
        return
    try:
        hours = int(payload)
    except ValueError:
        await callback.answer()
        return
    if hours not in (6, 8, 10, 12):
        await callback.answer()
        return
    data = await state.get_data()
    kind = data.get("tariff_kind")
    s_opts = await db.get_all_settings()
    if kind == "night":
        nts = tariff_night_start_list(s_opts)
        start = data.get("tariff_night_start")
        if not start:
            if len(nts) == 1:
                start = nts[0]
                await state.update_data(tariff_night_start=start)
            else:
                await callback.answer("Сначала выберите время начала ночной сессии.", show_alert=True)
                return
        start = str(start)
    elif kind == "day":
        start = data.get("tariff_day_start")
        if not start:
            dts = tariff_day_start_list(s_opts)
            if dts:
                start = dts[0]
                await state.update_data(tariff_day_start=start)
            else:
                await callback.answer("Сначала выберите время начала дня.", show_alert=True)
                return
        start = str(start)
    else:
        await callback.answer()
        return
    await state.update_data(tariff_hours=hours, tariff_start=start)
    await state.set_state(BookingStates.choosing_tariff_date)
    await show_tariff_calendar(callback, state, db, config)
    await callback.answer()


@router.callback_query(F.data == "trf:cal:back", BookingStates.choosing_tariff_date)
async def tariff_cal_back(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
) -> None:
    await state.update_data(tariff_hours=None, tariff_start=None)
    await state.set_state(BookingStates.choosing_tariff)
    data = await state.get_data()
    we = data.get("product") == "with_engineer"
    kind = data.get("tariff_kind")
    s = await db.get_all_settings()
    if kind == "night":
        nts = tariff_night_start_list(s)
        if len(nts) > 1:
            await _edit_screen(
                callback,
                "<b>🌙 Ночная запись</b>\n\nС какого времени начинается сессия?",
                tariff_night_start_kb(nts),
                photo_path=ui_photo_tariff_night(s, config),
            )
        else:
            st0 = nts[0] if nts else "00:00"
            await state.update_data(tariff_night_start=st0)
            nb = "trf:c:back"
            await _edit_screen(
                callback,
                f"<b>🌙 Ночная запись</b> (начало в {html_escape(st0)})\n\nВыберите длительность:",
                tariff_hours_kb(
                    night=True, pricing=pricing, with_engineer=we, night_back_callback=nb
                ),
                photo_path=ui_photo_tariff_night(s, config),
            )
    elif kind == "day":
        dts = tariff_day_start_list(s)
        start = data.get("tariff_day_start") or (dts[0] if dts else "09:00")
        await _edit_screen(
            callback,
            f"<b>☀️ Дневная запись</b> (начало в {html_escape(str(start))})\n\nВыберите длительность:",
            tariff_hours_kb(night=False, pricing=pricing, with_engineer=we),
            photo_path=ui_photo_tariff_day(s, config),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("tcal:"), BookingStates.choosing_tariff_date)
async def tariff_calendar_nav(callback: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    payload = callback.data.split(":", maxsplit=1)[1]
    y_str, m_str = payload.split("-")
    y, m = int(y_str), int(m_str)
    await show_tariff_calendar(callback, state, db, config, year=y, month=m)
    await callback.answer()


async def _send_studio_pay_contact_screen(
    bot: Bot,
    *,
    chat_id: int,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
    day: str,
    slot_text: str,
    hours: int,
    total: int,
    product: str,
    tariff_label: str | None,
    hours_display: str | None = None,
) -> None:
    await state.update_data(pay_online=False)
    svc_line = html_escape(str(tariff_label or pricing.service_title(product)))
    h_str = (hours_display or "").strip() or str(hours)
    base_core = "\n".join(
        [
            f"<b>Услуга:</b> {svc_line}",
            f"<b>Дата:</b> {day}",
            f"<b>Время:</b> {slot_text}",
            f"<b>Длит./часов (тариф):</b> {h_str}",
            f"<b>Итого:</b> {total} руб",
        ]
    )
    s = await db.get_all_settings()
    pay_ph = ui_photo_payment(s, config)
    if is_yookassa_configured(config):
        await state.set_state(BookingStates.choosing_pay_method)
        text = (
            "<b>💳 Оплата записи</b>\n\n"
            f"{base_core}\n\n"
            "<b>Способ оплаты</b>\n"
            "• <b>Онлайн (ЮKassa)</b> — после ввода данных откроется ссылка; фамилию и банк указывать не нужно.\n"
            "• <b>Перевод на карту</b> — реквизиты и форма: имя, фамилия, банк, @username.\n\n"
            "Выберите вариант ниже."
        )
        sent_id, is_photo = await _send_payment_screen_message(
            bot,
            chat_id=chat_id,
            text=text,
            reply_markup=payment_method_kb(),
            photo_path=pay_ph,
        )
    else:
        await state.set_state(BookingStates.entering_contacts)
        dest = payment_destination_block_html(config, bank_transfer=True, settings=s)
        text = (
            "<b>💳 Реквизиты для оплаты</b>\n\n"
            f"{base_core}\n\n"
            f"{dest}\n\n"
            "<b>Далее отправьте одним сообщением (4 строки):</b>\n"
            "• имя\n"
            "• фамилия\n"
            "• банк (с какого будет оплата)\n"
            "• ваш @username в Telegram (или «—»)\n\n"
            "<i>Пример:\n"
            "Иван\n"
            "Иванов\n"
            "Сбербанк\n"
            "@nickname</i>"
        )
        sent_id, is_photo = await _send_payment_screen_message(
            bot,
            chat_id=chat_id,
            text=text,
            reply_markup=back_to_menu_kb(),
            photo_path=pay_ph,
        )
    await state.update_data(
        payment_root_message_id=sent_id,
        payment_root_is_photo=is_photo,
        cleanup_ids=[],
    )


@router.callback_query(F.data.startswith("tdate:"), BookingStates.choosing_tariff_date)
async def pick_tariff_date(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config, pricing: EffectivePricing
) -> None:
    picked_day = callback.data.split(":", maxsplit=1)[1]
    data = await state.get_data()
    start = str(data.get("tariff_start") or "")
    hours = int(data.get("tariff_hours") or 0)
    product = data.get("product")
    kind = data.get("tariff_kind")
    if product not in ("no_engineer", "with_engineer") or hours < 1 or not start:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    slots = await db.get_all_slots_for_day(picked_day)
    ids = Database.slot_ids_for_consecutive_hours_from(slots, start, hours)
    if not ids:
        await callback.answer(
            "Этот день уже недоступен для такого блока. Выберите другую дату.",
            show_alert=True,
        )
        return
    st_label, end_label = Database.tariff_time_range_label(start, hours)
    slot_text = f"{st_label} — {end_label}"
    night = kind == "night"
    with_eng = product == "with_engineer"
    total = pricing.tariff_rub(night=night, hours=hours, with_engineer=with_eng)
    if total <= 0:
        total = pricing.service_price(product) * hours
    if kind == "night":
        tariff_label = f"Тариф: ночная запись, {hours} ч"
    else:
        tariff_label = f"Тариф: дневная запись (с {data.get('tariff_day_start', '')}), {hours} ч"
    await state.update_data(
        day=picked_day,
        slot_ids=ids,
        slot_text=slot_text,
        total=total,
        booking_mode="tariff",
        tariff_label=tariff_label,
    )
    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _send_studio_pay_contact_screen(
        callback.bot,
        chat_id=chat_id,
        state=state,
        config=config,
        db=db,
        pricing=pricing,
        day=picked_day,
        slot_text=slot_text,
        hours=hours,
        total=total,
        product=str(product),
        tariff_label=tariff_label,
    )


@router.callback_query(F.data.startswith("date:"), BookingStates.choosing_date)
async def pick_date(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    picked_day = callback.data.split(":", maxsplit=1)[1]
    data = await state.get_data()
    use_yc = yc.yclients_studio_enabled(config) and (data.get("product") in ("no_engineer", "with_engineer"))
    yc_pack = use_yc and yc.hour_pack_configured(config)
    if use_yc:
        try:
            seances, slots = await yc.load_day_seances_and_slots(config, picked_day)
        except YclientsError as e:
            logger.warning("Yclients pick_date: %s", e)
            await callback.answer(
                "Не удалось загрузить слоты Yclients. Попробуйте другую дату.",
                show_alert=True,
            )
            return
        yc_flag = True
    else:
        slots = await db.get_all_slots_for_day(picked_day)
        seances = []
        yc_flag = False
    if not slots:
        await callback.answer("На эту дату нет расписания слотов.", show_alert=True)
        return
    if not any(Database.slot_row_is_active(s["is_active"]) for s in slots):
        await callback.answer("На эту дату нет свободных слотов", show_alert=True)
        return
    if yc_pack:
        await state.set_state(BookingStates.choosing_yc_start)
        await state.update_data(
            day=picked_day,
            selected_slot_ids=[],
            yclients_studio=True,
            yclients_seances=seances,
            yclients_hour_pack=None,
            yclients_start_idx=None,
            slot_ids=[],
        )
        await _edit(
            callback.message,
            _yc_start_caption(picked_day),
            reply_markup=yclients_start_kb(slots),
        )
        await callback.answer()
        return
    await state.set_state(BookingStates.choosing_slot)
    await state.update_data(
        day=picked_day,
        selected_slot_ids=[],
        yclients_studio=yc_flag,
        yclients_seances=seances if yc_flag else [],
        yclients_hour_pack=None,
        yclients_start_idx=None,
    )
    await _edit(
        callback.message,
        _slots_caption(picked_day, 0),
        reply_markup=slots_pick_kb(slots, set()),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("yc_st:"), BookingStates.choosing_yc_start)
async def yc_pick_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    try:
        idx = int(callback.data.split(":", maxsplit=1)[1])
    except (IndexError, ValueError):
        await callback.answer()
        return
    data = await state.get_data()
    day = data.get("day")
    seances = data.get("yclients_seances") or []
    if not day or not isinstance(seances, list) or idx < 0 or idx >= len(seances):
        await callback.answer("Сессия устарела. Откройте календарь снова.", show_alert=True)
        return
    slots_ui = yc.seances_to_ui_slots(seances, config)
    if idx >= len(slots_ui):
        await callback.answer("Сессия устарела. Откройте календарь снова.", show_alert=True)
        return
    row = slots_ui[idx]
    start_lbl = f"{row['start_time']} — {row['end_time']}"
    await state.set_state(BookingStates.choosing_yc_hours)
    await state.update_data(yclients_start_idx=idx)
    await _edit(
        callback.message,
        _yc_hours_caption(str(day), html_escape(start_lbl)),
        reply_markup=yclients_hours_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "yc_back:start", BookingStates.choosing_yc_hours)
async def yc_back_to_start(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    data = await state.get_data()
    day = data.get("day")
    seances = data.get("yclients_seances") or []
    if not day or not isinstance(seances, list) or not seances:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    slots_ui = yc.seances_to_ui_slots(seances, config)
    await state.set_state(BookingStates.choosing_yc_start)
    await _edit(
        callback.message,
        _yc_start_caption(str(day)),
        reply_markup=yclients_start_kb(slots_ui),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("yc_h:"), BookingStates.choosing_yc_hours)
async def yc_pick_hours(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    pricing: EffectivePricing,
) -> None:
    try:
        hours = int(callback.data.split(":", maxsplit=1)[1])
    except (IndexError, ValueError):
        await callback.answer()
        return
    if hours not in (1, 2, 3, 4):
        await callback.answer()
        return
    data = await state.get_data()
    day = str(data.get("day") or "")
    seances = data.get("yclients_seances") or []
    idx_raw = data.get("yclients_start_idx")
    try:
        idx = int(idx_raw) if idx_raw is not None else -1
    except (TypeError, ValueError):
        idx = -1
    product = data.get("product")
    if product not in ("no_engineer", "with_engineer") or not day or not isinstance(seances, list):
        await callback.answer("Сессия устарела", show_alert=True)
        return
    if idx < 0 or idx >= len(seances):
        await callback.answer("Сессия устарела", show_alert=True)
        return
    try:
        matched = await yc.match_duration_seance(
            config,
            day_yyyy_mm_dd=day,
            start_ref=seances[idx],
            hours=hours,
        )
    except YclientsError as e:
        logger.warning("Yclients yc_pick_hours: %s", e)
        await callback.answer("Не удалось проверить слот в Yclients. Попробуйте снова.", show_alert=True)
        return
    if not matched:
        await callback.answer(
            "На это время нет свободной записи выбранной длительности. Выберите другое время или часы.",
            show_alert=True,
        )
        return
    try:
        prices = await yc.service_prices_map_rub(config)
    except YclientsError as e:
        logger.warning("Yclients prices: %s", e)
        await callback.answer("Не удалось загрузить цены из Yclients.", show_alert=True)
        return
    sid = yc.service_id_for_hours_pack(config, hours)
    total = int(prices.get(sid, 0))
    if total <= 0:
        await callback.answer(
            "Для этой услуги в Yclients не найдена цена (price_min / price_max в онлайн-записи).",
            show_alert=True,
        )
        return
    ui_one = yc.seances_to_ui_slots([matched], config)
    slot_text = f"{ui_one[0]['start_time']} — {ui_one[0]['end_time']}"
    await state.update_data(
        yclients_hour_pack={"start_idx": idx, "hours": hours, "service_id": sid},
        slot_ids=[],
        slot_text=slot_text,
        total=total,
        tariff_label=None,
        booking_mode="hourly",
        slot_hours_caption=str(hours),
        selected_slot_ids=[],
    )
    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _send_studio_pay_contact_screen(
        callback.bot,
        chat_id=chat_id,
        state=state,
        config=config,
        db=db,
        pricing=pricing,
        day=day,
        slot_text=slot_text,
        hours=hours,
        total=total,
        product=str(product),
        tariff_label=None,
        hours_display=str(hours),
    )


@router.callback_query(F.data.startswith("slot_pick:"), BookingStates.choosing_slot)
async def slot_toggle(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    sid = int(callback.data.split(":", maxsplit=1)[1])
    data = await state.get_data()
    day = data.get("day")
    if not day:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    slots = await _studio_day_slots(data, config, db, day)
    by_id = {int(s["id"]): s for s in slots}
    if sid not in by_id or not Database.slot_row_is_active(by_id[sid]["is_active"]):
        await callback.answer("Этот час занят", show_alert=True)
        return
    raw = data.get("selected_slot_ids") or []
    selected = {int(x) for x in raw}
    if sid in selected:
        selected.discard(sid)
    else:
        selected.add(sid)
    # Явные int и стабильный порядок — надёжнее для FSM/JSON-хранилища
    stable_ids = sorted(int(x) for x in selected)
    await state.update_data(selected_slot_ids=stable_ids)
    await _edit(
        callback.message,
        _slots_caption(day, len(selected)),
        reply_markup=slots_pick_kb(slots, selected),
    )
    await callback.answer()


@router.callback_query(F.data == "slot_confirm", BookingStates.choosing_slot)
async def slot_confirm(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config, pricing: EffectivePricing
) -> None:
    data = await state.get_data()
    product = data.get("product")
    if product not in ("no_engineer", "with_engineer"):
        await callback.answer("Неверный сценарий записи", show_alert=True)
        return
    raw = data.get("selected_slot_ids") or []
    if not raw:
        await callback.answer("Выберите хотя бы один свободный час", show_alert=True)
        return
    day = data.get("day")
    if not day:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    slots = await _studio_day_slots(data, config, db, day)
    try:
        id_set = {int(x) for x in raw}
    except (TypeError, ValueError):
        await callback.answer("Данные устарели, откройте календарь снова", show_alert=True)
        return
    # Порядок как в расписании дня (уже по времени), а не лексикографический ORDER BY SQLite.
    chosen = [s for s in slots if int(s["id"]) in id_set]
    if len(chosen) != len(id_set):
        await callback.answer("Часть слотов не найдена. Откройте календарь снова.", show_alert=True)
        return
    if any(not Database.slot_row_is_active(s["is_active"]) for s in chosen):
        await callback.answer("Часть выбранных часов уже занята. Обновите экран.", show_alert=True)
        fresh = await _studio_day_slots(data, config, db, day)
        await state.update_data(selected_slot_ids=[])
        await _edit(
            callback.message,
            _slots_caption(day, 0),
            reply_markup=slots_pick_kb(fresh, set()),
        )
        return
    if not Database.selection_is_valid_multihour_slot_chain(slots, id_set, chosen):
        await callback.answer(
            "Выберите только соседние часы подряд (без пропуска часа между ними).",
            show_alert=True,
        )
        return

    chosen_sorted = sorted(
        chosen,
        key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"])),
    )
    ids_ordered = [int(s["id"]) for s in chosen_sorted]
    hourly_price = pricing.service_price(product)
    h_disp: str | None = None
    if data.get("yclients_studio") and all(int(x) < 0 for x in ids_ordered):
        seances = data.get("yclients_seances") or []
        if not isinstance(seances, list) or not seances:
            await callback.answer("Сессия устарела, откройте календарь снова", show_alert=True)
            return
        total, _sec, slot_text, hlab, hfrac = yc.compute_billing(
            seances, ids_ordered, hourly_price, config
        )
        if total <= 0 or not slot_text:
            await callback.answer("Некорректные слоты, выберите снова.", show_alert=True)
            return
        hours = max(1, int(math.ceil(hfrac)))
        h_disp = hlab or None
    else:
        hours = len(chosen_sorted)
        total = hours * hourly_price
        slot_text = f"{chosen_sorted[0]['start_time']} — {chosen_sorted[-1]['end_time']}"

    await state.update_data(
        slot_ids=ids_ordered,
        slot_text=slot_text,
        total=total,
        tariff_label=None,
        booking_mode="hourly",
        slot_hours_caption=(h_disp or "") if h_disp else "",
    )

    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass

    await _send_studio_pay_contact_screen(
        callback.bot,
        chat_id=chat_id,
        state=state,
        config=config,
        db=db,
        pricing=pricing,
        day=day,
        slot_text=slot_text,
        hours=hours,
        total=total,
        product=str(product),
        tariff_label=None,
        hours_display=h_disp,
    )

@router.callback_query(F.data.startswith("paymeth:"), BookingStates.choosing_pay_method)
async def pay_method_chosen(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    pricing: EffectivePricing,
) -> None:
    choice = callback.data.split(":", 1)[1]
    if choice not in ("online", "standard"):
        await callback.answer()
        return
    pay_online = choice == "online"
    await state.update_data(pay_online=pay_online)
    data = await state.get_data()
    product = data.get("product")
    root_mid = data.get("payment_root_message_id")
    is_photo = bool(data.get("payment_root_is_photo", False))
    cid = callback.message.chat.id
    if not root_mid:
        await callback.answer("Сессия устарела. Откройте меню и начните снова.", show_alert=True)
        return

    s = await db.get_all_settings()
    pay_path = ui_photo_payment(s, config)

    if product in ("lyrics", "beat"):
        await state.set_state(BookingStates.entering_brief)
        price = pricing.service_price(product)
        if pay_online:
            screen = (
                "<b>📝 Опишите заказ</b>\n\n"
                "Отправьте сообщение в таком порядке:\n"
                "• сначала — <b>пожелания</b> к заказу (можно несколько строк);\n"
                "• затем <b>2 строки</b>:\n"
                "  — имя\n"
                "  — ваш @username в Telegram (или «—» если без username)\n\n"
                f"<b>Стоимость:</b> {price} руб"
            )
        else:
            screen = (
                "<b>📝 Опишите заказ</b>\n\n"
                "Отправьте <b>несколько строк</b> в таком порядке:\n"
                "• сначала — <b>пожелания</b> к заказу (можно несколько строк);\n"
                "• затем <b>4 отдельные строки</b>:\n"
                "  — имя\n"
                "  — фамилия\n"
                "  — банк (с какого будет оплата)\n"
                "  — ваш @username в Telegram (или «—» если без username)\n\n"
                f"<b>Стоимость:</b> {price} руб"
            )
        try:
            await _edit_payment_screen_message(
                callback.bot,
                chat_id=cid,
                message_id=int(root_mid),
                text=screen,
                reply_markup=back_to_menu_kb(),
                is_photo=is_photo,
            )
        except Exception:
            new_id, new_ph = await _send_payment_screen_message(
                callback.bot,
                chat_id=cid,
                text=screen,
                reply_markup=back_to_menu_kb(),
                photo_path=pay_path,
            )
            await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
        await callback.answer()
        return

    if product not in ("no_engineer", "with_engineer"):
        await callback.answer("Сессия устарела", show_alert=True)
        return

    day = data.get("day")
    slot_text = data.get("slot_text")
    total = data.get("total")
    slot_ids = data.get("slot_ids") or []
    hp = data.get("yclients_hour_pack")
    if isinstance(hp, dict) and int(hp.get("hours") or 0) > 0:
        hours = int(hp["hours"])
    else:
        hours = len(slot_ids)
    yc_st = bool(data.get("yclients_studio"))
    shc = (data.get("slot_hours_caption") or "").strip()
    h_line = shc or str(hours)
    tariff_label = data.get("tariff_label")
    svc_line = html_escape(str(tariff_label or pricing.service_title(product)))
    base_core = "\n".join(
        [
            f"<b>Услуга:</b> {svc_line}",
            f"<b>Дата:</b> {day}",
            f"<b>Время:</b> {slot_text}",
            f"<b>Длит./часов (тариф):</b> {h_line}",
            f"<b>Итого:</b> {total} руб",
        ]
    )
    await state.set_state(BookingStates.entering_contacts)
    if pay_online:
        if yc_st:
            screen = (
                "<b>💳 Онлайн-оплата (ЮKassa)</b>\n\n"
                f"{base_core}\n\n"
                "Для CRM Yclients укажите <b>телефон</b>.\n\n"
                "Отправьте <b>одним сообщением (3 строки)</b>:\n"
                "• имя\n"
                "• телефон (например +79001234567)\n"
                "• ваш @username в Telegram (или «—»)\n\n"
                "<i>Пример:\n"
                "Иван\n"
                "+79001234567\n"
                "@nickname</i>"
            )
        else:
            screen = (
                "<b>💳 Онлайн-оплата (ЮKassa)</b>\n\n"
                f"{base_core}\n\n"
                "Отправьте <b>одним сообщением (2 строки)</b>:\n"
                "• имя\n"
                "• ваш @username в Telegram (или «—»)\n\n"
                "<i>Пример:\n"
                "Иван\n"
                "@nickname</i>"
            )
    else:
        dest = payment_destination_block_html(config, bank_transfer=True, settings=s)
        if yc_st:
            screen = (
                "<b>💳 Реквизиты для оплаты</b>\n\n"
                f"{base_core}\n\n"
                f"{dest}\n\n"
                "<b>Далее отправьте одним сообщением (5 строк):</b>\n"
                "• имя\n"
                "• фамилия\n"
                "• телефон (для записи в Yclients)\n"
                "• банк (с какого будет оплата)\n"
                "• ваш @username в Telegram (или «—»)\n\n"
                "<i>Пример:\n"
                "Иван\n"
                "Иванов\n"
                "+79001234567\n"
                "Сбербанк\n"
                "@nickname</i>"
            )
        else:
            screen = (
                "<b>💳 Реквизиты для оплаты</b>\n\n"
                f"{base_core}\n\n"
                f"{dest}\n\n"
                "<b>Далее отправьте одним сообщением (4 строки):</b>\n"
                "• имя\n"
                "• фамилия\n"
                "• банк (с какого будет оплата)\n"
                "• ваш @username в Telegram (или «—»)\n\n"
                "<i>Пример:\n"
                "Иван\n"
                "Иванов\n"
                "Сбербанк\n"
                "@nickname</i>"
            )
    try:
        await _edit_payment_screen_message(
            callback.bot,
            chat_id=cid,
            message_id=int(root_mid),
            text=screen,
            reply_markup=back_to_menu_kb(),
            is_photo=is_photo,
        )
    except Exception:
        new_id, new_ph = await _send_payment_screen_message(
            callback.bot,
            chat_id=cid,
            text=screen,
            reply_markup=back_to_menu_kb(),
            photo_path=pay_path,
        )
        await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
    await callback.answer()


@router.message(BookingStates.entering_brief)
async def enter_brief(
    message: Message, state: FSMContext, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    raw = (message.text or "").strip()
    data = await state.get_data()
    product = data.get("product")
    if product not in ("lyrics", "beat"):
        await state.clear()
        return

    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    pay_online = bool(data.get("pay_online"))
    min_lines = 3 if pay_online else 5
    if len(lines) < min_lines:
        if pay_online:
            warn = await message.answer(
                "Нужно минимум 3 строки: пожелания (можно несколько строк), затем имя и @username (или «—»).\n\n"
                "Пример:\n"
                "Хочу поп, женский вокал, минор...\n"
                "Иван\n"
                "@nickname"
            )
        else:
            warn = await message.answer(
                "Нужно минимум 5 строк: пожелания (можно несколько строк), затем имя, фамилию, банк и @username "
                "(или «—»).\n\n"
                "Пример:\n"
                "Хочу поп, женский вокал, минор...\n"
                "Иван\n"
                "Иванов\n"
                "Сбербанк\n"
                "@nickname"
            )
        c = list(data.get("cleanup_ids", []))
        c.extend([message.message_id, warn.message_id])
        await state.update_data(cleanup_ids=c)
        return

    if pay_online:
        first_name = lines[-2]
        tg_line = lines[-1]
        brief = "\n".join(lines[:-2]).strip()
        user_display_name = first_name.strip()
        bank = "—"
        tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
        if len(brief) < 10 or len(first_name) < 1:
            warn = await message.answer(
                "Проверьте: пожелания (от 10 символов), имя и username."
            )
            c = list(data.get("cleanup_ids", []))
            c.extend([message.message_id, warn.message_id])
            await state.update_data(cleanup_ids=c)
            return
    else:
        first_name = lines[-4]
        last_name = lines[-3]
        bank = lines[-2]
        tg_line = lines[-1]
        brief = "\n".join(lines[:-4]).strip()
        user_display_name = f"{first_name} {last_name}".strip()
        tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
        if len(brief) < 10 or len(first_name) < 1 or len(last_name) < 1 or len(bank) < 2:
            warn = await message.answer(
                "Проверьте: пожелания (от 10 символов), имя, фамилию, банк (от 2 символов), username."
            )
            c = list(data.get("cleanup_ids", []))
            c.extend([message.message_id, warn.message_id])
            await state.update_data(cleanup_ids=c)
            return

    price = pricing.service_price(product)
    await state.update_data(
        brief=brief,
        total=price,
        name=user_display_name,
        phone=bank,
        tg_username=tg_u,
    )
    await state.set_state(BookingStates.waiting_payment)
    root_mid = data.get("payment_root_message_id")
    is_photo = data.get("payment_root_is_photo", False)
    if root_mid:
        use_yk_btn = bool(is_yookassa_configured(config) and pay_online)
        if use_yk_btn:
            pay_screen = (
                "<b>💳 Оплата</b>\n\n"
                f"<b>Услуга:</b> {pricing.service_title(product)}\n"
                f"<b>Стоимость:</b> {price} руб\n\n"
                "Нажмите кнопку ниже — откроется оплата ЮKassa."
            )
        else:
            s_pay = await db.get_all_settings()
            dest = payment_destination_block_html(config, bank_transfer=True, settings=s_pay)
            pay_screen = (
                "<b>💳 Реквизиты для оплаты</b>\n\n"
                f"<b>Услуга:</b> {pricing.service_title(product)}\n"
                f"<b>Стоимость:</b> {price} руб\n\n"
                f"{dest}\n\n"
                "После оплаты нажмите кнопку ниже."
            )
        cid = message.chat.id
        mid = int(root_mid)
        s = await db.get_all_settings()
        pay_path = ui_photo_payment(s, config)
        try:
            await _edit_payment_screen_message(
                message.bot,
                chat_id=cid,
                message_id=mid,
                text=pay_screen,
                reply_markup=paid_kb(online=use_yk_btn),
                is_photo=is_photo,
            )
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                pass
            else:
                new_id, new_ph = await _send_payment_screen_message(
                    message.bot,
                    chat_id=cid,
                    text=pay_screen,
                    reply_markup=paid_kb(online=use_yk_btn),
                    photo_path=pay_path,
                )
                await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
        except Exception:
            new_id, new_ph = await _send_payment_screen_message(
                message.bot,
                chat_id=cid,
                text=pay_screen,
                reply_markup=paid_kb(online=use_yk_btn),
                photo_path=pay_path,
            )
            await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
    # Можно удалить сообщение пользователя после успешной оплаты — сохраним id
    cleanup = list(data.get("cleanup_ids", []))
    cleanup.append(message.message_id)
    await state.update_data(cleanup_ids=cleanup)


@router.message(BookingStates.entering_contacts)
async def enter_contacts(
    message: Message, state: FSMContext, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    raw = (message.text or "").strip()
    data = await state.get_data()
    pay_online = bool(data.get("pay_online"))
    product = data.get("product")
    yc_st = bool(data.get("yclients_studio")) and product in ("no_engineer", "with_engineer")
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    yc_phone: str = ""
    if pay_online:
        if yc_st:
            if len(lines) < 3:
                warn = await message.answer(
                    "Нужно 3 строки:\n"
                    "• имя\n"
                    "• телефон (например +79001234567)\n"
                    "• @username в Telegram (или «—»)\n\n"
                    "Пример:\n"
                    "Иван\n"
                    "+79001234567\n"
                    "@nickname"
                )
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
            first_name, phone_line, tg_line = lines[0], lines[1], lines[2]
            user_name = first_name.strip()
            yc_phone = yc.normalize_ru_phone(phone_line)
            tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
            if len(first_name) < 1 or len(yc_phone) < 10:
                warn = await message.answer(
                    "Укажите непустое имя и корректный телефон (как в примере +79001234567)."
                )
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
            bank = yc_phone
        else:
            if len(lines) < 2:
                warn = await message.answer(
                    "Нужно 2 строки:\n"
                    "• имя\n"
                    "• @username в Telegram (или «—»)\n\n"
                    "Пример:\n"
                    "Иван\n"
                    "@nickname"
                )
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
            first_name, tg_line = lines[0], lines[1]
            user_name = first_name.strip()
            bank = "—"
            tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
            if len(first_name) < 1:
                warn = await message.answer("Укажите имя (непустая первая строка).")
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
    else:
        if yc_st:
            if len(lines) < 5:
                warn = await message.answer(
                    "Нужно 5 строк:\n"
                    "• имя\n"
                    "• фамилия\n"
                    "• телефон\n"
                    "• банк (с какого будет оплата)\n"
                    "• @username в Telegram (или «—»)\n\n"
                    "Пример:\n"
                    "Иван\n"
                    "Иванов\n"
                    "+79001234567\n"
                    "Сбербанк\n"
                    "@nickname"
                )
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
            first_name, last_name, phone_line, bank, tg_line = (
                lines[0],
                lines[1],
                lines[2],
                lines[3],
                lines[4],
            )
            user_name = f"{first_name} {last_name}".strip()
            yc_phone = yc.normalize_ru_phone(phone_line)
            tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
            if len(first_name) < 1 or len(last_name) < 1 or len(bank) < 2 or len(yc_phone) < 10:
                warn = await message.answer(
                    "Проверьте имя, фамилию, телефон, банк (от 2 символов) и username."
                )
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
            bank = f"тел. {yc_phone} | банк: {bank}"
        else:
            if len(lines) < 4:
                warn = await message.answer(
                    "Нужно ровно 4 строки:\n"
                    "• имя\n"
                    "• фамилия\n"
                    "• банк (с какого будет оплата)\n"
                    "• @username в Telegram (или «—»)\n\n"
                    "Пример:\n"
                    "Иван\n"
                    "Иванов\n"
                    "Сбербанк\n"
                    "@nickname"
                )
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
            first_name, last_name, bank, tg_line = lines[0], lines[1], lines[2], lines[3]
            user_name = f"{first_name} {last_name}".strip()
            tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
            if len(first_name) < 1 or len(last_name) < 1 or len(bank) < 2:
                warn = await message.answer("Проверьте имя, фамилию и название банка (минимум 2 символа).")
                c = list(data.get("cleanup_ids", []))
                c.extend([message.message_id, warn.message_id])
                await state.update_data(cleanup_ids=c)
                return
    await state.update_data(
        name=user_name,
        phone=bank,
        tg_username=tg_u,
        yc_client_phone=yc_phone,
    )
    await state.set_state(BookingStates.waiting_payment)

    data = await state.get_data()
    product = data.get("product")
    if product not in ("no_engineer", "with_engineer"):
        return
    day = data.get("day")
    slot_text = data.get("slot_text")
    total = data.get("total")
    slot_ids = data.get("slot_ids") or []
    shc2 = (data.get("slot_hours_caption") or "").strip()
    hp2 = data.get("yclients_hour_pack")
    if isinstance(hp2, dict) and int(hp2.get("hours") or 0) > 0:
        hours = int(hp2["hours"])
    else:
        hours = len(slot_ids)
    h_line2 = shc2 or str(hours)
    svc_line = html_escape(str(data.get("tariff_label") or pricing.service_title(product)))
    tg_disp = f"@{html_escape(tg_u)}" if tg_u else "—"
    use_yk_btn = bool(is_yookassa_configured(config) and pay_online)
    if use_yk_btn:
        screen2 = (
            "<b>💳 Оплата</b>\n\n"
            f"<b>Услуга:</b> {svc_line}\n"
            f"<b>Дата:</b> {day}\n"
            f"<b>Время:</b> {slot_text}\n"
            f"<b>Длит./часов (тариф):</b> {h_line2}\n"
            f"<b>Итого:</b> {total} руб\n\n"
            f"<b>Имя:</b> {html_escape(user_name)}\n"
            f"<b>Telegram:</b> {tg_disp}\n\n"
            "Нажмите кнопку ниже — откроется оплата ЮKassa."
        )
    else:
        s_dest = await db.get_all_settings()
        dest = payment_destination_block_html(config, bank_transfer=True, settings=s_dest)
        screen2 = (
            "<b>💳 Реквизиты для оплаты</b>\n\n"
            f"<b>Услуга:</b> {svc_line}\n"
            f"<b>Дата:</b> {day}\n"
            f"<b>Время:</b> {slot_text}\n"
            f"<b>Длит./часов (тариф):</b> {h_line2}\n"
            f"<b>Итого:</b> {total} руб\n\n"
            f"{dest}\n\n"
            f"<b>Данные:</b> {html_escape(user_name)}\n"
            f"<b>Банк:</b> {html_escape(bank)}\n"
            f"<b>Telegram:</b> {tg_disp}\n\n"
            "После оплаты нажмите кнопку ниже."
        )
    root_mid = data.get("payment_root_message_id")
    is_photo = data.get("payment_root_is_photo", False)
    cid = message.chat.id
    if root_mid:
        s = await db.get_all_settings()
        pay_path = ui_photo_payment(s, config)
        try:
            await _edit_payment_screen_message(
                message.bot,
                chat_id=cid,
                message_id=int(root_mid),
                text=screen2,
                reply_markup=paid_kb(online=use_yk_btn),
                is_photo=is_photo,
            )
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                pass
            else:
                new_id, new_ph = await _send_payment_screen_message(
                    message.bot,
                    chat_id=cid,
                    text=screen2,
                    reply_markup=paid_kb(online=use_yk_btn),
                    photo_path=pay_path,
                )
                await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
        except Exception:
            new_id, new_ph = await _send_payment_screen_message(
                message.bot,
                chat_id=cid,
                text=screen2,
                reply_markup=paid_kb(online=use_yk_btn),
                photo_path=pay_path,
            )
            await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)

    ids = list(data.get("cleanup_ids", []))
    ids.append(message.message_id)
    await state.update_data(cleanup_ids=ids)


@router.callback_query(F.data == "book:paid", BookingStates.waiting_payment)
async def paid(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    pricing: EffectivePricing,
) -> None:
    data = await state.get_data()
    if data.get("paid_processing"):
        await callback.answer("⏳ Уже обрабатываю оплату...", show_alert=True)
        return
    await state.update_data(paid_processing=True)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    product = data.get("product")
    chat_id = callback.message.chat.id
    root_mid = data.get("payment_root_message_id")
    is_photo = bool(data.get("payment_root_is_photo", False))
    use_yk = bool(is_yookassa_configured(config) and data.get("pay_online"))

    async def _edit_waiting() -> None:
        waiting = await append_manager_contact_html(
            db,
            "<b>⏳ Ожидайте подтверждения</b>\n\n"
            "Мы проверяем оплату. Как только оператор подтвердит заявку, "
            "вы получите уведомление в этот чат.",
            config,
        )
        if root_mid:
            try:
                await _edit_payment_screen_message(
                    callback.bot,
                    chat_id=chat_id,
                    message_id=int(root_mid),
                    text=waiting,
                    reply_markup=None,
                    is_photo=is_photo,
                )
            except Exception:
                await callback.bot.send_message(
                    chat_id, waiting, parse_mode=ParseMode.HTML
                )

    async def _edit_waiting_yk() -> None:
        waiting = await append_manager_contact_html(
            db,
            "<b>⏳ Ожидайте</b>\n\n"
            "Перейдите по ссылке ниже и завершите оплату на странице ЮKassa. "
            "После успешной оплаты заявка подтвердится автоматически.",
            config,
        )
        if root_mid:
            try:
                await _edit_payment_screen_message(
                    callback.bot,
                    chat_id=chat_id,
                    message_id=int(root_mid),
                    text=waiting,
                    reply_markup=None,
                    is_photo=is_photo,
                )
            except Exception:
                await callback.bot.send_message(
                    chat_id, waiting, parse_mode=ParseMode.HTML
                )

    s = await db.get_all_settings()
    inbox = effective_payments_inbox_chat_id(s, config)

    if product in ("lyrics", "beat"):
        svc_title = pricing.service_title(product)
        order_id = await db.create_service_order(
            user_id=callback.from_user.id,
            user_name=str(data.get("name", "")),
            phone=str(data.get("phone", "")),
            tg_username=(data.get("tg_username") or callback.from_user.username or ""),
            product=product,
            services_label=svc_title,
            total_price=int(data.get("total", 0)),
            notes=str(data.get("brief", "")),
            status="awaiting_yookassa" if use_yk else "pending_payment",
        )
        booking = await db.get_booking_by_id(order_id)
        await save_booking_pending_ui_cleanup(db, order_id, chat_id=chat_id, data=data)
        client_tg = _format_tg_username(data.get("tg_username") or callback.from_user.username or "")
        maker = await effective_maker_username(db, config, kind=product)
        admin_text = (
            "<b>💳 Подтверждение оплаты</b> <i>(текст/бит)</i>\n\n"
            f"<b>ID заявки:</b> <code>#{order_id}</code>\n"
            f"<b>Время заявки:</b> {html_escape(str(booking.get('created_at', '—')))}\n"
            f"<b>Услуга:</b> {html_escape(svc_title)}\n"
            f"<b>Клиент:</b> {html_escape(str(data.get('name', '')))}\n"
            f"<b>Банк:</b> {html_escape(str(data.get('phone', '')))}\n"
            f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
            f"<b>Сумма:</b> {data.get('total', 0)} руб\n"
            f"<b>Исполнитель:</b> {html_escape(_format_maker_username(maker))}\n\n"
            f"<b>Пожелания:</b>\n{html_escape(str(data.get('brief', '')))}"
        )
        if use_yk:
            admin_text += (
                "\n\n<i>Клиент оплачивает через ЮKassa — после оплаты заявка подтвердится автоматически.</i>"
            )
            try:
                pay_url = await create_payment(
                    int(data.get("total", 0)),
                    f"{svc_title} #{order_id}",
                    callback.from_user.id,
                    {"booking_id": order_id},
                    config=config,
                    db=db,
                )
            except Exception:
                logger.exception("YooKassa create_payment failed order_id=%s", order_id)
                await db.cancel_booking(order_id)
                await state.update_data(paid_processing=False)
                await callback.answer(
                    "Не удалось создать платёж. Попробуйте позже или напишите администратору.",
                    show_alert=True,
                )
                return
            try:
                await callback.bot.send_message(
                    inbox,
                    admin_text,
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                await callback.bot.send_message(
                    config.admin_id,
                    admin_text + "\n\n<i>(не удалось отправить в PAYMENTS_CHAT_ID)</i>",
                    parse_mode=ParseMode.HTML,
                )
            # Не отправляем новое сообщение: обновляем существующий экран оплаты.
            waiting = await append_manager_contact_html(
                db,
                "<b>⏳ Ожидайте</b>\n\n"
                "Нажмите кнопку ниже и завершите оплату на странице ЮKassa. "
                "После успешной оплаты заявка подтвердится автоматически.",
                config,
            )
            if root_mid:
                try:
                    await _edit_payment_screen_message(
                        callback.bot,
                        chat_id=chat_id,
                        message_id=int(root_mid),
                        text=waiting,
                        reply_markup=_yookassa_pay_url_kb(pay_url),
                        is_photo=is_photo,
                    )
                except Exception:
                    await callback.bot.send_message(
                        chat_id,
                        "<b>Оплата ЮKassa</b>\n\nНажмите кнопку ниже — откроется страница оплаты.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=_yookassa_pay_url_kb(pay_url),
                    )
            else:
                await callback.bot.send_message(
                    chat_id,
                    "<b>Оплата ЮKassa</b>\n\nНажмите кнопку ниже — откроется страница оплаты.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_yookassa_pay_url_kb(pay_url),
                )
            # Расписание/канал обновляем только после успешной оплаты (webhook/подтверждение),
            # иначе в канале появятся изменения "до оплаты".
            await state.set_state(BookingStates.awaiting_payment_confirm)
            await state.update_data(paid_processing=False, pending_booking_id=order_id)
            await callback.answer("Откройте ссылку и оплатите")
            return

        try:
            await callback.bot.send_message(
                inbox,
                admin_text,
                parse_mode=ParseMode.HTML,
                reply_markup=_payment_review_kb(order_id),
            )
        except Exception:
            await callback.bot.send_message(
                config.admin_id,
                admin_text + "\n\n<i>(не удалось отправить в PAYMENTS_CHAT_ID)</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=_payment_review_kb(order_id),
            )
        await _edit_waiting()
        await state.set_state(BookingStates.awaiting_payment_confirm)
        await state.update_data(paid_processing=False, pending_booking_id=order_id)
        await callback.answer("Заявка отправлена на проверку")
        return

    services_human = str(data.get("tariff_label") or pricing.service_title(str(product)))

    slot_ids_c = [int(x) for x in data.get("slot_ids") or []]
    yc_st = bool(data.get("yclients_studio")) and slot_ids_c and all(x < 0 for x in slot_ids_c)
    hour_pack_raw = data.get("yclients_hour_pack")
    yc_hour_pack = (
        isinstance(hour_pack_raw, dict)
        and bool(data.get("yclients_studio"))
        and int(hour_pack_raw.get("hours") or 0) in (1, 2, 3, 4)
    )

    if yc_hour_pack:
        hp = hour_pack_raw  # type: ignore[assignment]
        day_yc = str(data.get("day") or "")
        try:
            start_idx = int(hp.get("start_idx", -1))
            pack_hours = int(hp.get("hours", 0))
            svc_pack = int(hp.get("service_id", 0))
        except (TypeError, ValueError):
            await callback.answer("Сессия устарела. Начните запись снова.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        if not day_yc or start_idx < 0 or svc_pack <= 0:
            await callback.answer("Сессия устарела. Начните запись снова.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        if int(svc_pack) != int(yc.service_id_for_hours_pack(config, pack_hours)):
            await callback.answer("Данные записи устарели. Выберите время снова.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        phone_api = (data.get("yc_client_phone") or "").strip() or yc.normalize_ru_phone(
            str(data.get("phone") or "")
        )
        if len(phone_api) < 10:
            await callback.answer("Нужен корректный телефон для Yclients. Введите контакты снова.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        verified = await yc.verify_hour_pack_ready(
            config,
            day_yyyy_mm_dd=day_yc,
            start_idx=start_idx,
            hours=pack_hours,
        )
        if not verified:
            await callback.answer(
                "Слот больше недоступен на выбранную длительность. Откройте календарь и выберите время снова.",
                show_alert=True,
            )
            await state.update_data(paid_processing=False)
            return
        _, nh_seance = verified
        api_id = f"tg{callback.from_user.id}-{day_yc}-pack{pack_hours}h-{start_idx}"
        try:
            yc_rec_id, _yc_payload = await yc.create_yclients_studio_record_pack(
                config,
                day=day_yc,
                record_seance=nh_seance,
                service_id=svc_pack,
                client_name=str(data.get("name") or "Клиент"),
                client_phone_digits=phone_api,
                api_id=api_id,
            )
        except YclientsError as e:
            logger.warning("Yclients create pack: %s", e)
            await callback.answer(
                f"Yclients: не удалось оформить запись. {str(e)[:180]}",
                show_alert=True,
            )
            await state.update_data(paid_processing=False)
            return
        ui_sl = yc.seances_to_ui_slots([nh_seance], config)
        st0 = str(ui_sl[0]["start_time"])
        en0 = str(ui_sl[0]["end_time"])
        booking_id = await db.create_booking_studio_yclients(
            user_id=callback.from_user.id,
            user_name=str(data.get("name") or ""),
            phone=str(data.get("phone") or ""),
            tg_username=(data.get("tg_username") or callback.from_user.username or ""),
            requires_engineer=(product == "with_engineer"),
            day=day_yc,
            start_time=st0,
            end_time=en0,
            booked_slot_ids_csv=f"yc_pack:{pack_hours}h:{svc_pack}:{start_idx}",
            services=services_human,
            total_price=int(data["total"]),
            status="awaiting_yookassa" if use_yk else "pending_payment",
            yclients_record_id=yc_rec_id,
        )
    elif yc_st:
        day_yc = str(data.get("day") or "")
        seances_yc = data.get("yclients_seances") or []
        if not day_yc or not isinstance(seances_yc, list) or not seances_yc:
            await callback.answer("Сессия устарела. Начните запись в календаре снова.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        phone_api = (data.get("yc_client_phone") or "").strip() or yc.normalize_ru_phone(
            str(data.get("phone") or "")
        )
        if len(phone_api) < 10:
            await callback.answer("Нужен корректный телефон для Yclients. Введите контакты снова.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        try:
            fresh_se, _ = await yc.load_day_seances_and_slots(config, day_yc)
        except YclientsError as e:
            logger.warning("Yclients paid refresh: %s", e)
            await callback.answer("Yclients недоступен. Попробуйте позже.", show_alert=True)
            await state.update_data(paid_processing=False)
            return
        if not yc.selection_still_fresh(slot_ids_c, seances_yc, fresh_se, config):
            await callback.answer(
                "Свободные слоты изменились. Откройте «Записаться» и выберите время снова.",
                show_alert=True,
            )
            await state.update_data(paid_processing=False)
            return
        api_id = f"tg{callback.from_user.id}-{day_yc}-{min(slot_ids_c)}x{len(slot_ids_c)}"
        try:
            yc_rec_id, _yc_payload = await yc.create_yclients_studio_record(
                config,
                seances=seances_yc,
                selected_neg_ids=slot_ids_c,
                day=day_yc,
                client_name=str(data.get("name") or "Клиент"),
                client_phone_digits=phone_api,
                api_id=api_id,
            )
        except YclientsError as e:
            logger.warning("Yclients create: %s", e)
            await callback.answer(
                f"Yclients: не удалось оформить запись. {str(e)[:180]}",
                show_alert=True,
            )
            await state.update_data(paid_processing=False)
            return
        st0, en0 = yc.booking_time_bounds(seances_yc, slot_ids_c, config)
        booking_id = await db.create_booking_studio_yclients(
            user_id=callback.from_user.id,
            user_name=str(data.get("name") or ""),
            phone=str(data.get("phone") or ""),
            tg_username=(data.get("tg_username") or callback.from_user.username or ""),
            requires_engineer=(product == "with_engineer"),
            day=day_yc,
            start_time=st0,
            end_time=en0,
            booked_slot_ids_csv=",".join(str(x) for x in slot_ids_c),
            services=services_human,
            total_price=int(data["total"]),
            status="awaiting_yookassa" if use_yk else "pending_payment",
            yclients_record_id=yc_rec_id,
        )
    else:
        booking_id = await db.create_booking(
            user_id=callback.from_user.id,
            user_name=data["name"],
            phone=data["phone"],
            tg_username=(data.get("tg_username") or callback.from_user.username or ""),
            requires_engineer=(product == "with_engineer"),
            slot_ids=slot_ids_c,
            services=services_human,
            total_price=int(data["total"]),
            status="awaiting_yookassa" if use_yk else "pending_payment",
        )
    if not booking_id:
        await callback.answer("Не удалось создать запись. Возможно слот уже занят.", show_alert=True)
        await state.update_data(paid_processing=False)
        await state.clear()
        return

    booking = await db.get_booking_by_id(booking_id)
    await save_booking_pending_ui_cleanup(db, booking_id, chat_id=chat_id, data=data)
    client_tg = _format_tg_username(booking.get("tg_username"))
    admin_text = (
        "<b>💳 Подтверждение оплаты</b> <i>(студия)</i>\n\n"
        f"<b>ID заявки:</b> <code>#{booking_id}</code>\n"
        f"<b>Время заявки:</b> {html_escape(str(booking.get('created_at', '—')))}\n"
        f"<b>Клиент:</b> {html_escape(str(booking['user_name']))}\n"
        f"<b>Банк:</b> {html_escape(str(booking['phone']))}\n"
        f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
        f"<b>Дата:</b> {html_escape(str(booking['day']))}\n"
        f"<b>Время:</b> {html_escape(str(booking['start_time']))} — "
        f"{html_escape(str(booking['end_time']))}\n"
        f"<b>Услуги:</b> {html_escape(str(booking['services']))}\n"
        f"<b>Сумма:</b> {booking['total_price']} руб"
    )
    yc_id = booking.get("yclients_record_id")
    if yc_id:
        admin_text += f"\n<b>Yclients (CRM):</b> <code>#{yc_id}</code>"
    if use_yk:
        admin_text += (
            "\n\n<i>Клиент оплачивает через ЮKassa — после оплаты заявка подтвердится автоматически.</i>"
        )
        try:
            pay_url = await create_payment(
                int(data["total"]),
                f"Запись на студию #{booking_id}",
                callback.from_user.id,
                {"booking_id": booking_id},
                config=config,
                db=db,
            )
        except Exception:
            logger.exception("YooKassa create_payment failed booking_id=%s", booking_id)
            await db.cancel_booking(booking_id)
            await state.update_data(paid_processing=False)
            await callback.answer(
                "Не удалось создать платёж. Попробуйте позже или напишите администратору.",
                show_alert=True,
            )
            return
        try:
            await callback.bot.send_message(
                inbox,
                admin_text,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            await callback.bot.send_message(
                config.admin_id,
                admin_text + "\n\n<i>(не удалось отправить в PAYMENTS_CHAT_ID)</i>",
                parse_mode=ParseMode.HTML,
            )
        waiting = await append_manager_contact_html(
            db,
            "<b>⏳ Ожидайте</b>\n\n"
            "Нажмите кнопку ниже и завершите оплату на странице ЮKassa. "
            "После успешной оплаты заявка подтвердится автоматически.",
            config,
        )
        if root_mid:
            try:
                await _edit_payment_screen_message(
                    callback.bot,
                    chat_id=chat_id,
                    message_id=int(root_mid),
                    text=waiting,
                    reply_markup=_yookassa_pay_url_kb(pay_url),
                    is_photo=is_photo,
                )
            except Exception:
                await callback.bot.send_message(
                    chat_id,
                    "<b>Оплата ЮKassa</b>\n\nНажмите кнопку ниже — откроется страница оплаты.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_yookassa_pay_url_kb(pay_url),
                )
        else:
            await callback.bot.send_message(
                chat_id,
                "<b>Оплата ЮKassa</b>\n\nНажмите кнопку ниже — откроется страница оплаты.",
                parse_mode=ParseMode.HTML,
                reply_markup=_yookassa_pay_url_kb(pay_url),
            )
        # Расписание/канал обновляем только после успешной оплаты (webhook/подтверждение)
        await state.set_state(BookingStates.awaiting_payment_confirm)
        await state.update_data(paid_processing=False, pending_booking_id=booking_id)
        await callback.answer("Откройте ссылку и оплатите")
        return

    try:
        await callback.bot.send_message(
            inbox,
            admin_text,
            parse_mode=ParseMode.HTML,
            reply_markup=_payment_review_kb(booking_id),
        )
    except Exception:
        await callback.bot.send_message(
            config.admin_id,
            admin_text + "\n\n<i>(не удалось отправить в PAYMENTS_CHAT_ID)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=_payment_review_kb(booking_id),
        )
    await _edit_waiting()
    await state.set_state(BookingStates.awaiting_payment_confirm)
    await state.update_data(paid_processing=False, pending_booking_id=booking_id)
    await callback.answer("Заявка отправлена на проверку")


def _booking_type_title(kind: str | None) -> str:
    k = kind or "studio"
    if k == "studio":
        return "🎧 Запись на студию"
    if k == "lyrics":
        return "📝 Текст для песни"
    if k == "beat":
        return "🎚️ Бит для песни"
    return k


async def _format_my_bookings_screen(
    rows: list[dict],
    db: Database,
    config: Config,
    *,
    studio_address_html: str = "",
) -> str:
    def sort_key(b: dict) -> tuple[int, int]:
        k = b.get("booking_kind") or "studio"
        order = {"studio": 0, "lyrics": 1, "beat": 2}.get(k, 9)
        return order, -int(b["id"])

    ordered = sorted(rows, key=sort_key)
    lines: list[str] = [
        "<b>📅 Мои активные заявки</b>",
        "",
        "В этом сообщении перечислены все текущие заявки: для каждой указаны "
        "<b>ID</b>, услуга в кавычках, стоимость, даты/время (для студии) и контакты.",
        "",
    ]
    for b in ordered:
        kind = b.get("booking_kind") or "studio"
        title = _booking_type_title(kind)
        svc = html_escape(str(b.get("services", "—")))
        lines.append(f"<b>━━━ {title} ━━━</b>")
        lines.append(
            f"<b>ID:</b> <code>#{b['id']}</code>  |  "
            f"<b>Услуга:</b> «{svc}»  |  "
            f"<b>Стоимость:</b> {b['total_price']} руб"
        )
        if b.get("status") == "pending_payment":
            lines.append("<i>⏳ Ожидает подтверждения оплаты оператором</i>")
        if b.get("status") == "pending_cancel":
            lines.append("<i>⏳ Ожидает подтверждения отмены оператором</i>")
        if b.get("status") == "pending_reschedule":
            lines.append("<i>⏳ Ожидает подтверждения переноса оператором</i>")
        if kind in ("lyrics", "beat"):
            maker = await effective_maker_username(db, config, kind=kind)
            lines.append(f"<b>Исполнитель:</b> {html_escape(_format_maker_username(maker))}")
            lines.append(
                f"<b>Имя:</b> {html_escape(str(b.get('user_name', '—')))}  |  "
                f"<b>Банк:</b> {html_escape(str(b.get('phone', '—')))}"
            )
            tu = _format_tg_username(b.get("tg_username"))
            if tu:
                lines.append(f"<b>Telegram:</b> {html_escape(tu)}")
            notes = (b.get("notes") or "").strip()
            if notes:
                preview = notes.replace("\n", " ")
                if len(preview) > 400:
                    preview = preview[:397] + "..."
                lines.append(f"<b>Текст заказа:</b> {html_escape(preview)}")
        else:
            lines.append(
                f"<b>Дата:</b> {html_escape(str(b.get('day', '—')))}  |  "
                f"<b>Время:</b> {html_escape(str(b['start_time']))} — {html_escape(str(b['end_time']))}"
            )
            if studio_address_html:
                lines.append(f"<b>Адрес студии:</b> {studio_address_html}")
            lines.append(
                f"<b>Имя:</b> {html_escape(str(b.get('user_name', '—')))}  |  "
                f"<b>Банк:</b> {html_escape(str(b.get('phone', '—')))}"
            )
            tu = _format_tg_username(b.get("tg_username"))
            if tu:
                lines.append(f"<b>Telegram:</b> {html_escape(tu)}")
        lines.append("")
    return "\n".join(lines).strip()


@router.callback_query(F.data == "book:my")
async def my_booking(callback: CallbackQuery, db: Database, config: Config) -> None:
    rows = await db.get_user_active_bookings(callback.from_user.id)
    if not rows:
        await _present_my_bookings_message(
            callback,
            "Активных заявок нет.",
            main_menu_kb(),
        )
        await callback.answer()
        return
    addr = await studio_address_html(db)
    text = await _format_my_bookings_screen(rows, db, config, studio_address_html=addr)
    ids = [int(b["id"]) for b in rows]
    has_studio = any((b.get("booking_kind") or "studio") == "studio" for b in rows)
    await _present_my_bookings_message(
        callback,
        text,
        my_bookings_kb(rows, show_directions=has_studio),
    )
    await callback.answer()


@router.callback_query(F.data == "book:directions")
async def book_directions_to_studio(callback: CallbackQuery, db: Database) -> None:
    rows = await db.get_user_active_bookings(callback.from_user.id)
    has_studio = any((b.get("booking_kind") or "studio") == "studio" for b in rows)
    if not has_studio:
        await callback.answer("Нет активной записи на студию", show_alert=True)
        return
    vid = await studio_directions_video_file_id(db)
    addr = await studio_address_html(db)
    if not vid and not addr:
        await callback.answer("Маршрут ещё не настроен администратором", show_alert=True)
        return
    await callback.answer()
    uid = callback.from_user.id
    if vid:
        cap = "<b>Как пройти до студии</b>"
        if addr:
            cap = f"{cap}\n\n{addr}"
        cap = _truncate_html(cap, 1024)
        try:
            await callback.bot.send_video(
                uid,
                video=vid,
                caption=cap,
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            await callback.bot.send_message(
                uid,
                "Не удалось отправить видео. Напишите администратору.",
            )
    else:
        await callback.bot.send_message(
            uid,
            _truncate_html(f"<b>Адрес студии</b>\n\n{addr}", 4096),
            parse_mode=ParseMode.HTML,
        )


@router.callback_query(F.data.startswith("book:cancel:"))
async def cancel_prompt(callback: CallbackQuery, db: Database, config: Config) -> None:
    booking_id = int(callback.data.split(":")[2])
    booking = await db.get_booking_by_id(booking_id)
    if not booking or booking["user_id"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    kind = booking.get("booking_kind") or "studio"
    st = booking.get("status")
    if st not in ("active", "pending_payment"):
        await callback.answer("Заявка не может быть отменена в этом статусе.", show_alert=True)
        return
    if kind in ("lyrics", "beat"):
        lines = ["<b>Отменить заявку?</b>", ""]
        if st == "pending_payment":
            lines.append(
                "Оплата ещё не подтверждена оператором. Заявка будет снята сразу, без ожидания ответа."
            )
        else:
            lines.append("Запрос на отмену будет отправлен оператору.")
    else:
        lines = ["<b>Отменить запись?</b>", ""]
        warn = await cancel_refund_warning_html(db, config)
        if Database.booking_time_started(booking, timezone=config.timezone) and warn:
            lines.append(warn)
            lines.append("")
        lines.append(
            "Запрос на отмену будет отправлен оператору — слот останется занятым до подтверждения."
        )
    text = "\n".join(lines)
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(
                caption=_truncate_html(text, 1024),
                reply_markup=cancel_confirm_kb(booking_id),
                parse_mode=ParseMode.HTML,
            )
        else:
            await callback.message.edit_text(
                _truncate_html(text, 4096),
                reply_markup=cancel_confirm_kb(booking_id),
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        await callback.message.answer(
            _truncate_html(text, 4096),
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_confirm_kb(booking_id),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("book:cc:no:"))
async def cancel_prompt_abort(callback: CallbackQuery, db: Database, config: Config, state: FSMContext) -> None:
    await state.clear()
    rows = await db.get_user_active_bookings(callback.from_user.id)
    if not rows:
        await _present_my_bookings_message(callback, "Активных заявок нет.", main_menu_kb())
        await callback.answer()
        return
    addr = await studio_address_html(db)
    text = await _format_my_bookings_screen(rows, db, config, studio_address_html=addr)
    has_studio = any((b.get("booking_kind") or "studio") == "studio" for b in rows)
    await _present_my_bookings_message(
        callback, text, my_bookings_kb(rows, show_directions=has_studio)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("book:cc:yes:"))
async def cancel_request_send(
    callback: CallbackQuery,
    db: Database,
    config: Config,
    state: FSMContext,
) -> None:
    booking_id = int(callback.data.split(":")[3])
    booking = await db.get_booking_by_id(booking_id)
    if not booking or booking["user_id"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    bk = booking.get("booking_kind") or "studio"
    st = booking.get("status")

    if bk in ("lyrics", "beat") and st == "pending_payment":
        await state.clear()
        snap = dict(booking)
        row = await db.cancel_booking(booking_id)
        if not row:
            await callback.answer("Не удалось отменить заявку.", show_alert=True)
            return
        await delete_booking_pending_ui_messages(callback.bot, snap, db)
        try:
            await _publish_weekly_and_tasks(callback.bot, db, config)
        except Exception:
            pass
        s = await db.get_all_settings()
        inbox = effective_payments_inbox_chat_id(s, config)
        client_tg = _format_tg_username(booking.get("tg_username"))
        note = (
            "<b>ℹ️ Клиент отменил заявку до подтверждения оплаты</b>\n\n"
            f"<b>ID:</b> <code>#{booking_id}</code>\n"
            f"<b>Услуга:</b> {html_escape(str(booking.get('services', '—')))}\n"
            f"<b>Клиент:</b> {html_escape(str(booking['user_name']))}\n"
            f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
            f"<b>Сумма:</b> {booking['total_price']} руб"
        )
        try:
            await callback.bot.send_message(inbox, note, parse_mode=ParseMode.HTML)
        except Exception:
            try:
                await callback.bot.send_message(
                    config.admin_id,
                    note + "\n\n<i>(не удалось отправить в чат заявок)</i>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        ann = await append_manager_contact_html(
            db,
            "<b>Заявка отменена</b>\n\nОплата ещё не была подтверждена.",
            config,
        )
        try:
            await delete_pending_ui_and_send_main_menu(
                callback.bot,
                db,
                config,
                booking_snapshot=row,
                announcement_html=ann,
            )
        except Exception:
            pass
        await callback.answer("Заявка отменена")
        return

    row = await db.request_user_cancellation(booking_id, callback.from_user.id)
    if not row:
        await callback.answer("Не удалось отправить запрос.", show_alert=True)
        return
    await state.clear()
    # Сначала быстро отвечаем пользователю (без ожидания отправки в админ-чат).
    client_tg = _format_tg_username(booking.get("tg_username"))
    if bk in ("lyrics", "beat"):
        notes = (booking.get("notes") or "").strip().replace("\n", " ")
        if len(notes) > 400:
            notes = notes[:397] + "..."
        admin_text = (
            "<b>❌ Запрос на отмену заявки (текст / бит)</b>\n\n"
            f"<b>ID:</b> <code>#{booking_id}</code>\n"
            f"<b>Клиент:</b> {html_escape(str(booking['user_name']))}\n"
            f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
            f"<b>Услуга:</b> {html_escape(str(booking['services']))}\n"
            f"<b>Сумма:</b> {booking['total_price']} руб\n"
            f"<b>Пожелания:</b> {html_escape(notes or '—')}"
        )
    else:
        admin_text = (
            "<b>❌ Запрос на отмену записи</b>\n\n"
            f"<b>ID:</b> <code>#{booking_id}</code>\n"
            f"<b>Клиент:</b> {html_escape(str(booking['user_name']))}\n"
            f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
            f"<b>Дата:</b> {html_escape(str(booking['day']))}\n"
            f"<b>Время:</b> {html_escape(str(booking['start_time']))} — {html_escape(str(booking['end_time']))}\n"
            f"<b>Услуги:</b> {html_escape(str(booking['services']))}\n"
            f"<b>Сумма:</b> {booking['total_price']} руб"
        )
    studio_wait = bk not in ("lyrics", "beat")
    wait_body = await cancel_request_sent_body_html(db, studio=studio_wait)
    wait = await append_manager_contact_html(db, wait_body, config)
    uid = callback.from_user.id
    act = await db.get_user_activity_message(uid)
    chat_id = callback.message.chat.id
    wait_mid = callback.message.message_id
    if act:
        await db.set_user_activity_notice(uid, wait)
        await _render_user_activity_message(callback.bot, db, config, uid)
        wait_mid = int(act["message_id"])
        chat_id = int(act["chat_id"])
    else:
        try:
            if getattr(callback.message, "photo", None):
                await callback.message.edit_caption(
                    caption=_truncate_html(wait, 1024),
                    reply_markup=None,
                    parse_mode=ParseMode.HTML,
                )
            else:
                await callback.message.edit_text(
                    _truncate_html(wait, 4096),
                    reply_markup=None,
                    parse_mode=ParseMode.HTML,
                )
        except Exception:
            sent = await callback.message.answer(wait, parse_mode=ParseMode.HTML)
            wait_mid = sent.message_id
            chat_id = sent.chat.id
    await db.update_booking_client_cleanup(
        booking_id,
        json.dumps(
            {"chat_id": chat_id, "root": wait_mid, "extra": []},
            ensure_ascii=False,
        ),
    )
    await callback.answer("Запрос отправлен")

    # Отправка оператору — в фоне, чтобы кнопка «отмена» отвечала мгновенно.
    async def _notify_admin_cancel_request() -> None:
        try:
            s = await db.get_all_settings()
            inbox = effective_payments_inbox_chat_id(s, config)
            await callback.bot.send_message(
                inbox,
                admin_text,
                parse_mode=ParseMode.HTML,
                reply_markup=_cancellation_review_kb(booking_id),
            )
        except Exception:
            try:
                await callback.bot.send_message(
                    config.admin_id,
                    admin_text + "\n\n<i>(не удалось отправить в чат заявок)</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_cancellation_review_kb(booking_id),
                )
            except Exception:
                pass

    asyncio.create_task(_notify_admin_cancel_request())


def _rs_slots_caption(day: str, n_selected: int, need: int) -> str:
    return (
        f"<b>Новая дата:</b> {day}\n\n"
        f"Отметьте <b>{need}</b> подряд идущих свободных часов, затем «Далее».\n"
        f"<b>Выбрано:</b> {n_selected} / {need}"
    )


@router.callback_query(F.data.startswith("book:rsch:"))
async def reschedule_start(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    bid = int(callback.data.split(":")[2])
    booking = await db.get_booking_by_id(bid)
    if not booking or booking["user_id"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    if (booking.get("booking_kind") or "studio") != "studio" or booking.get("status") != "active":
        await callback.answer(
            "Перенос доступен только для подтверждённой записи на студию.",
            show_alert=True,
        )
        return
    if booking.get("yclients_record_id") and yc.yclients_studio_enabled(config):
        await callback.answer(
            "Перенос для записи из Yclients пока в CRM: напишите в студию или администратору.",
            show_alert=True,
        )
        return
    hours = _booking_hours_count(booking)
    eng = int(booking.get("requires_engineer") or 0) == 1
    y, m = now_month()
    await state.set_state(BookingStates.reschedule_pick_date)
    await state.update_data(
        rs_bid=bid,
        rs_hours=hours,
        rs_engineer=eng,
        cal_year=y,
        cal_month=m,
    )
    available_days = set(await db.get_available_days())
    if eng:
        available_days = await db.filter_days_for_engineer_booking(available_days)
    closed_admin = await db.get_closed_days_in_month(y, m)
    blocked = set(closed_admin)
    if eng:
        blocked |= await db.get_engineer_unavailable_days_in_month(y, m)
    await _edit(
        callback.message,
        "<b>📅 Выберите новую дату</b>\n\n"
        "Число — есть свободные слоты под вашу длительность.",
        reply_markup=month_calendar_kb(
            y,
            m,
            allowed_days=available_days,
            blocked_days=blocked,
            prefix="rsdate",
            nav_prefix="rscal",
            nav_back_callback="book:rsc:quit",
            nav_back_text="⬅ Отмена",
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rscal:"), BookingStates.reschedule_pick_date)
async def reschedule_calendar_nav(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    payload = callback.data.split(":", maxsplit=1)[1]
    if payload == "back":
        await callback.answer()
        return
    y_str, m_str = payload.split("-")
    y, m = int(y_str), int(m_str)
    await state.update_data(cal_year=y, cal_month=m)
    available_days = set(await db.get_available_days())
    data = await state.get_data()
    eng = bool(data.get("rs_engineer"))
    if eng:
        available_days = await db.filter_days_for_engineer_booking(available_days)
    closed_admin = await db.get_closed_days_in_month(y, m)
    blocked = set(closed_admin)
    if eng:
        blocked |= await db.get_engineer_unavailable_days_in_month(y, m)
    await _edit(
        callback.message,
        "<b>📅 Выберите новую дату</b>",
        reply_markup=month_calendar_kb(
            y,
            m,
            allowed_days=available_days,
            blocked_days=blocked,
            prefix="rsdate",
            nav_prefix="rscal",
            nav_back_callback="book:rsc:quit",
            nav_back_text="⬅ Отмена",
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rsdate:"), BookingStates.reschedule_pick_date)
async def reschedule_pick_day(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    picked_day = callback.data.split(":", maxsplit=1)[1]
    slots = await db.get_all_slots_for_day(picked_day)
    if not slots:
        await callback.answer("На эту дату нет слотов.", show_alert=True)
        return
    if not any(Database.slot_row_is_active(s["is_active"]) for s in slots):
        await callback.answer("Нет свободных слотов", show_alert=True)
        return
    data = await state.get_data()
    need = int(data.get("rs_hours") or 1)
    await state.set_state(BookingStates.reschedule_pick_slot)
    await state.update_data(rs_day=picked_day, rs_selected_slot_ids=[])
    await _edit(
        callback.message,
        _rs_slots_caption(picked_day, 0, need),
        reply_markup=slots_rs_pick_kb(slots, set()),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rs_pick:"), BookingStates.reschedule_pick_slot)
async def reschedule_slot_toggle(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    sid = int(callback.data.split(":", maxsplit=1)[1])
    data = await state.get_data()
    day = data.get("rs_day")
    need = int(data.get("rs_hours") or 1)
    if not day:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    slots = await db.get_all_slots_for_day(day)
    by_id = {int(s["id"]): s for s in slots}
    if sid not in by_id or not Database.slot_row_is_active(by_id[sid]["is_active"]):
        await callback.answer("Этот час недоступен", show_alert=True)
        return
    raw = data.get("rs_selected_slot_ids") or []
    selected = {int(x) for x in raw}
    if sid in selected:
        selected.discard(sid)
    else:
        selected.add(sid)
    stable_ids = sorted(int(x) for x in selected)
    await state.update_data(rs_selected_slot_ids=stable_ids)
    await _edit(
        callback.message,
        _rs_slots_caption(day, len(selected), need),
        reply_markup=slots_rs_pick_kb(slots, set(stable_ids)),
    )
    await callback.answer()


@router.callback_query(F.data == "rs_slot_confirm", BookingStates.reschedule_pick_slot)
async def reschedule_slot_confirm(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    day = data.get("rs_day")
    need = int(data.get("rs_hours") or 1)
    bid = int(data.get("rs_bid") or 0)
    raw = data.get("rs_selected_slot_ids") or []
    if not day or not bid:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    if len(raw) != need:
        await callback.answer(f"Нужно выбрать ровно {need} час(ов) подряд.", show_alert=True)
        return
    slots = await db.get_all_slots_for_day(day)
    id_set = {int(x) for x in raw}
    chosen = [s for s in slots if int(s["id"]) in id_set]
    if len(chosen) != need:
        await callback.answer("Часть слотов недоступна", show_alert=True)
        return
    if any(not Database.slot_row_is_active(s["is_active"]) for s in chosen):
        await callback.answer("Часть часов уже занята", show_alert=True)
        return
    if not Database.selection_is_valid_multihour_slot_chain(slots, id_set, chosen):
        await callback.answer("Выберите только соседние часы подряд.", show_alert=True)
        return
    chosen_sorted = sorted(
        chosen,
        key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"])),
    )
    slot_text = f"{chosen_sorted[0]['start_time']} — {chosen_sorted[-1]['end_time']}"
    ids_ordered = [int(s["id"]) for s in chosen_sorted]
    await state.update_data(rs_new_day=day, rs_new_slot_ids=ids_ordered)
    preview = (
        f"<b>Перенос записи #{bid}</b>\n\n"
        f"<b>Новая дата:</b> {html_escape(day)}\n"
        f"<b>Новое время:</b> {html_escape(slot_text)}\n\n"
        "Запрос уйдёт оператору на подтверждение."
    )
    await _edit(
        callback.message,
        preview,
        reply_markup=reschedule_confirm_kb(bid),
    )
    await callback.answer()


@router.callback_query(F.data == "rscal:back", BookingStates.reschedule_pick_slot)
async def reschedule_slot_back_calendar(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    y = int(data.get("cal_year") or now_month()[0])
    m = int(data.get("cal_month") or now_month()[1])
    eng = bool(data.get("rs_engineer"))
    await state.set_state(BookingStates.reschedule_pick_date)
    await state.update_data(rs_selected_slot_ids=[], rs_day=None)
    available_days = set(await db.get_available_days())
    if eng:
        available_days = await db.filter_days_for_engineer_booking(available_days)
    closed_admin = await db.get_closed_days_in_month(y, m)
    blocked = set(closed_admin)
    if eng:
        blocked |= await db.get_engineer_unavailable_days_in_month(y, m)
    await _edit(
        callback.message,
        "<b>📅 Выберите новую дату</b>",
        reply_markup=month_calendar_kb(
            y,
            m,
            allowed_days=available_days,
            blocked_days=blocked,
            prefix="rsdate",
            nav_prefix="rscal",
            nav_back_callback="book:rsc:quit",
            nav_back_text="⬅ Отмена",
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("book:rsc:abort:"))
async def reschedule_abort_confirm(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    await state.clear()
    rows = await db.get_user_active_bookings(callback.from_user.id)
    if not rows:
        await _present_my_bookings_message(callback, "Активных заявок нет.", main_menu_kb())
        await callback.answer()
        return
    addr = await studio_address_html(db)
    text = await _format_my_bookings_screen(rows, db, config, studio_address_html=addr)
    has_studio = any((b.get("booking_kind") or "studio") == "studio" for b in rows)
    await _present_my_bookings_message(
        callback, text, my_bookings_kb(rows, show_directions=has_studio)
    )
    await callback.answer()


@router.callback_query(F.data == "book:rsc:quit")
async def reschedule_quit(
    callback: CallbackQuery, state: FSMContext, db: Database, config: Config
) -> None:
    await state.clear()
    rows = await db.get_user_active_bookings(callback.from_user.id)
    if not rows:
        await _present_my_bookings_message(callback, "Активных заявок нет.", main_menu_kb())
        await callback.answer()
        return
    addr = await studio_address_html(db)
    text = await _format_my_bookings_screen(rows, db, config, studio_address_html=addr)
    has_studio = any((b.get("booking_kind") or "studio") == "studio" for b in rows)
    await _present_my_bookings_message(
        callback, text, my_bookings_kb(rows, show_directions=has_studio)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("book:rsc:send:"))
async def reschedule_request_send(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
) -> None:
    booking_id = int(callback.data.split(":")[3])
    data = await state.get_data()
    if int(data.get("rs_bid") or 0) != booking_id:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    new_day = data.get("rs_new_day")
    new_ids = data.get("rs_new_slot_ids") or []
    if not new_day or not new_ids:
        await callback.answer("Сначала выберите дату и время.", show_alert=True)
        return
    booking = await db.get_booking_by_id(booking_id)
    if not booking or booking["user_id"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    row = await db.request_user_reschedule(
        booking_id,
        callback.from_user.id,
        new_day=str(new_day),
        new_slot_ids=[int(x) for x in new_ids],
    )
    if not row:
        await callback.answer(
            "Не удалось оформить запрос (слоты могли занять). Попробуйте снова.",
            show_alert=True,
        )
        return
    await state.clear()
    s = await db.get_all_settings()
    inbox = effective_payments_inbox_chat_id(s, config)

    meta = json.loads(row.get("pending_meta") or "{}")
    st = meta.get("slot_text", "—")
    client_tg = _format_tg_username(booking.get("tg_username"))
    admin_text = (
        "<b>📅 Запрос на перенос записи</b>\n\n"
        f"<b>ID:</b> <code>#{booking_id}</code>\n"
        f"<b>Клиент:</b> {html_escape(str(booking['user_name']))}\n"
        f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
        f"<b>Было:</b> {html_escape(str(booking['day']))} "
        f"{html_escape(str(booking['start_time']))}—{html_escape(str(booking['end_time']))}\n"
        f"<b>Станет:</b> {html_escape(str(new_day))} {html_escape(st)}\n"
        f"<b>Услуги:</b> {html_escape(str(booking['services']))}\n"
        f"<b>Сумма:</b> {booking['total_price']} руб"
    )
    try:
        await callback.bot.send_message(
            inbox,
            admin_text,
            parse_mode=ParseMode.HTML,
            reply_markup=_reschedule_review_kb(booking_id),
        )
    except Exception:
        await callback.bot.send_message(
            config.admin_id,
            admin_text + "\n\n<i>(не удалось отправить в чат заявок)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=_reschedule_review_kb(booking_id),
        )
    wait = await append_manager_contact_html(
        db,
        "<b>⏳ Запрос на перенос отправлен</b>\n\n"
        "Ожидайте решения оператора.",
        config,
    )
    chat_id = callback.message.chat.id
    wait_mid = callback.message.message_id
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(
                caption=_truncate_html(wait, 1024),
                reply_markup=None,
                parse_mode=ParseMode.HTML,
            )
        else:
            await callback.message.edit_text(
                _truncate_html(wait, 4096),
                reply_markup=None,
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        sent = await callback.message.answer(wait, parse_mode=ParseMode.HTML)
        wait_mid = sent.message_id
        chat_id = sent.chat.id
    await db.update_booking_client_cleanup(
        booking_id,
        json.dumps(
            {"chat_id": chat_id, "root": wait_mid, "extra": []},
            ensure_ascii=False,
        ),
    )
    await callback.answer("Запрос отправлен")


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery) -> None:
    await callback.answer()

