from __future__ import annotations

import asyncio
import json
import os
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
    format_maker_username as _format_maker_username,
    post_payment_contact_block_html,
    append_manager_contact_html,
    cancel_refund_warning_html,
    manager_contact_html,
    studio_address_html,
    studio_directions_video_file_id,
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
    reschedule_confirm_kb,
    slots_pick_kb,
    slots_rs_pick_kb,
    studio_mode_kb,
    subscription_kb,
    tariff_category_kb,
    tariff_day_start_kb,
    tariff_hours_kb,
)
from services.effective_pricing import EffectivePricing
from services.reminders import ReminderService
from services.subscription import is_subscribed
from states import BookingStates


router = Router()

def _truncate_html(text: str, limit: int) -> str:
    """Обрезка HTML-текста под лимит Telegram для обычных сообщений / подписей."""
    t = (text or "").strip()
    if len(t) <= limit:
        return t
    cut = max(1, limit - 10)
    return t[:cut].rstrip() + "…"


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


async def _upsert_channel_message(
    bot,
    db: Database,
    *,
    key: str,
    chat_id: int,
    text: str,
) -> None:
    """
    Одно "закреплённое" сообщение: редактируем, а если не вышло — отправляем новое и запоминаем message_id.
    """
    if not chat_id:
        return
    text = (text or "").strip()
    if not text:
        text = "—"
    # Ограничение Telegram: 4096 символов для текста
    if len(text) > 4096:
        text = text[:4090] + "…"
    stored = await db.get_bot_message(key)
    if stored and int(stored.get("chat_id", 0)) == int(chat_id):
        mid = int(stored.get("message_id", 0))
        if mid:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, parse_mode=ParseMode.HTML)
                return
            except TelegramBadRequest as e:
                # Важно: если текст не изменился, Telegram кидает ошибку — но это НЕ повод слать новое сообщение.
                if "message is not modified" in str(e).lower():
                    return
            except Exception:
                pass
    msg = await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    await db.upsert_bot_message(key, int(chat_id), int(msg.message_id))


async def _publish_weekly_and_tasks(bot, db: Database, cfg: Config) -> None:
    """
    3 сообщения в канале расписания:
    - расписание на ближайшие 7 дней
    - задачи текстовика
    - задачи битмейкера
    """
    s = await db.get_all_settings()
    sch_id = effective_schedule_channel_id(s, cfg)
    if not sch_id:
        return
    from datetime import date as _d, timedelta as _td

    days = [(_d.today() + _td(days=i)).isoformat() for i in range(7)]
    lines = ["<b>📅 Расписание (7 дней)</b>", ""]
    for day in days:
        schedule = await db.get_day_schedule(day)
        lines.append(f"<b>{day}</b>")
        # Табличный вывод в 2 колонки (00-11 и 12-23)
        left = [r for r in schedule if int(Database.time_sort_key(Database._coerce_cell_str(r["start_time"]))[0]) < 12]
        right = [r for r in schedule if int(Database.time_sort_key(Database._coerce_cell_str(r["start_time"]))[0]) >= 12]
        n = max(len(left), len(right))
        table_lines: list[str] = []
        for i in range(n):
            def cell(r: dict | None) -> str:
                if not r:
                    return ""
                st = Database.normalize_time_str(r["start_time"])
                et = Database.normalize_time_str(r["end_time"])
                if r["is_active"]:
                    mark = "🟢"
                else:
                    # 🟠 — занято со звукорежиссёром, 🔴 — занято без
                    mark = "🟠" if int(r.get("requires_engineer") or 0) == 1 else "🔴"
                return f"{mark} {st}-{et}"
            l = cell(left[i]) if i < len(left) else ""
            rr = cell(right[i]) if i < len(right) else ""
            table_lines.append(f"{l:<12}    {rr}")
        legend = "<i>🟢 свободно · 🟠 занято (со звукорежиссёром) · 🔴 занято (без звукорежиссёра)</i>"
        lines.append(legend)
        lines.append("<pre>" + "\n".join(html_escape(x) for x in table_lines).strip() + "</pre>")
        lines.append("")
    await _upsert_channel_message(
        bot, db, key="schedule_week_7d", chat_id=sch_id, text="\n".join(lines).strip()
    )

    async def _tasks(kind: str, title: str) -> str:
        orders = await db.get_active_service_orders(kind)
        maker_u = cfg.textmaker_username if kind == "lyrics" else cfg.beatmaker_username
        out = [f"<b>{title}</b>", ""]
        out.append(f"<b>Исполнитель:</b> {html_escape(_format_maker_username(maker_u))}")
        out.append("")
        if not orders:
            out.append("Нет активных заявок.")
            return "\n".join(out)
        for o in orders:
            client_u = _format_tg_username(o.get("tg_username"))
            notes = (o.get("notes") or "").strip().replace("\n", " ")
            if len(notes) > 250:
                notes = notes[:247] + "..."
            out.append(
                f"<b>#{o['id']}</b> — {html_escape(str(o.get('services','—')))} — <b>{o.get('total_price',0)} руб</b>\n"
                f"<b>Клиент:</b> {html_escape(str(o.get('user_name','')))}\n"
                f"<b>Банк:</b> {html_escape(str(o.get('phone','')))}\n"
                f"<b>Telegram клиента:</b> {html_escape(client_u) if client_u else '—'}\n"
                f"{html_escape(notes) if notes else '—'}"
            )
            out.append("")
        return "\n".join(out).strip()

    await _upsert_channel_message(
        bot,
        db,
        key="tasks_lyrics",
        chat_id=sch_id,
        text=await _tasks("lyrics", "📝 Задачи текстовика"),
    )
    await _upsert_channel_message(
        bot,
        db,
        key="tasks_beat",
        chat_id=sch_id,
        text=await _tasks("beat", "🎚️ Задачи битмейкера"),
    )


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


async def delete_booking_pending_ui_messages(bot: Bot, booking: dict[str, Any]) -> None:
    """Удаляет у клиента экран ожидания и сообщение с контактами (сохранённые при «Я оплатил»)."""
    raw = booking.get("client_cleanup_json")
    if not raw:
        return
    try:
        p = json.loads(raw)
        cid = int(p["chat_id"])
        mids: list[int] = []
        if p.get("root") is not None:
            mids.append(int(p["root"]))
        mids.extend(int(x) for x in (p.get("extra") or []))
        for mid in mids:
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
    """После решения по заявке: убрать экран ожидания у клиента и показать главное меню."""
    await delete_booking_pending_ui_messages(bot, booking_snapshot)
    uid = int(booking_snapshot["user_id"])
    ann = (announcement_html or "").strip()
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
) -> tuple[bool, str]:
    """pending_payment → active, напоминания, канал, сводка пользователю, уведомление в чат."""
    row = await db.confirm_booking_payment(booking_id)
    if not row:
        return False, "Заявка не найдена или уже обработана"
    await delete_booking_pending_ui_messages(bot, row)
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


def _prices_text(cfg: Config, pricing: EffectivePricing) -> str:
    ne = pricing.price_no_engineer
    we = pricing.price_with_engineer
    ly = pricing.price_lyrics
    bt = pricing.price_beat
    return (
        "<b>💳 Прайс студии</b>\n\n"
        "<b>Почасовая запись</b>\n"
        f"🎤 1 час без звукорежиссёра — <b>{ne} руб</b>\n"
        f"🎛️ 1 час с звукорежиссёром — <b>{we} руб</b>\n\n"
        "<b>Тарифы — без звукорежиссёра</b>\n"
        "<i>Ночь (с 00:00):</i>\n"
        f"🌙 6 ч — <b>{pricing.tariff_night_6h} руб</b> · 8 ч — <b>{pricing.tariff_night_8h} руб</b>\n"
        f"🌙 10 ч — <b>{pricing.tariff_night_10h} руб</b> · 12 ч — <b>{pricing.tariff_night_12h} руб</b>\n"
        "<i>День (09:00 / 12:00):</i>\n"
        f"☀️ 6 ч — <b>{pricing.tariff_day_6h} руб</b> · 8 ч — <b>{pricing.tariff_day_8h} руб</b>\n"
        f"☀️ 10 ч — <b>{pricing.tariff_day_10h} руб</b> · 12 ч — <b>{pricing.tariff_day_12h} руб</b>\n\n"
        "<b>Тарифы — со звукорежиссёром</b>\n"
        "<i>Ночь (с 00:00):</i>\n"
        f"🌙 6 ч — <b>{pricing.tariff_night_6h_engineer} руб</b> · 8 ч — <b>{pricing.tariff_night_8h_engineer} руб</b>\n"
        f"🌙 10 ч — <b>{pricing.tariff_night_10h_engineer} руб</b> · 12 ч — <b>{pricing.tariff_night_12h_engineer} руб</b>\n"
        "<i>День (09:00 / 12:00):</i>\n"
        f"☀️ 6 ч — <b>{pricing.tariff_day_6h_engineer} руб</b> · 8 ч — <b>{pricing.tariff_day_8h_engineer} руб</b>\n"
        f"☀️ 10 ч — <b>{pricing.tariff_day_10h_engineer} руб</b> · 12 ч — <b>{pricing.tariff_day_12h_engineer} руб</b>\n\n"
        "<b>Услуги</b>\n"
        f"📝 Текст для вашей песни — <b>{ly} руб</b>\n"
        f"🎚️ Бит для песни — <b>{bt} руб</b>"
    )


_ACTIVITY_SEP = "\n\n───────────────\n\n"
_ACTIVITY_HEADER = "<b>✅ Оформленные заявки</b>\n\n"
_ACTIVITY_FOOTER = "\n\n<b>🏠 Главное меню</b> — выберите действие ниже."
_MAX_ACTIVITY_BODY_STORE = 10000


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

    full = _ACTIVITY_HEADER + body + _ACTIVITY_FOOTER
    caption_display = _truncate_html(full, 1024)
    text_display = _truncate_html(full, 4096)

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
        await db.upsert_user_activity_message(user_id, chat_id, int(msg.message_id), body)
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
    target_mid = int(root_id) if root_id else int(callback.message.message_id)

    if prompt_id:
        try:
            await callback.bot.delete_message(chat_id, prompt_id)
        except Exception:
            pass

    await state.clear()

    await _present_main_menu_on_message(
        callback.bot,
        chat_id=chat_id,
        message_id=target_mid,
        config=config,
        db=db,
    )
    await callback.answer()


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

    # Текст/бит: одно текстовое сообщение — шаг «описание» и «оплата» меняют его через edit (без лимита подписи к фото).
    price = pricing.service_price(code)
    await state.set_state(BookingStates.entering_brief)
    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass
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
    s = await db.get_all_settings()
    sent_id, is_photo = await _send_payment_screen_message(
        callback.bot,
        chat_id=chat_id,
        text=screen,
        reply_markup=back_to_menu_kb(),
        photo_path=ui_photo_payment(s, config),
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
            await show_calendar(callback, state, db)
        return
    await callback.answer("Подписка не найдена", show_alert=True)


async def show_studio_mode(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    """Почасовая запись или тарифы (после выбора записи с/без звукорежиссёра)."""
    await state.set_state(BookingStates.choosing_studio_mode)
    await state.update_data(booking_mode=None, tariff_label=None)
    s = await db.get_all_settings()
    await _edit_screen(
        callback,
        "<b>Запись на студию</b>\n\n"
        "Выберите режим:\n"
        "• <b>Почасовая запись</b> — сами отмечаете нужные часы.\n"
        "• <b>Тарифы</b> — пакеты на 6 / 8 / 10 / 12 ч: "
        "<b>ночная</b> (с 00:00) или <b>дневная</b> (с 09:00 или 12:00).",
        studio_mode_kb(),
        photo_path=ui_photo_main_menu(s, config),
    )


async def show_calendar(callback: CallbackQuery, state: FSMContext, db: Database, year: int | None = None, month: int | None = None) -> None:
    available_days = set(await db.get_available_days(days_ahead=60))
    if year is None or month is None:
        year, month = now_month()
    await state.set_state(BookingStates.choosing_date)
    await state.update_data(cal_year=year, cal_month=month, booking_mode="hourly")

    data = await state.get_data()
    blocked: set[str] = set()
    if data.get("product") == "with_engineer":
        from datetime import datetime as _dt
        for d in list(available_days):
            wd = _dt.fromisoformat(d).weekday()  # 0..6
            if wd in (5, 6):  # Sat/Sun
                blocked.add(d)
        available_days = {d for d in available_days if d not in blocked}
    closed_admin = await db.get_closed_days_in_month(year, month)
    blocked = blocked | closed_admin
    await _edit(
        callback.message,
        "<b>📅 Выберите дату записи</b>\n\n"
        "✅ — есть свободные слоты\n"
        "❌ — недоступно (прошедшие даты, выходные при записи с звукорежиссёром, день закрыт студией)\n\n"
        "<i>Показываем доступность на ближайшие 60 дней.</i>",
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
    block_weekends = product == "with_engineer"
    allowed = await db.get_days_with_free_tariff_block(
        days_ahead=60,
        start_hhmm=start_hhmm,
        hour_count=hours,
        block_weekends=block_weekends,
    )
    if year is None or month is None:
        year, month = now_month()
    await state.update_data(cal_year=year, cal_month=month)
    st_label, end_label = Database.tariff_time_range_label(start_hhmm, hours)
    caption = (
        f"<b>Ваше время: {st_label} — {end_label}</b>\n\n"
        "<b>📅 Выберите дату</b>\n\n"
        "✅ — весь интервал свободен для брони\n"
        "❌ — нельзя выбрать (прошлые дни, занято, день закрыт студией или выходные при записи со звукорежиссёром)\n\n"
        "<i>После выбора даты блокируется весь указанный отрезок.</i>"
    )
    closed_admin = await db.get_closed_days_in_month(year, month)
    mk = month_calendar_kb(
        year,
        month,
        allowed_days=allowed,
        blocked_days=closed_admin,
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
async def back_to_calendar(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    await show_calendar(callback, state, db, year=data.get("cal_year"), month=data.get("cal_month"))
    await callback.answer()


@router.callback_query(F.data.startswith("cal:"))
async def calendar_nav(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    payload = callback.data.split(":", maxsplit=1)[1]
    if payload == "today":
        y, m = now_month()
    else:
        y_str, m_str = payload.split("-")
        y, m = int(y_str), int(m_str)
    await show_calendar(callback, state, db, year=y, month=m)
    await callback.answer()


@router.callback_query(F.data == "book:pick_product")
async def back_to_pick_product(callback: CallbackQuery, state: FSMContext, pricing: EffectivePricing) -> None:
    await state.set_state(BookingStates.choosing_product)
    await _edit(callback.message, "<b>Выберите услугу</b>", reply_markup=booking_products_kb(pricing=pricing))
    await callback.answer()


@router.callback_query(F.data == "stm:hourly", BookingStates.choosing_studio_mode)
async def studio_mode_hourly(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    await state.update_data(booking_mode="hourly", tariff_label=None)
    await show_calendar(callback, state, db)
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
    await state.update_data(tariff_kind="night")
    data = await state.get_data()
    we = data.get("product") == "with_engineer"
    s = await db.get_all_settings()
    await _edit_screen(
        callback,
        "<b>🌙 Ночная запись</b>\n\n"
        "Интервал с <b>00:00</b> (например 6 ч → 00:00–06:00, 12 ч → 00:00–12:00).\n\n"
        "Выберите длительность:",
        tariff_hours_kb(night=True, pricing=pricing, with_engineer=we),
        photo_path=ui_photo_tariff_night(s, config),
    )
    await callback.answer()


@router.callback_query(F.data == "trf:c:day", BookingStates.choosing_tariff)
async def tariff_cat_day(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    await state.update_data(tariff_kind="day")
    s = await db.get_all_settings()
    await _edit_screen(
        callback,
        "<b>☀️ Дневная запись</b>\n\nС какого времени начинается сессия?",
        tariff_day_start_kb(),
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
    if part == "09":
        start = "09:00"
    elif part == "12":
        start = "12:00"
    else:
        await callback.answer()
        return
    await state.update_data(tariff_day_start=start)
    data = await state.get_data()
    we = data.get("product") == "with_engineer"
    s = await db.get_all_settings()
    await _edit_screen(
        callback,
        f"<b>☀️ Дневная запись</b> (начало в {start})\n\nВыберите длительность:",
        tariff_hours_kb(night=False, pricing=pricing, with_engineer=we),
        photo_path=ui_photo_tariff_day(s, config),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("trf:h:"), BookingStates.choosing_tariff)
async def tariff_pick_hours(callback: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    payload = callback.data.split(":")[2]
    if payload == "back":
        data = await state.get_data()
        if data.get("tariff_kind") == "day":
            s = await db.get_all_settings()
            await _edit_screen(
                callback,
                "<b>☀️ Дневная запись</b>\n\nС какого времени начинается сессия?",
                tariff_day_start_kb(),
                photo_path=ui_photo_tariff_day(s, config),
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
    if kind == "night":
        start = "00:00"
    elif kind == "day":
        start = data.get("tariff_day_start")
        if not start:
            await callback.answer("Сначала выберите время начала дня.", show_alert=True)
            return
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
        await _edit_screen(
            callback,
            "<b>🌙 Ночная запись</b>\n\nВыберите длительность:",
            tariff_hours_kb(night=True, pricing=pricing, with_engineer=we),
            photo_path=ui_photo_tariff_night(s, config),
        )
    elif kind == "day":
        start = data.get("tariff_day_start") or "09:00"
        await _edit_screen(
            callback,
            f"<b>☀️ Дневная запись</b> (начало в {start})\n\nВыберите длительность:",
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
    await state.set_state(BookingStates.entering_contacts)
    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass
    pay = (config.payment_details or "").strip().replace("\\n", "\n")
    screen1 = (
        "<b>💳 Реквизиты для оплаты</b>\n\n"
        f"<b>Услуга:</b> {html_escape(tariff_label)}\n"
        f"<b>Дата:</b> {picked_day}\n"
        f"<b>Время:</b> {slot_text}\n"
        f"<b>Часов:</b> {hours}\n"
        f"<b>Итого:</b> {total} руб\n\n"
        f"<b>Куда отправить:</b>\n{html_escape(pay)}\n\n"
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
    s = await db.get_all_settings()
    sent_id, is_photo = await _send_payment_screen_message(
        callback.bot,
        chat_id=chat_id,
        text=screen1,
        reply_markup=back_to_menu_kb(),
        photo_path=ui_photo_payment(s, config),
    )
    await state.update_data(
        payment_root_message_id=sent_id,
        payment_root_is_photo=is_photo,
        cleanup_ids=[],
    )


@router.callback_query(F.data.startswith("date:"), BookingStates.choosing_date)
async def pick_date(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    picked_day = callback.data.split(":", maxsplit=1)[1]
    slots = await db.get_all_slots_for_day(picked_day)
    if not slots:
        await callback.answer("На эту дату нет расписания слотов.", show_alert=True)
        return
    if not any(Database.slot_row_is_active(s["is_active"]) for s in slots):
        await callback.answer("На эту дату нет свободных слотов", show_alert=True)
        return
    await state.set_state(BookingStates.choosing_slot)
    await state.update_data(day=picked_day, selected_slot_ids=[])
    await _edit(
        callback.message,
        _slots_caption(picked_day, 0),
        reply_markup=slots_pick_kb(slots, set()),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("slot_pick:"), BookingStates.choosing_slot)
async def slot_toggle(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    sid = int(callback.data.split(":", maxsplit=1)[1])
    data = await state.get_data()
    day = data.get("day")
    if not day:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    slots = await db.get_all_slots_for_day(day)
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
    slots = await db.get_all_slots_for_day(day)
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
        fresh = await db.get_all_slots_for_day(day)
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
    hours = len(chosen_sorted)
    hourly_price = pricing.service_price(product)
    total = hours * hourly_price
    slot_text = f"{chosen_sorted[0]['start_time']} — {chosen_sorted[-1]['end_time']}"
    await state.update_data(
        slot_ids=ids_ordered,
        slot_text=slot_text,
        total=total,
        tariff_label=None,
        booking_mode="hourly",
    )
    await state.set_state(BookingStates.entering_contacts)

    await callback.answer()
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass

    pay = (config.payment_details or "").strip().replace("\\n", "\n")
    screen1 = (
        "<b>💳 Реквизиты для оплаты</b>\n\n"
        f"<b>Услуга:</b> {pricing.service_title(product)}\n"
        f"<b>Дата:</b> {day}\n"
        f"<b>Время:</b> {slot_text}\n"
        f"<b>Часов:</b> {hours}\n"
        f"<b>Итого:</b> {total} руб\n\n"
        f"<b>Куда отправить:</b>\n{html_escape(pay)}\n\n"
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
    s = await db.get_all_settings()
    sent_id, is_photo = await _send_payment_screen_message(
        callback.bot,
        chat_id=chat_id,
        text=screen1,
        reply_markup=back_to_menu_kb(),
        photo_path=ui_photo_payment(s, config),
    )
    await state.update_data(
        payment_root_message_id=sent_id,
        payment_root_is_photo=is_photo,
        cleanup_ids=[],
    )

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
    if len(lines) < 5:
        await message.answer(
            "Нужно минимум 5 строк: пожелания (можно несколько строк), затем имя, фамилию, банк и @username "
            "(или «—»).\n\n"
            "Пример:\n"
            "Хочу поп, женский вокал, минор...\n"
            "Иван\n"
            "Иванов\n"
            "Сбербанк\n"
            "@nickname"
        )
        return

    first_name = lines[-4]
    last_name = lines[-3]
    bank = lines[-2]
    tg_line = lines[-1]
    brief = "\n".join(lines[:-4]).strip()
    user_display_name = f"{first_name} {last_name}".strip()
    tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
    if len(brief) < 10 or len(first_name) < 1 or len(last_name) < 1 or len(bank) < 2:
        await message.answer(
            "Проверьте: пожелания (от 10 символов), имя, фамилию, банк (от 2 символов), username."
        )
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
        pay = (config.payment_details or "").strip()
        # payment_details may contain literal "\n" from .env; turn into real newlines for readability.
        pay = pay.replace("\\n", "\n")
        pay_screen = (
            "<b>💳 Реквизиты для оплаты</b>\n\n"
            f"<b>Услуга:</b> {pricing.service_title(product)}\n"
            f"<b>Стоимость:</b> {price} руб\n\n"
            f"<b>Куда отправить:</b>\n{html_escape(pay)}\n\n"
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
                reply_markup=paid_kb(),
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
                    reply_markup=paid_kb(),
                    photo_path=pay_path,
                )
                await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
        except Exception:
            new_id, new_ph = await _send_payment_screen_message(
                message.bot,
                chat_id=cid,
                text=pay_screen,
                reply_markup=paid_kb(),
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
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    if len(lines) < 4:
        await message.answer(
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
        return
    first_name, last_name, bank, tg_line = lines[0], lines[1], lines[2], lines[3]
    user_name = f"{first_name} {last_name}".strip()
    tg_u = _parse_tg_username_line(tg_line, message.from_user.username)
    if len(first_name) < 1 or len(last_name) < 1 or len(bank) < 2:
        await message.answer("Проверьте имя, фамилию и название банка (минимум 2 символа).")
        return
    await state.update_data(name=user_name, phone=bank, tg_username=tg_u)
    await state.set_state(BookingStates.waiting_payment)

    data = await state.get_data()
    product = data.get("product")
    if product not in ("no_engineer", "with_engineer"):
        return
    pay = (config.payment_details or "").strip().replace("\\n", "\n")
    day = data.get("day")
    slot_text = data.get("slot_text")
    total = data.get("total")
    slot_ids = data.get("slot_ids") or []
    hours = len(slot_ids)
    svc_line = html_escape(str(data.get("tariff_label") or pricing.service_title(product)))
    tg_disp = f"@{html_escape(tg_u)}" if tg_u else "—"
    screen2 = (
        "<b>💳 Реквизиты для оплаты</b>\n\n"
        f"<b>Услуга:</b> {svc_line}\n"
        f"<b>Дата:</b> {day}\n"
        f"<b>Время:</b> {slot_text}\n"
        f"<b>Часов:</b> {hours}\n"
        f"<b>Итого:</b> {total} руб\n\n"
        f"<b>Куда отправить:</b>\n{html_escape(pay)}\n\n"
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
                reply_markup=paid_kb(),
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
                    reply_markup=paid_kb(),
                    photo_path=pay_path,
                )
                await state.update_data(payment_root_message_id=new_id, payment_root_is_photo=new_ph)
        except Exception:
            new_id, new_ph = await _send_payment_screen_message(
                message.bot,
                chat_id=cid,
                text=screen2,
                reply_markup=paid_kb(),
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

    async def _edit_waiting() -> None:
        waiting = await append_manager_contact_html(
            db,
            "<b>⏳ Ожидайте подтверждения</b>\n\n"
            "Мы проверяем оплату. Как только оператор подтвердит заявку, "
            "вы получите уведомление в этот чат.",
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
            status="pending_payment",
        )
        booking = await db.get_booking_by_id(order_id)
        await save_booking_pending_ui_cleanup(db, order_id, chat_id=chat_id, data=data)
        client_tg = _format_tg_username(data.get("tg_username") or callback.from_user.username or "")
        maker = config.textmaker_username if product == "lyrics" else config.beatmaker_username
        admin_text = (
            "<b>💳 Подтверждение оплаты</b> <i>(текст/бит)</i>\n\n"
            f"<b>ID заявки:</b> <code>#{order_id}</code>\n"
            f"<b>Время заявки:</b> {html_escape(str(booking.get('created_at', '—')))}\n"
            f"<b>Услуга:</b> {html_escape(svc_title)}\n"
            f"<b>Клиент:</b> {html_escape(str(data.get('name', '')))}\n"
            f"<b>Банк:</b> {html_escape(str(data.get('phone', '')))}\n"
            f"<b>Telegram:</b> {html_escape(client_tg or '—')}\n"
            f"<b>Сумма:</b> {data.get('total', 0)} руб\n"
            f"<b>Исполнитель (.env):</b> {html_escape(_format_maker_username(maker))}\n\n"
            f"<b>Пожелания:</b>\n{html_escape(str(data.get('brief', '')))}"
        )
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

    booking_id = await db.create_booking(
        user_id=callback.from_user.id,
        user_name=data["name"],
        phone=data["phone"],
        tg_username=(data.get("tg_username") or callback.from_user.username or ""),
        requires_engineer=(product == "with_engineer"),
        slot_ids=[int(x) for x in data["slot_ids"]],
        services=services_human,
        total_price=int(data["total"]),
        status="pending_payment",
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


def _format_my_bookings_screen(
    rows: list[dict], config: Config, *, studio_address_html: str = ""
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
            maker = config.textmaker_username if kind == "lyrics" else config.beatmaker_username
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
    text = _format_my_bookings_screen(rows, config, studio_address_html=addr)
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
    if (booking.get("booking_kind") or "studio") != "studio":
        await callback.answer("Для этой заявки отмена через бота недоступна.", show_alert=True)
        return
    st = booking.get("status")
    if st not in ("active", "pending_payment"):
        await callback.answer("Заявка не может быть отменена в этом статусе.", show_alert=True)
        return
    lines: list[str] = ["<b>Отменить запись?</b>", ""]
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
    text = _format_my_bookings_screen(rows, config, studio_address_html=addr)
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
    row = await db.request_user_cancellation(booking_id, callback.from_user.id)
    if not row:
        await callback.answer("Не удалось отправить запрос.", show_alert=True)
        return
    await state.clear()
    s = await db.get_all_settings()
    inbox = effective_payments_inbox_chat_id(s, config)
    client_tg = _format_tg_username(booking.get("tg_username"))
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
    try:
        await callback.bot.send_message(
            inbox,
            admin_text,
            parse_mode=ParseMode.HTML,
            reply_markup=_cancellation_review_kb(booking_id),
        )
    except Exception:
        await callback.bot.send_message(
            config.admin_id,
            admin_text + "\n\n<i>(не удалось отправить в чат заявок)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=_cancellation_review_kb(booking_id),
        )
    wait = await append_manager_contact_html(
        db,
        "<b>⏳ Запрос на отмену отправлен</b>\n\n"
        "Ожидайте решения оператора. Слот пока занят.",
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
    available_days = set(await db.get_available_days(days_ahead=60))
    blocked: set[str] = set()
    if eng:
        from datetime import datetime as _dt

        for d in list(available_days):
            wd = _dt.fromisoformat(d).weekday()
            if wd in (5, 6):
                blocked.add(d)
        available_days = {d for d in available_days if d not in blocked}
    closed_admin = await db.get_closed_days_in_month(y, m)
    await _edit(
        callback.message,
        "<b>📅 Выберите новую дату</b>\n\n"
        "✅ — есть свободные слоты под вашу длительность.",
        reply_markup=month_calendar_kb(
            y,
            m,
            allowed_days=available_days,
            blocked_days=blocked | closed_admin,
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
    available_days = set(await db.get_available_days(days_ahead=60))
    data = await state.get_data()
    eng = bool(data.get("rs_engineer"))
    blocked: set[str] = set()
    if eng:
        from datetime import datetime as _dt

        for d in list(available_days):
            wd = _dt.fromisoformat(d).weekday()
            if wd in (5, 6):
                blocked.add(d)
        available_days = {d for d in available_days if d not in blocked}
    closed_admin = await db.get_closed_days_in_month(y, m)
    await _edit(
        callback.message,
        "<b>📅 Выберите новую дату</b>",
        reply_markup=month_calendar_kb(
            y,
            m,
            allowed_days=available_days,
            blocked_days=blocked | closed_admin,
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
    available_days = set(await db.get_available_days(days_ahead=60))
    blocked: set[str] = set()
    if eng:
        from datetime import datetime as _dt

        for d in list(available_days):
            wd = _dt.fromisoformat(d).weekday()
            if wd in (5, 6):
                blocked.add(d)
        available_days = {d for d in available_days if d not in blocked}
    closed_admin = await db.get_closed_days_in_month(y, m)
    await _edit(
        callback.message,
        "<b>📅 Выберите новую дату</b>",
        reply_markup=month_calendar_kb(
            y,
            m,
            allowed_days=available_days,
            blocked_days=blocked | closed_admin,
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
    text = _format_my_bookings_screen(rows, config, studio_address_html=addr)
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
    text = _format_my_bookings_screen(rows, config, studio_address_html=addr)
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

