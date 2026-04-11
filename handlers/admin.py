from __future__ import annotations

import re
from calendar import monthrange
from datetime import date
from html import escape as html_escape

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup

from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import Config
from database.db import Database
from handlers.user import delete_booking_pending_ui_messages, finalize_confirmed_payment
from keyboards import month_calendar_kb, now_month
from services.content_settings import setting_bool
from services.effective_pricing import EffectivePricing, load_effective_pricing
from services.reminders import ReminderService
from states import AdminStates


router = Router()

ADMIN_HOME_HTML = "<b>🛠 Админ-панель</b>"


def _is_admin(user_id: int, config: Config) -> bool:
    return user_id == config.admin_id


def _month_days(year: int, month: int) -> set[str]:
    days_count = monthrange(year, month)[1]
    return {date(year, month, d).isoformat() for d in range(1, days_count + 1)}


def _admin_abort_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅ Отмена", callback_data="admin:abort_input")
    kb.adjust(1)
    return kb.as_markup()


async def _admin_edit_panel(
    callback: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup | None = None
) -> None:
    try:
        await callback.message.edit_text(
            text, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )
    except TelegramBadRequest:
        await callback.message.answer(
            text, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )


async def _restore_admin_panel_message(
    bot,
    chat_id: int,
    message_id: int,
    *,
    text: str = ADMIN_HOME_HTML,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup or admin_menu_kb(),
        )
    except TelegramBadRequest:
        pass


def _admin_services_kb(pricing: EffectivePricing) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    ne = "✅ Без звукаря" if pricing.service_no_engineer_enabled else "⛔ Без звукаря"
    we = "✅ Со звукарём" if pricing.service_with_engineer_enabled else "⛔ Со звукарём"
    ly = "✅ Текст" if pricing.service_lyrics_enabled else "⛔ Текст"
    bt = "✅ Бит" if pricing.service_beat_enabled else "⛔ Бит"
    kb.button(text=ne, callback_data="admsvc:no_engineer")
    kb.button(text=we, callback_data="admsvc:with_engineer")
    kb.button(text=ly, callback_data="admsvc:lyrics")
    kb.button(text=bt, callback_data="admsvc:beat")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def _equipment_admin_text(settings: dict[str, str]) -> str:
    custom = setting_bool(settings, "equipment_use_custom", False)
    mode = (
        "Сейчас: <b>свой шаблон (HTML)</b> — базовый макет не используется."
        if custom
        else "Сейчас: <b>стандартный шаблон</b> — поля из настроек или .env."
    )
    return (
        "<b>📸 Оборудование и фото студии</b>\n\n"
        f"{mode}\n\n"
        "• <b>Свой HTML</b> — весь текст экрана.\n"
        "• <b>6 строк</b> — заголовок, описание, микрофон, аудиокарта, наушники, мониторы.\n"
        "• <b>Пути к фото</b> — абсолютные пути Windows/Linux, по одному в строке; пусто — из .env."
    )


def _equipment_menu_kb(settings: dict[str, str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    custom_on = setting_bool(settings, "equipment_use_custom", False)
    kb.button(
        text=("⛔ Выключить свой шаблон" if custom_on else "✅ Включить свой шаблон"),
        callback_data="admeq:toggle_custom",
    )
    kb.button(text="✏️ Свой HTML", callback_data="admeq:edit_custom")
    kb.button(text="✏️ Стандарт — 6 строк", callback_data="admeq:edit_std")
    kb.button(text="🖼 Пути к фото", callback_data="admeq:edit_photos")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def _postpay_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Заказ текста песни", callback_data="admpay:lyrics")
    kb.button(text="🎚 Заказ бита", callback_data="admpay:beat")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def _admin_prices_kb(pricing: EffectivePricing) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"🎤 Без звукаря: {pricing.price_no_engineer} ₽",
        callback_data="admkv:price_no_engineer",
    )
    kb.button(
        text=f"🎛️ Со звукарём: {pricing.price_with_engineer} ₽",
        callback_data="admkv:price_with_engineer",
    )
    kb.button(text=f"📝 Текст: {pricing.price_lyrics} ₽", callback_data="admkv:price_lyrics")
    kb.button(text=f"🎚️ Бит: {pricing.price_beat} ₽", callback_data="admkv:price_beat")
    for key, label in [
        ("tariff_night_6h", "🌙6"),
        ("tariff_night_8h", "🌙8"),
        ("tariff_night_10h", "🌙10"),
        ("tariff_night_12h", "🌙12"),
        ("tariff_day_6h", "☀️6"),
        ("tariff_day_8h", "☀️8"),
        ("tariff_day_10h", "☀️10"),
        ("tariff_day_12h", "☀️12"),
        ("tariff_night_6h_engineer", "🌙6+"),
        ("tariff_night_8h_engineer", "🌙8+"),
        ("tariff_night_10h_engineer", "🌙10+"),
        ("tariff_night_12h_engineer", "🌙12+"),
        ("tariff_day_6h_engineer", "☀️6+"),
        ("tariff_day_8h_engineer", "☀️8+"),
        ("tariff_day_10h_engineer", "☀️10+"),
        ("tariff_day_12h_engineer", "☀️12+"),
    ]:
        kb.button(text=label, callback_data=f"admkv:{key}")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def admin_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Добавить рабочий день", callback_data="admin:add_day")
    kb.button(text="🔓🔒 Открыть / закрыть день", callback_data="admin:openclose_day")
    kb.button(text="➕ Добавить слот", callback_data="admin:add_slot")
    kb.button(text="➖ Удалить слот", callback_data="admin:remove_slot")
    kb.button(text="🔁 Слот занят/свободен", callback_data="admin:toggle_slots")
    kb.button(text="❌ Отменить запись клиента", callback_data="admin:cancel_booking")
    kb.button(text="📋 Расписание на дату", callback_data="admin:schedule")
    kb.button(text="💰 Цены и тарифы", callback_data="admin:prices")
    kb.button(text="🛒 Услуги вкл/выкл", callback_data="admin:services")
    kb.button(text="📸 Оборудование и фото", callback_data="admin:equipment")
    kb.button(text="📣 Текст после оплаты (текст/бит)", callback_data="admin:postpay")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


@router.message(F.text == "/admin")
async def admin_entry(message: Message, config: Config, state: FSMContext) -> None:
    if not _is_admin(message.from_user.id, config):
        return
    await state.clear()
    await message.answer(ADMIN_HOME_HTML, parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())


def _admin_slots_markup(slots: list) -> object:
    kb = InlineKeyboardBuilder()
    for s in slots:
        st = Database.normalize_time_str(str(s["start_time"]))
        free = Database.slot_row_is_active(s["is_active"])
        mark = "🟢" if free else "🔴"
        kb.button(text=f"{mark} {st}", callback_data=f"admst:{int(s['id'])}")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(3)
    return kb.as_markup()


@router.callback_query(F.data == "admin:home")
async def admin_home(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await _admin_edit_panel(callback, ADMIN_HOME_HTML, admin_menu_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:"))
async def admin_actions(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return

    action = callback.data.split(":")[1]

    if action == "abort_input":
        await state.clear()
        await _admin_edit_panel(callback, ADMIN_HOME_HTML, admin_menu_kb())
        await callback.answer()
        return

    if action == "prices":
        await state.clear()
        await _admin_edit_panel(
            callback,
            "<b>💰 Цены</b>\nВыберите позицию, затем введите целое число (руб) в чат.",
            _admin_prices_kb(pricing),
        )
        await callback.answer()
        return

    if action == "services":
        await state.clear()
        await _admin_edit_panel(
            callback,
            "<b>🛒 Услуги</b>\n"
            "Переключите доступность: запись без/со звукорежиссёром, текст и бит.",
            _admin_services_kb(pricing),
        )
        await callback.answer()
        return

    if action == "equipment":
        await state.clear()
        s = await db.get_all_settings()
        await _admin_edit_panel(
            callback,
            _equipment_admin_text(s),
            _equipment_menu_kb(s),
        )
        await callback.answer()
        return

    if action == "postpay":
        await state.clear()
        await _admin_edit_panel(
            callback,
            "<b>📣 Текст после оплаты</b>\n\n"
            "Что видит клиент в сводке после подтверждения оплаты (заказ текста или бита). "
            "Можно HTML. Пустое значение — контакт из .env (TEXTMAKER_USERNAME / BEATMAKER_USERNAME).",
            _postpay_menu_kb(),
        )
        await callback.answer()
        return

    y, m = now_month()
    await state.set_state(AdminStates.action_date)
    await state.update_data(admin_action=action, cal_year=y, cal_month=m)

    allowed = _month_days(y, m)
    closed = await db.get_closed_days_in_month(y, m)
    await _admin_edit_panel(
        callback,
        "<b>Выберите дату</b>\n⛔ — день сейчас закрыт (нажмите, чтобы открыть).",
        month_calendar_kb(
            y,
            m,
            allowed,
            prefix="admin_date",
            nav_prefix="acal",
            mark_past_as_blocked=False,
            closed_days_highlight=closed,
            nav_back_callback="admin:home",
            nav_back_text="⬅ Админ-панель",
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("acal:"), AdminStates.action_date)
async def admin_calendar_nav(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    payload = callback.data.split(":", maxsplit=1)[1]
    if payload == "today":
        y, m = now_month()
    else:
        y_str, m_str = payload.split("-")
        y, m = int(y_str), int(m_str)
    await state.update_data(cal_year=y, cal_month=m)
    closed = await db.get_closed_days_in_month(y, m)
    try:
        await callback.message.edit_text(
            "<b>Выберите дату</b>\n⛔ — день сейчас закрыт (нажмите, чтобы открыть).",
            parse_mode=ParseMode.HTML,
            reply_markup=month_calendar_kb(
                y,
                m,
                _month_days(y, m),
                prefix="admin_date",
                nav_prefix="acal",
                mark_past_as_blocked=False,
                closed_days_highlight=closed,
                nav_back_callback="admin:home",
                nav_back_text="⬅ Админ-панель",
            ),
        )
    except TelegramBadRequest:
        await callback.message.edit_reply_markup(
            reply_markup=month_calendar_kb(
                y,
                m,
                _month_days(y, m),
                prefix="admin_date",
                nav_prefix="acal",
                mark_past_as_blocked=False,
                closed_days_highlight=closed,
                nav_back_callback="admin:home",
                nav_back_text="⬅ Админ-панель",
            )
        )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_date:"), AdminStates.action_date)
async def admin_date_selected(
    callback: CallbackQuery,
    state: FSMContext,
    config: Config,
    db: Database,
    reminder_service: ReminderService,
) -> None:
    picked_day = callback.data.split(":")[1]
    data = await state.get_data()
    action = data.get("admin_action")

    back_kb = InlineKeyboardBuilder()
    back_kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    back_kb.adjust(1)
    back_only = back_kb.as_markup()

    if action == "add_day":
        await db.add_work_day(picked_day)
        await state.clear()
        await _admin_edit_panel(callback, ADMIN_HOME_HTML, admin_menu_kb())
        await callback.answer(f"Рабочий день {picked_day} добавлен")
        return

    if action == "openclose_day":
        if await db.is_work_day_closed(picked_day):
            await db.open_work_day(picked_day)
            await state.clear()
            await _admin_edit_panel(callback, ADMIN_HOME_HTML, admin_menu_kb())
            await callback.answer("День открыт")
            return
        if await db.day_has_active_studio_bookings(picked_day):
            await callback.answer(
                "Нельзя закрыть: есть активные или ожидающие оплаты записи.",
                show_alert=True,
            )
            return
        await db.close_day(picked_day)
        await state.clear()
        await _admin_edit_panel(callback, ADMIN_HOME_HTML, admin_menu_kb())
        await callback.answer("День закрыт")
        return

    if action == "schedule":
        schedule = await db.get_day_schedule(picked_day)
        await state.clear()
        if not schedule:
            await _admin_edit_panel(callback, "На дату нет слотов.", back_only)
        else:
            lines = [f"<b>📋 Расписание на {picked_day}</b>"]
            for row in schedule:
                if row["is_active"]:
                    lines.append(f"🟢 {row['start_time']} - {row['end_time']} свободно")
                else:
                    lines.append(
                        f"🔴 ID:{row['booking_id']} {row['start_time']} - {row['end_time']} "
                        f"{row.get('user_name','')} {row.get('phone','')}"
                    )
            await _admin_edit_panel(callback, "\n".join(lines), back_only)
        await callback.answer()
        return

    if action in ("add_slot", "remove_slot"):
        await state.set_state(AdminStates.slot_time_input)
        await state.update_data(
            day=picked_day,
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        if action == "add_slot":
            prompt = (
                "Отправьте время в формате HH:MM-HH:MM, например 10:00-11:00 (один час)"
            )
        else:
            prompt = "Отправьте начало слота в формате HH:MM, например 10:00"
        try:
            await callback.message.edit_text(prompt, reply_markup=_admin_abort_kb())
        except TelegramBadRequest:
            await callback.message.answer(prompt, reply_markup=_admin_abort_kb())
        await callback.answer()
        return

    if action == "cancel_booking":
        await state.set_state(AdminStates.cancel_booking_input)
        schedule = await db.get_day_schedule(picked_day)
        lines = [f"<b>Активные брони на {picked_day}</b>"]
        for row in schedule:
            if not row["is_active"] and row.get("booking_id"):
                lines.append(
                    f"ID:{row['booking_id']} | {row['start_time']} - {row['end_time']} | "
                    f"{row.get('user_name','')} {row.get('phone','')}"
                )
        lines.append("Отправьте ID брони для отмены.")
        await state.update_data(
            day=picked_day,
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        try:
            await callback.message.edit_text(
                "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=_admin_abort_kb()
            )
        except TelegramBadRequest:
            await callback.message.answer(
                "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=_admin_abort_kb()
            )
        await callback.answer()
        return

    if action == "toggle_slots":
        slots = await db.get_all_slots_for_day(picked_day)
        if not slots:
            await state.clear()
            await _admin_edit_panel(callback, "На эту дату нет слотов.", back_only)
            await callback.answer()
            return
        await state.set_state(AdminStates.toggle_slots_pick)
        await state.update_data(admin_toggle_day=picked_day)
        await _admin_edit_panel(
            callback,
            f"<b>Слоты {picked_day}</b>\n🟢 — доступен · 🔴 — занят\n"
            "Переключение без брони клиента: свободный→админ.занят, "
            "админ.занят→свободный. Если слот в брони — сначала отмените запись.",
            _admin_slots_markup(slots),
        )
        await callback.answer()
        return

    await callback.answer()


@router.message(AdminStates.slot_time_input)
async def admin_slot_input(message: Message, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    day = data["day"]
    action = data["admin_action"]
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")
    payload = (message.text or "").strip()
    try:
        if action == "add_slot":
            m = re.match(r"^\s*(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*$", payload)
            if not m:
                raise ValueError("bad format")
            start_time, end_time = m.group(1), m.group(2)
            await db.add_slot(day, start_time, end_time)
        else:
            await db.remove_slot(day, payload)
        await state.clear()
        if mid is not None and cid is not None:
            await _restore_admin_panel_message(message.bot, int(cid), int(mid))
        try:
            await message.delete()
        except Exception:
            pass
    except Exception:
        await message.answer("Неверный формат. Попробуйте ещё раз.")


@router.message(AdminStates.cancel_booking_input)
async def admin_cancel_booking(
    message: Message,
    state: FSMContext,
    db: Database,
    reminder_service: ReminderService,
    config: Config,
) -> None:
    try:
        booking_id = int(message.text.strip())
    except ValueError:
        await message.answer("Введите числовой ID брони.")
        return

    data = await state.get_data()
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")

    booking = await db.cancel_booking(booking_id)
    if not booking:
        await message.answer("Активная запись с таким ID не найдена.")
        return
    await reminder_service.remove_for_booking(booking_id)
    try:
        await message.bot.send_message(booking["user_id"], f"Ваша запись #{booking_id} была отменена администратором.")
    except Exception:
        pass
    await state.clear()
    if mid is not None and cid is not None:
        await _restore_admin_panel_message(message.bot, int(cid), int(mid))
    try:
        await message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("admkv:"))
async def admin_price_key(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    key = callback.data.split(":", maxsplit=1)[1]
    await state.set_state(AdminStates.price_wait_value)
    await state.update_data(
        admin_price_key=key,
        admin_panel_mid=callback.message.message_id,
        admin_panel_cid=callback.message.chat.id,
    )
    try:
        await callback.message.edit_text(
            f"Введите цену (целое число, руб) для ключа <code>{html_escape(key)}</code>:",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
    except TelegramBadRequest:
        await callback.message.answer(
            f"Введите цену (целое число, руб) для ключа <code>{html_escape(key)}</code>:",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
    await callback.answer()


@router.message(AdminStates.price_wait_value)
async def admin_price_value(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    key = data.get("admin_price_key")
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")
    if not key:
        await state.clear()
        return
    try:
        v = int((message.text or "").strip())
        if v < 0 or v > 10_000_000:
            raise ValueError
    except ValueError:
        await message.answer("Нужно целое число от 0 до 10000000.")
        return
    await db.set_setting(str(key), str(v))
    pricing2 = await load_effective_pricing(db, config)
    await state.clear()
    if mid is not None and cid is not None:
        try:
            await message.bot.edit_message_text(
                chat_id=int(cid),
                message_id=int(mid),
                text="<b>💰 Цены</b>\nВыберите позицию, затем введите целое число (руб) в чат.",
                parse_mode=ParseMode.HTML,
                reply_markup=_admin_prices_kb(pricing2),
            )
        except TelegramBadRequest:
            pass
    try:
        await message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("admsvc:"))
async def admin_service_flip(
    callback: CallbackQuery, config: Config, db: Database, pricing: EffectivePricing
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    kind = callback.data.split(":")[1]
    settings = await db.get_all_settings()
    if kind == "lyrics":
        cur = settings.get("service_lyrics_enabled", "1") == "1"
        await db.set_setting("service_lyrics_enabled", "0" if cur else "1")
    elif kind == "beat":
        cur = settings.get("service_beat_enabled", "1") == "1"
        await db.set_setting("service_beat_enabled", "0" if cur else "1")
    elif kind == "no_engineer":
        cur = settings.get("service_no_engineer_enabled", "1") == "1"
        await db.set_setting("service_no_engineer_enabled", "0" if cur else "1")
    elif kind == "with_engineer":
        cur = settings.get("service_with_engineer_enabled", "1") == "1"
        await db.set_setting("service_with_engineer_enabled", "0" if cur else "1")
    else:
        await callback.answer()
        return
    pricing2 = await load_effective_pricing(db, config)
    try:
        await callback.message.edit_reply_markup(reply_markup=_admin_services_kb(pricing2))
    except Exception:
        pass
    await callback.answer("Переключено")


@router.callback_query(F.data.startswith("admeq:"))
async def admin_equipment_sub(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    sub = callback.data.split(":")[1]
    if sub == "toggle_custom":
        s = await db.get_all_settings()
        cur = setting_bool(s, "equipment_use_custom", False)
        await db.set_setting("equipment_use_custom", "0" if cur else "1")
        s2 = await db.get_all_settings()
        await _admin_edit_panel(
            callback, _equipment_admin_text(s2), _equipment_menu_kb(s2)
        )
        await callback.answer("Готово")
        return
    if sub == "edit_custom":
        await state.set_state(AdminStates.wait_setting_text)
        await state.update_data(
            setting_key="equipment_custom_html",
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            "<b>Свой HTML</b>\n\nОтправьте одним сообщением весь текст раздела (поддерживается HTML). "
            "Один символ <code>-</code> — очистить свой шаблон.",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return
    if sub == "edit_std":
        await state.set_state(AdminStates.wait_setting_text)
        await state.update_data(
            setting_key="__equipment_std__",
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            "<b>Стандартный шаблон</b>\n\n"
            "Ровно <b>6 строк</b>:\n"
            "1) Заголовок (без эмодзи 📸)\n"
            "2) Описание\n"
            "3) Микрофон\n"
            "4) Аудиокарта\n"
            "5) Наушники\n"
            "6) Мониторы",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return
    if sub == "edit_photos":
        await state.set_state(AdminStates.wait_setting_text)
        await state.update_data(
            setting_key="equipment_photos_raw",
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            "<b>Пути к фото</b>\n\n"
            "По одному абсолютному пути на строку. Файлы должны быть на диске у бота.\n"
            "Один символ <code>-</code> — сбросить и брать список из .env.",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return
    await callback.answer()


@router.callback_query(F.data.startswith("admpay:"))
async def admin_postpay_sub(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    kind = callback.data.split(":")[1]
    key = "postpay_lyrics_html" if kind == "lyrics" else "postpay_beat_html"
    label = "текста песни" if kind == "lyrics" else "бита"
    await state.set_state(AdminStates.wait_setting_text)
    await state.update_data(
        setting_key=key,
        admin_panel_mid=callback.message.message_id,
        admin_panel_cid=callback.message.chat.id,
    )
    await callback.message.edit_text(
        f"<b>Текст после оплаты</b> ({label})\n\n"
        "Что увидит клиент в сводке. Можно HTML. "
        "Один символ <code>-</code> — сбросить и использовать .env.",
        parse_mode=ParseMode.HTML,
        reply_markup=_admin_abort_kb(),
    )
    await callback.answer()


@router.message(AdminStates.wait_setting_text)
async def admin_wait_setting_text(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not _is_admin(message.from_user.id, config):
        await state.clear()
        return
    data = await state.get_data()
    key = data.get("setting_key")
    if not key:
        await state.clear()
        return
    raw = (message.text or "").strip()
    if key == "__equipment_std__":
        if raw == "-":
            await message.answer("Нужно 6 строк или отмена.")
            return
        lines = [l.strip() for l in raw.splitlines() if l.strip()]
        if len(lines) < 6:
            await message.answer(
                "Нужно минимум 6 непустых строк: заголовок, описание, микрофон, карта, наушники, мониторы."
            )
            return
        field_keys = [
            "equipment_title",
            "equipment_body",
            "equipment_mic",
            "equipment_audiocard",
            "equipment_headphones",
            "equipment_monitors",
        ]
        for fk, val in zip(field_keys, lines[:6]):
            await db.set_setting(fk, val)
    else:
        val = "" if raw == "-" else raw
        await db.set_setting(key, val)

    await state.clear()
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")
    if mid is not None and cid is not None:
        await _restore_admin_panel_message(message.bot, int(cid), int(mid))
    try:
        await message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("admst:"), AdminStates.toggle_slots_pick)
async def admin_hit_slot(callback: CallbackQuery, state: FSMContext, config: Config, db: Database) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        sid = int(callback.data.split(":")[1])
    except ValueError:
        await callback.answer()
        return
    _ok, msg = await db.admin_toggle_slot_availability(sid)
    await callback.answer(msg, show_alert=True)
    data = await state.get_data()
    day = data.get("admin_toggle_day")
    if not day:
        return
    slots = await db.get_all_slots_for_day(day)
    try:
        await callback.message.edit_reply_markup(reply_markup=_admin_slots_markup(slots))
    except Exception:
        pass


@router.callback_query(F.data.startswith("pay:ok:"))
async def payment_confirm(
    callback: CallbackQuery,
    db: Database,
    config: Config,
    reminder_service: ReminderService,
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        bid = int(callback.data.split(":")[2])
    except ValueError:
        await callback.answer()
        return
    ok, _ = await finalize_confirmed_payment(callback.bot, db, config, reminder_service, bid)
    if not ok:
        await callback.answer("Уже обработано или не найдено", show_alert=True)
        return
    try:
        t = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            t + "\n\n<b>✅ Оплата подтверждена</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await callback.answer("Подтверждено")


@router.callback_query(F.data.startswith("pay:no:"))
async def payment_reject(
    callback: CallbackQuery,
    db: Database,
    config: Config,
    reminder_service: ReminderService,
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        bid = int(callback.data.split(":")[2])
    except ValueError:
        await callback.answer()
        return
    b = await db.get_booking_by_id(bid)
    if not b or b.get("status") != "pending_payment":
        await callback.answer("Уже обработано", show_alert=True)
        return
    await delete_booking_pending_ui_messages(callback.bot, dict(b))
    row = await db.cancel_booking(bid)
    if not row:
        await callback.answer("Ошибка отмены", show_alert=True)
        return
    kind = row.get("booking_kind") or "studio"
    if kind in (None, "studio"):
        await reminder_service.remove_for_booking(bid)
    try:
        await callback.bot.send_message(
            int(row["user_id"]),
            "<b>Оплата не подтверждена</b>\n\n"
            "Если вы уже перевели средства, напишите администратору. "
            "Можно оформить новую заявку через меню.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    try:
        t = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            t + "\n\n<b>❌ Оплата отклонена</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await callback.answer("Отклонено")


