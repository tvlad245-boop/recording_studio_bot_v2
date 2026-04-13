from __future__ import annotations

import logging
import re
from calendar import monthrange
from datetime import date
from html import escape as html_escape
from pathlib import Path
from uuid import uuid4

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup

from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import Config
from database.db import Database
from handlers.user import (
    delete_booking_pending_ui_messages,
    delete_pending_ui_and_send_main_menu,
    finalize_confirmed_payment,
    remove_booking_from_user_activity,
    remove_service_order_from_user_activity,
    _publish_weekly_and_tasks,
)
from keyboards import month_calendar_kb, now_month
from services.content_settings import (
    append_manager_contact_html,
    cancel_confirmed_custom_html,
    cancel_refund_warning_html,
    effective_maker_username,
    equipment_photo_paths,
    setting_bool,
)
from services.channel_settings import (
    effective_payments_inbox_chat_id,
    effective_schedule_channel_id,
    effective_subscription_channel_id,
    effective_subscription_channel_link,
)
from services.content_settings import tariff_day_start_list, tariff_night_start_list
from services.effective_pricing import EffectivePricing, build_default_settings_dict, load_effective_pricing
from services.reminders import ReminderService
from states import AdminStates


router = Router()
logger = logging.getLogger(__name__)

ADMIN_HOME_HTML = "<b>🛠 Админ-панель</b>"

# Локальные файлы, присланные админом из Telegram (абсолютные пути попадают в equipment_photos_raw)
EQUIPMENT_UPLOAD_ROOT = Path(__file__).resolve().parent.parent / "user_uploads" / "equipment"
UI_UPLOAD_ROOT = Path(__file__).resolve().parent.parent / "user_uploads" / "ui"

# callback_id -> (setting_key, короткая подпись для экрана)
_UI_PHOTO_SLOTS: dict[str, tuple[str, str]] = {
    "main_menu": ("ui_photo_main_menu_path", "Главное меню"),
    "prices": ("ui_photo_prices_path", "Прайс"),
    "payment": ("ui_photo_payment_path", "Оплата / реквизиты"),
    "tariff_cat": ("ui_photo_tariff_category_path", "Тарифы: ночь/день"),
    "tariff_night": ("ui_photo_tariff_night_path", "Тариф: ночь"),
    "tariff_day": ("ui_photo_tariff_day_path", "Тариф: день"),
}

# Подсказка при загрузке фото в раздел «Оборудование»
_EQUIPMENT_PHOTO_SIZE_HINT = (
    "\n\n<i>Старайтесь выбирать изображения небольшого веса: в идеале до ~100 КБ — "
    "так быстрее откроется у клиентов в Telegram.</i>"
)


def _try_delete_uploaded_file(path_str: str) -> None:
    try:
        p = Path(path_str).resolve()
        p.relative_to(EQUIPMENT_UPLOAD_ROOT.resolve())
        if p.is_file():
            p.unlink()
    except (ValueError, OSError):
        pass


def _try_delete_ui_upload(path_str: str) -> None:
    try:
        p = Path(path_str).resolve()
        p.relative_to(UI_UPLOAD_ROOT.resolve())
        if p.is_file():
            p.unlink()
    except (ValueError, OSError):
        pass


def _is_admin(user_id: int, config: Config) -> bool:
    return user_id == config.admin_id


async def _user_can_mark_service_done(
    callback: CallbackQuery, db: Database, config: Config, *, kind: str
) -> bool:
    """Админ или username совпадает с контактом текстовика/битмейкера из настроек/.env."""
    if not callback.from_user:
        return False
    if _is_admin(callback.from_user.id, config):
        return True
    maker_line = (await effective_maker_username(db, config, kind=kind)).strip()
    u = (callback.from_user.username or "").strip().lstrip("@").lower()
    if not u:
        return False
    for token in re.split(r"[\s,;|]+", maker_line):
        t = token.strip().lstrip("@").lower()
        if t and t == u:
            return True
    return False


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
        "• <b>Загрузить фото</b> — прислать картинку в чат; лучше лёгкие файлы (в идеале до ~100 КБ).\n"
        "• <b>Пути к фото</b> — вручную, по одному в строке; пусто — из .env."
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
    kb.button(text="📷 Добавить фото (в конец)", callback_data="admeq:upload_append")
    kb.button(text="🔄 Заменить фото…", callback_data="admeq:replace_menu")
    kb.button(text="🖼 Пути к фото (текстом)", callback_data="admeq:edit_photos")
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


def _studio_nav_admin_text(settings: dict[str, str]) -> str:
    addr = (settings.get("studio_address_html") or "").strip()
    vid = (settings.get("studio_directions_video_file_id") or "").strip()
    addr_prev = addr if addr else "<i>не задан</i>"
    vid_prev = "✅ загружено" if vid else "<i>нет</i>"
    return (
        "<b>📍 Адрес и видео «как пройти»</b>\n\n"
        f"<b>Адрес:</b> {addr_prev}\n"
        f"<b>Видео:</b> {vid_prev}\n\n"
        "Адрес показывается клиенту после оплаты и в «Моя запись». "
        "Видео отправляется после подтверждения оплаты и по кнопке «Как пройти до студии»."
    )


def _studio_nav_menu_kb(settings: dict[str, str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Адрес (HTML)", callback_data="admsn:edit_address")
    kb.button(text="🎬 Загрузить / заменить видео", callback_data="admsn:upload_video")
    if (settings.get("studio_directions_video_file_id") or "").strip():
        kb.button(text="🗑 Удалить видео", callback_data="admsn:clear_video")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def _ui_photos_admin_text(settings: dict[str, str]) -> str:
    lines: list[str] = [
        "<b>🖼 Картинки интерфейса</b>",
        "",
        "Файлы из этого раздела подставляются вместо путей из <code>.env</code>. "
        "После «Сброса» снова используется .env.",
        "",
    ]
    for _slot_id, (sk, label) in _UI_PHOTO_SLOTS.items():
        custom = bool((settings.get(sk) or "").strip())
        src = "свой файл" if custom else "как в .env"
        lines.append(f"• <b>{html_escape(label)}</b>: <i>{src}</i>")
    lines.extend(["", "<i>Лучше небольшие файлы (в идеале до ~100 КБ).</i>"])
    return "\n".join(lines)


def _ui_photos_menu_kb(settings: dict[str, str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for slot_id, (sk, label) in _UI_PHOTO_SLOTS.items():
        kb.button(text=f"📷 {label}", callback_data=f"admui:{slot_id}")
        if (settings.get(sk) or "").strip():
            kb.button(text=f"🗑 {label}", callback_data=f"admui:clear:{slot_id}")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def _channels_admin_text(settings: dict[str, str], config: Config) -> str:
    sub_id = effective_subscription_channel_id(settings, config)
    sub_link = effective_subscription_channel_link(settings, config)
    sch = effective_schedule_channel_id(settings, config)
    pay = effective_payments_inbox_chat_id(settings, config)
    link_short = sub_link if len(sub_link) <= 72 else sub_link[:69] + "…"
    return (
        "<b>📡 Каналы и чаты</b>\n\n"
        "<b>Сейчас (настройки бота поверх .env):</b>\n"
        f"• Подписка — ID канала: <code>{sub_id}</code>\n"
        f"• Ссылка «Подписаться»: {html_escape(link_short)}\n"
        f"• Расписание и задачи — ID канала: <code>{sch}</code>\n"
        f"• Подтверждения оплаты — ID чата: <code>{pay}</code>\n\n"
        "Выберите пункт и пришлите значение одним сообщением.\n"
        "Для ID: целое число (часто отрицательное). "
        "Символ <code>-</code> сбрасывает свой ID и снова берёт значение из .env.\n"
        "Для ссылки: полный URL или <code>-</code> для сброса."
    )


def _channels_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 ID канала подписки", callback_data="adch:sub_id")
    kb.button(text="🔗 Ссылка «Подписаться»", callback_data="adch:sub_link")
    kb.button(text="📅 Канал расписания", callback_data="adch:schedule")
    kb.button(
        text="🔄 Обновить посты расписания",
        callback_data="adch:republish_schedule",
    )
    kb.button(text="💳 Чат подтверждений оплаты", callback_data="adch:payments")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


_CHANNEL_EDIT_PROMPTS: dict[str, tuple[str, str, str]] = {
    "sub_id": (
        "subscription_channel_id",
        "ID канала для проверки подписки",
        "Целое число (например <code>-1001234567890</code>). "
        "<code>0</code> — не требовать подписку. <code>-</code> — как в .env.",
    ),
    "sub_link": (
        "subscription_channel_link",
        "Ссылка для кнопки «Подписаться»",
        "Например <code>https://t.me/your_channel</code>. <code>-</code> — взять из .env.",
    ),
    "schedule": (
        "schedule_channel_id",
        "Канал для расписания и задач",
        "Сюда публикуются расписание на 7 дней и задачи текстовика/битмейкера. "
        "<code>0</code> — не слать. <code>-</code> — как в .env.",
    ),
    "payments": (
        "payments_inbox_chat_id",
        "Чат для заявок на подтверждение оплаты",
        "Группа или канал с заявками. <code>0</code> — в личку администратору (как в .env при пустом PAYMENTS). "
        "<code>-</code> — сброс к .env.",
    ),
}

_CHANNEL_SETTING_KEYS = frozenset(v[0] for v in _CHANNEL_EDIT_PROMPTS.values())

_TARIFF_TIME_SETTING_KEYS = frozenset({"tariff_day_start_times", "tariff_night_start_times"})

_CLIENT_TEXT_SETTING_KEYS = frozenset(
    {
        "cancel_refund_warning_html",
        "cancel_request_sent_html",
        "cancel_confirmed_studio_html",
        "cancel_confirmed_service_html",
    }
)

_ADTX_PROMPTS: dict[str, tuple[str, str, str]] = {
    "refund": (
        "cancel_refund_warning_html",
        "Предупреждение о возврате",
        "Текст показывается при отмене записи, если время аренды уже началось, и в сообщении клиенту после подтверждённой отмены в этом случае. "
        "Один символ <code>-</code> — взять текст по умолчанию из настроек бота.",
    ),
    "cancel_wait": (
        "cancel_request_sent_html",
        "Ожидание отмены (клиенту)",
        "Показывается в главном окне после того, как клиент запросил отмену и ждёт решения оператора. "
        "Пусто или <code>-</code> — стандартный текст. Контакт менеджера задаётся в разделе «Контакты».",
    ),
    "cancel_done_studio": (
        "cancel_confirmed_studio_html",
        "Отмена записи на студию (клиенту)",
        "Текст после подтверждённой отмены записи на студию (до предупреждения о возврате и блока менеджера). "
        "Пусто — как раньше: «Запись отменена», слот снова доступен.",
    ),
    "cancel_done_svc": (
        "cancel_confirmed_service_html",
        "Отмена заказа текст/бит (клиенту)",
        "Текст после подтверждённой отмены заявки на текст или бит. Пусто — «Заявка отменена».",
    ),
}

_CONTACTS_SETTING_KEYS = frozenset({"textmaker_username", "beatmaker_username", "manager_contact_html"})

_CONTACT_PROMPTS: dict[str, tuple[str, str, str]] = {
    "textmaker": (
        "textmaker_username",
        "Контакт текстовика",
        "Одна строка: @username или текст. Пустое значение в боте — брать из .env (TEXTMAKER_USERNAME). "
        "Символ <code>-</code> — сбросить и снова использовать .env.",
    ),
    "beatmaker": (
        "beatmaker_username",
        "Контакт битмейкера",
        "Одна строка: @username или текст. Пустое — из .env (BEATMAKER_USERNAME). "
        "<code>-</code> — сброс к .env.",
    ),
    "manager": (
        "manager_contact_html",
        "Контакт менеджера",
        "HTML-блок (ссылка, телефон, @username, «напишите в Telegram»): показывается клиенту при ожидании оплаты, "
        "запросе отмены и в итоговых сообщениях (без текста «ответьте на это сообщение»). "
        "<code>-</code> — очистить.",
    ),
}


def _client_texts_admin_text() -> str:
    return (
        "<b>✉️ Тексты для клиентов</b>\n\n"
        "• <b>Предупреждение о возврате</b> — если время аренды уже началось.\n"
        "• <b>Ожидание отмены</b> — пока клиент ждёт решения по отмене (контакт менеджера — в «Контактах»).\n"
        "• <b>Отмена: студия / текст·бит</b> — текст после подтверждённой отмены оператором."
    )


def _client_texts_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⚠️ Предупреждение о возврате", callback_data="adtx:refund")
    kb.button(text="⏳ Ожидание отмены", callback_data="adtx:cancel_wait")
    kb.button(text="✅ Отмена: студия", callback_data="adtx:cancel_done_studio")
    kb.button(text="✅ Отмена: текст/бит", callback_data="adtx:cancel_done_svc")
    kb.button(text="⬅ Админ-панель", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def _contacts_admin_text() -> str:
    return (
        "<b>📇 Контакты</b>\n\n"
        "Контакты текстовика и битмейкера показываются в задачах в канале, в «Моих заявках» и в сводке после оплаты. "
        "Менеджер — HTML в сообщениях клиенту.\n"
        "Пустое поле в боте = значение из .env."
    )


def _contacts_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Текстовик", callback_data="adct:textmaker")
    kb.button(text="🎚 Битмейкер", callback_data="adct:beatmaker")
    kb.button(text="👤 Менеджер (HTML)", callback_data="adct:manager")
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
    kb.button(text="☀️ Старты дневного тарифа (времена)", callback_data="admkv:tariff_day_start_times")
    kb.button(text="🌙 Старты ночного тарифа (времена)", callback_data="admkv:tariff_night_start_times")
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
    kb.button(text="📍 Адрес и «как пройти»", callback_data="admin:studio_nav")
    kb.button(text="🖼 Картинки интерфейса", callback_data="admin:ui_photos")
    kb.button(text="📡 Каналы и чаты", callback_data="admin:channels")
    kb.button(text="✉️ Тексты для клиентов", callback_data="admin:client_texts")
    kb.button(text="📇 Контакты", callback_data="admin:contacts")
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
            "<b>💰 Цены</b>\nВыберите позицию, затем введите целое число (руб) в чат.\n"
            "<i>Стартовые времена дневного и ночного тарифа — отдельными кнопками (формат ЧЧ:ММ).</i>",
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

    if action == "studio_nav":
        await state.clear()
        s = await db.get_all_settings()
        await _admin_edit_panel(
            callback,
            _studio_nav_admin_text(s),
            _studio_nav_menu_kb(s),
        )
        await callback.answer()
        return

    if action == "ui_photos":
        await state.clear()
        s = await db.get_all_settings()
        await _admin_edit_panel(
            callback,
            _ui_photos_admin_text(s),
            _ui_photos_menu_kb(s),
        )
        await callback.answer()
        return

    if action == "channels":
        await state.clear()
        s = await db.get_all_settings()
        await _admin_edit_panel(
            callback,
            _channels_admin_text(s, config),
            _channels_menu_kb(),
        )
        await callback.answer()
        return

    if action == "client_texts":
        await state.clear()
        s = await db.get_all_settings()
        await _admin_edit_panel(
            callback,
            _client_texts_admin_text(),
            _client_texts_menu_kb(),
        )
        await callback.answer()
        return

    if action == "contacts":
        await state.clear()
        await _admin_edit_panel(
            callback,
            _contacts_admin_text(),
            _contacts_menu_kb(),
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
    kind = booking.get("booking_kind") or "studio"
    if kind in (None, "studio"):
        await reminder_service.remove_for_booking(booking_id)
    try:
        await _publish_weekly_and_tasks(message.bot, db, config)
    except Exception:
        pass
    try:
        await remove_booking_from_user_activity(
            message.bot,
            db,
            config,
            user_id=int(booking["user_id"]),
            booking_id=booking_id,
        )
    except Exception:
        pass
    try:
        lbl = "заявка" if kind in ("lyrics", "beat") else "запись"
        await message.bot.send_message(
            booking["user_id"],
            f"Ваша {lbl} #{booking_id} была отменена администратором.",
        )
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
    if key in _TARIFF_TIME_SETTING_KEYS:
        hints = {
            "tariff_day_start_times": (
                "<b>Старты дневного тарифа</b>\n\n"
                "По одному времени в строке, формат <code>ЧЧ:ММ</code> (например <code>09:00</code>). "
                "Можно через запятую или с новой строки. Минимум одно время.\n"
                "Символ <code>-</code> — сброс к значениям по умолчанию (09:00 и 12:00)."
            ),
            "tariff_night_start_times": (
                "<b>Старты ночного тарифа</b>\n\n"
                "По одному времени в строке, формат <code>ЧЧ:ММ</code> (часто <code>00:00</code>). "
                "Несколько вариантов — с новой строки или через запятую.\n"
                "Символ <code>-</code> — сброс к <code>00:00</code>."
            ),
        }
        await state.set_state(AdminStates.wait_setting_text)
        await state.update_data(
            setting_key=key,
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        try:
            await callback.message.edit_text(
                hints[key],
                parse_mode=ParseMode.HTML,
                reply_markup=_admin_abort_kb(),
            )
        except TelegramBadRequest:
            await callback.message.answer(
                hints[key],
                parse_mode=ParseMode.HTML,
                reply_markup=_admin_abort_kb(),
            )
        await callback.answer()
        return
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
    raw_cd = callback.data
    if raw_cd.startswith("admeq:slot:"):
        idx = int(raw_cd.split(":")[2])
        await state.set_state(AdminStates.equipment_photo_wait)
        await state.update_data(
            equipment_photo_slot=idx,
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            f"<b>Замена фото №{idx + 1}</b>\n\n"
            "Отправьте <b>новое изображение</b> одним сообщением (как фото, не как документ)."
            f"{_EQUIPMENT_PHOTO_SIZE_HINT}",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return

    sub = raw_cd.split(":")[1]
    if sub == "upload_append":
        await state.set_state(AdminStates.equipment_photo_wait)
        await state.update_data(
            equipment_photo_slot=None,
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            "<b>Новое фото</b>\n\n"
            "Оно будет <b>добавлено в конец</b> карусели. Пришлите изображение одним сообщением."
            f"{_EQUIPMENT_PHOTO_SIZE_HINT}",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return
    if sub == "replace_menu":
        s = await db.get_all_settings()
        paths = equipment_photo_paths(s, config)
        if not paths:
            await callback.answer(
                "Пока нет фото в списке. Сначала «Добавить фото» или задайте пути в .env.",
                show_alert=True,
            )
            return
        kb = InlineKeyboardBuilder()
        for i, p in enumerate(paths):
            name = Path(p).name
            if len(name) > 36:
                name = name[:33] + "…"
            kb.button(text=f"№{i + 1} · {name}", callback_data=f"admeq:slot:{i}")
        kb.button(text="⬅ Назад", callback_data="admin:equipment")
        kb.adjust(1)
        await callback.message.edit_text(
            "<b>Какой снимок заменить?</b>\n\n"
            "Выберите номер, затем отправьте новое фото."
            f"{_EQUIPMENT_PHOTO_SIZE_HINT}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb.as_markup(),
        )
        await callback.answer()
        return
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


@router.callback_query(F.data.startswith("admsn:"))
async def admin_studio_nav_sub(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    sub = callback.data.split(":")[1]
    if sub == "edit_address":
        await state.set_state(AdminStates.wait_setting_text)
        await state.update_data(
            setting_key="studio_address_html",
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            "<b>Адрес студии</b>\n\n"
            "Одним сообщением (HTML). Показывается в сводке после оплаты и в «Моя запись». "
            "Один символ <code>-</code> — очистить.",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return
    if sub == "upload_video":
        await state.set_state(AdminStates.directions_video_wait)
        await state.update_data(
            admin_panel_mid=callback.message.message_id,
            admin_panel_cid=callback.message.chat.id,
        )
        await callback.message.edit_text(
            "<b>Видео «как пройти до студии»</b>\n\n"
            "Пришлите <b>видео</b> одним сообщением (как видео).",
            parse_mode=ParseMode.HTML,
            reply_markup=_admin_abort_kb(),
        )
        await callback.answer()
        return
    if sub == "clear_video":
        await db.set_setting("studio_directions_video_file_id", "")
        s = await db.get_all_settings()
        await _admin_edit_panel(
            callback, _studio_nav_admin_text(s), _studio_nav_menu_kb(s)
        )
        await callback.answer("Видео удалено")
        return
    await callback.answer()


@router.callback_query(F.data.startswith("admui:"))
async def admin_ui_photo_actions(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) >= 3 and parts[1] == "clear":
        slot_id = parts[2]
        meta = _UI_PHOTO_SLOTS.get(slot_id)
        if not meta:
            await callback.answer()
            return
        sk, _lbl = meta
        s = await db.get_all_settings()
        old = (s.get(sk) or "").strip()
        if old:
            _try_delete_ui_upload(old)
        await db.set_setting(sk, "")
        s2 = await db.get_all_settings()
        await _admin_edit_panel(
            callback, _ui_photos_admin_text(s2), _ui_photos_menu_kb(s2)
        )
        await callback.answer("Сброшено")
        return
    if len(parts) < 2:
        await callback.answer()
        return
    slot_id = parts[1]
    meta = _UI_PHOTO_SLOTS.get(slot_id)
    if not meta:
        await callback.answer()
        return
    sk, label = meta
    await state.set_state(AdminStates.ui_photo_wait)
    await state.update_data(
        ui_photo_setting_key=sk,
        admin_panel_mid=callback.message.message_id,
        admin_panel_cid=callback.message.chat.id,
    )
    await callback.message.edit_text(
        f"<b>Картинка: {html_escape(label)}</b>\n\n"
        "Пришлите изображение <b>как фото</b>. Оно будет показано клиентам на соответствующем экране.",
        parse_mode=ParseMode.HTML,
        reply_markup=_admin_abort_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "adch:republish_schedule")
async def admin_channels_republish_schedule(
    callback: CallbackQuery, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        await db.clear_schedule_channel_bot_messages()
        await _publish_weekly_and_tasks(callback.bot, db, config)
    except Exception:
        logger.exception("republish schedule channel failed")
        await callback.answer("Ошибка публикации — см. логи бота", show_alert=True)
        return
    await callback.answer("Посты расписания в канале обновлены")


@router.callback_query(F.data.startswith("adch:"))
async def admin_channels_sub(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    sub = callback.data.split(":", maxsplit=1)[1]
    meta = _CHANNEL_EDIT_PROMPTS.get(sub)
    if not meta:
        await callback.answer()
        return
    sk, title, hint = meta
    await state.set_state(AdminStates.wait_setting_text)
    await state.update_data(
        setting_key=sk,
        admin_panel_mid=callback.message.message_id,
        admin_panel_cid=callback.message.chat.id,
    )
    await callback.message.edit_text(
        f"<b>{html_escape(title)}</b>\n\n{hint}",
        parse_mode=ParseMode.HTML,
        reply_markup=_admin_abort_kb(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adct:"))
async def admin_contacts_sub(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    sub = callback.data.split(":", maxsplit=1)[1]
    meta = _CONTACT_PROMPTS.get(sub)
    if not meta:
        await callback.answer()
        return
    sk, title, hint = meta
    await state.set_state(AdminStates.wait_setting_text)
    await state.update_data(
        setting_key=sk,
        admin_panel_mid=callback.message.message_id,
        admin_panel_cid=callback.message.chat.id,
    )
    await callback.message.edit_text(
        f"<b>{html_escape(title)}</b>\n\n{hint}",
        parse_mode=ParseMode.HTML,
        reply_markup=_admin_abort_kb(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adtx:"))
async def admin_client_texts_sub(
    callback: CallbackQuery, state: FSMContext, config: Config, db: Database
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    sub = callback.data.split(":", maxsplit=1)[1]
    meta = _ADTX_PROMPTS.get(sub)
    if not meta:
        await callback.answer()
        return
    sk, title, hint = meta
    await state.set_state(AdminStates.wait_setting_text)
    await state.update_data(
        setting_key=sk,
        admin_panel_mid=callback.message.message_id,
        admin_panel_cid=callback.message.chat.id,
    )
    await callback.message.edit_text(
        f"<b>{html_escape(title)}</b>\n\n{hint}",
        parse_mode=ParseMode.HTML,
        reply_markup=_admin_abort_kb(),
    )
    await callback.answer()


@router.message(AdminStates.equipment_photo_wait, F.photo)
async def admin_equipment_got_photo(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not _is_admin(message.from_user.id, config):
        await state.clear()
        return
    data = await state.get_data()
    slot = data.get("equipment_photo_slot")
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")

    EQUIPMENT_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    dest = EQUIPMENT_UPLOAD_ROOT / f"eq_{uuid4().hex}.jpg"
    await message.bot.download(message.photo[-1], destination=dest)
    new_path = str(dest.resolve())

    s = await db.get_all_settings()
    lines = list(equipment_photo_paths(s, config))

    if slot is None:
        lines.append(new_path)
    else:
        idx = int(slot)
        if 0 <= idx < len(lines):
            old_path = lines[idx]
            _try_delete_uploaded_file(old_path)
            lines[idx] = new_path
        else:
            lines.append(new_path)

    await db.set_setting("equipment_photos_raw", "\n".join(lines))
    await state.clear()

    if mid is not None and cid is not None:
        s2 = await db.get_all_settings()
        await _restore_admin_panel_message(
            message.bot,
            int(cid),
            int(mid),
            text=_equipment_admin_text(s2),
            reply_markup=_equipment_menu_kb(s2),
        )
    try:
        await message.delete()
    except Exception:
        pass


@router.message(AdminStates.equipment_photo_wait)
async def admin_equipment_need_photo(message: Message, config: Config) -> None:
    if not _is_admin(message.from_user.id, config):
        return
    await message.answer(
        "Пришлите изображение <b>как фото</b> (в сжатом виде). "
        "Или нажмите «Отмена» под предыдущим сообщением."
        f"{_EQUIPMENT_PHOTO_SIZE_HINT}",
        parse_mode=ParseMode.HTML,
    )


@router.message(AdminStates.directions_video_wait, F.video)
async def admin_directions_got_video(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not _is_admin(message.from_user.id, config):
        await state.clear()
        return
    data = await state.get_data()
    fid = message.video.file_id
    await db.set_setting("studio_directions_video_file_id", fid)
    await state.clear()
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")
    if mid is not None and cid is not None:
        s2 = await db.get_all_settings()
        await _restore_admin_panel_message(
            message.bot,
            int(cid),
            int(mid),
            text=_studio_nav_admin_text(s2),
            reply_markup=_studio_nav_menu_kb(s2),
        )
    try:
        await message.delete()
    except Exception:
        pass


@router.message(AdminStates.directions_video_wait)
async def admin_directions_need_video(message: Message, config: Config) -> None:
    if not _is_admin(message.from_user.id, config):
        return
    await message.answer(
        "Пришлите видео или нажмите «Отмена».",
        parse_mode=ParseMode.HTML,
    )


@router.message(AdminStates.ui_photo_wait, F.photo)
async def admin_ui_got_photo(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    if not _is_admin(message.from_user.id, config):
        await state.clear()
        return
    data = await state.get_data()
    sk = data.get("ui_photo_setting_key")
    if not sk:
        await state.clear()
        return
    s = await db.get_all_settings()
    old = (s.get(sk) or "").strip()
    if old:
        _try_delete_ui_upload(old)

    UI_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    dest = UI_UPLOAD_ROOT / f"ui_{uuid4().hex}.jpg"
    await message.bot.download(message.photo[-1], destination=dest)
    new_path = str(dest.resolve())
    await db.set_setting(sk, new_path)
    await state.clear()
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")
    if mid is not None and cid is not None:
        s2 = await db.get_all_settings()
        await _restore_admin_panel_message(
            message.bot,
            int(cid),
            int(mid),
            text=_ui_photos_admin_text(s2),
            reply_markup=_ui_photos_menu_kb(s2),
        )
    try:
        await message.delete()
    except Exception:
        pass


@router.message(AdminStates.ui_photo_wait)
async def admin_ui_need_photo(message: Message, config: Config) -> None:
    if not _is_admin(message.from_user.id, config):
        return
    await message.answer(
        "Пришлите изображение как фото или нажмите «Отмена».",
        parse_mode=ParseMode.HTML,
    )


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
    elif key == "subscription_channel_link":
        val = "" if raw == "-" else raw
        await db.set_setting(key, val)
    elif key in ("subscription_channel_id", "schedule_channel_id", "payments_inbox_chat_id"):
        if raw == "-":
            await db.set_setting(key, "")
        else:
            clean = "".join(raw.split())
            try:
                int(clean)
            except ValueError:
                await message.answer(
                    "Нужно целое число (ID канала или чата) или <code>-</code> для сброса.",
                    parse_mode=ParseMode.HTML,
                )
                return
            await db.set_setting(key, clean)
    elif key in _TARIFF_TIME_SETTING_KEYS:
        ddef = build_default_settings_dict(config)
        val = (ddef.get(key) or "").strip() if raw == "-" else raw
        cur = await db.get_all_settings()
        merged = dict(cur)
        merged[key] = val
        opts = (
            tariff_day_start_list(merged)
            if key == "tariff_day_start_times"
            else tariff_night_start_list(merged)
        )
        if not opts:
            await message.answer(
                "Нужен хотя бы один корректный формат времени ЧЧ:ММ (например 09:00). "
                "Несколько значений — с новой строки или через запятую.",
                parse_mode=ParseMode.HTML,
            )
            return
        await db.set_setting(key, val)
    else:
        val = "" if raw == "-" else raw
        await db.set_setting(key, val)

    if key == "schedule_channel_id":
        await db.clear_schedule_channel_bot_messages()
        try:
            await _publish_weekly_and_tasks(message.bot, db, config)
        except Exception:
            pass

    await state.clear()
    mid = data.get("admin_panel_mid")
    cid = data.get("admin_panel_cid")
    if mid is not None and cid is not None:
        if key == "studio_address_html":
            s2 = await db.get_all_settings()
            await _restore_admin_panel_message(
                message.bot,
                int(cid),
                int(mid),
                text=_studio_nav_admin_text(s2),
                reply_markup=_studio_nav_menu_kb(s2),
            )
        elif key in _CHANNEL_SETTING_KEYS:
            s2 = await db.get_all_settings()
            await _restore_admin_panel_message(
                message.bot,
                int(cid),
                int(mid),
                text=_channels_admin_text(s2, config),
                reply_markup=_channels_menu_kb(),
            )
        elif key in _CLIENT_TEXT_SETTING_KEYS:
            s2 = await db.get_all_settings()
            await _restore_admin_panel_message(
                message.bot,
                int(cid),
                int(mid),
                text=_client_texts_admin_text(),
                reply_markup=_client_texts_menu_kb(),
            )
        elif key in _TARIFF_TIME_SETTING_KEYS:
            pricing2 = await load_effective_pricing(db, config)
            await _restore_admin_panel_message(
                message.bot,
                int(cid),
                int(mid),
                text="<b>💰 Цены</b>\nВыберите позицию, затем введите целое число (руб) в чат.\n"
                "<i>Стартовые времена дневного и ночного тарифа — отдельными кнопками (формат ЧЧ:ММ).</i>",
                reply_markup=_admin_prices_kb(pricing2),
            )
        elif key in _CONTACTS_SETTING_KEYS:
            await _restore_admin_panel_message(
                message.bot,
                int(cid),
                int(mid),
                text=_contacts_admin_text(),
                reply_markup=_contacts_menu_kb(),
            )
        else:
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
    await delete_booking_pending_ui_messages(callback.bot, dict(b), db)
    row = await db.cancel_booking(bid)
    if not row:
        await callback.answer("Ошибка отмены", show_alert=True)
        return
    kind = row.get("booking_kind") or "studio"
    if kind in (None, "studio"):
        await reminder_service.remove_for_booking(bid)
    try:
        await _publish_weekly_and_tasks(callback.bot, db, config)
    except Exception:
        pass
    pay_reject = (
        "<b>Оплата не подтверждена</b>\n\n"
        "Если вы уже перевели средства, напишите администратору. "
        "Можно оформить новую заявку через меню."
    )
    pay_reject = await append_manager_contact_html(db, pay_reject, config)
    try:
        await callback.bot.send_message(
            int(row["user_id"]),
            pay_reject,
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


@router.callback_query(F.data.startswith("task_done:"))
async def channel_task_mark_done(
    callback: CallbackQuery,
    db: Database,
    config: Config,
) -> None:
    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer()
        return
    kind, bid_s = parts[1], parts[2]
    if kind not in ("lyrics", "beat"):
        await callback.answer()
        return
    try:
        bid = int(bid_s)
    except ValueError:
        await callback.answer()
        return

    if not await _user_can_mark_service_done(callback, db, config, kind=kind):
        await callback.answer(
            "Нет доступа. Кнопку может нажать админ или исполнитель (@username как в настройках контакта).",
            show_alert=True,
        )
        return

    try:
        row = await db.complete_service_order(bid, kind)
        if not row:
            await callback.answer(
                "Заявка не найдена или ещё не в статусе «оплачено» (ожидает подтверждения оплаты).",
                show_alert=True,
            )
            return
        uid = int(row["user_id"])
        try:
            await remove_service_order_from_user_activity(
                callback.bot,
                db,
                config,
                user_id=uid,
                booking_id=bid,
            )
        except Exception:
            logger.exception("remove_service_order_from_user_activity failed bid=%s", bid)
        try:
            await _publish_weekly_and_tasks(callback.bot, db, config)
        except Exception:
            logger.exception("_publish_weekly_and_tasks failed after task_done bid=%s", bid)
        await callback.answer("Готово")
    except Exception:
        logger.exception("task_done handler failed data=%s", callback.data)
        await callback.answer("Ошибка при обработке. Смотрите логи бота.", show_alert=True)


@router.callback_query(F.data.startswith("cnc:ok:"))
async def user_cancellation_approve(
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
    if not b or b.get("status") != "pending_cancel":
        await callback.answer("Уже обработано или не найдено", show_alert=True)
        return
    snap = dict(b)
    row = await db.cancel_booking(bid)
    if not row:
        await callback.answer("Ошибка отмены", show_alert=True)
        return
    kind = row.get("booking_kind") or "studio"
    if kind in (None, "studio"):
        await reminder_service.remove_for_booking(bid)
    try:
        await _publish_weekly_and_tasks(callback.bot, db, config)
    except Exception:
        pass
    try:
        await remove_booking_from_user_activity(
            callback.bot,
            db,
            config,
            user_id=int(row["user_id"]),
            booking_id=bid,
        )
    except Exception:
        pass
    custom = await cancel_confirmed_custom_html(db, kind)
    if custom:
        user_text = custom
    else:
        if kind in ("lyrics", "beat"):
            lines = ["<b>Заявка отменена.</b>"]
        else:
            lines = ["<b>Запись отменена.</b>", "Слот снова доступен для бронирования."]
        user_text = "\n\n".join(lines)
    warn = await cancel_refund_warning_html(db, config)
    if Database.booking_time_started(snap, timezone=config.timezone) and warn:
        user_text = f"{user_text}\n\n{warn}"
    user_text = await append_manager_contact_html(db, user_text, config)
    try:
        await delete_pending_ui_and_send_main_menu(
            callback.bot,
            db,
            config,
            booking_snapshot=row,
            announcement_html=user_text,
        )
    except Exception:
        pass
    try:
        t = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            t + "\n\n<b>✅ Отмена подтверждена</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await callback.answer("Подтверждено")


@router.callback_query(F.data.startswith("cnc:no:"))
async def user_cancellation_reject(
    callback: CallbackQuery,
    db: Database,
    config: Config,
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        bid = int(callback.data.split(":")[2])
    except ValueError:
        await callback.answer()
        return
    row = await db.reject_user_cancellation(bid)
    if not row:
        await callback.answer("Уже обработано", show_alert=True)
        return
    rej = await append_manager_contact_html(
        db,
        "<b>Запрос на отмену отклонён</b>\n\n"
        "Ваша запись остаётся в силе.",
        config,
    )
    try:
        await delete_pending_ui_and_send_main_menu(
            callback.bot,
            db,
            config,
            booking_snapshot=row,
            announcement_html=rej,
        )
    except Exception:
        pass
    try:
        t = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            t + "\n\n<b>❌ Отмена отклонена</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await callback.answer("Отклонено")


@router.callback_query(F.data.startswith("rsc:ok:"))
async def user_reschedule_approve(
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
    b = await db.approve_user_reschedule(bid)
    if not b:
        await callback.answer(
            "Слоты заняты или заявка уже обработана. Попробуйте позже.",
            show_alert=True,
        )
        return
    await reminder_service.remove_for_booking(bid)
    await reminder_service.schedule_for_booking(b)
    await _publish_weekly_and_tasks(callback.bot, db, config)
    uid = int(b["user_id"])
    msg = (
        "<b>Перенос подтверждён</b>\n\n"
        f"<b>Новая дата и время:</b> {html_escape(str(b['day']))} "
        f"{html_escape(str(b['start_time']))} — {html_escape(str(b['end_time']))}"
    )
    msg = await append_manager_contact_html(db, msg, config)
    try:
        await delete_pending_ui_and_send_main_menu(
            callback.bot,
            db,
            config,
            booking_snapshot=b,
            announcement_html=msg,
        )
    except Exception:
        pass
    try:
        t = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            t + "\n\n<b>✅ Перенос подтверждён</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await callback.answer("Подтверждено")


@router.callback_query(F.data.startswith("rsc:no:"))
async def user_reschedule_reject(
    callback: CallbackQuery,
    db: Database,
    config: Config,
) -> None:
    if not _is_admin(callback.from_user.id, config):
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        bid = int(callback.data.split(":")[2])
    except ValueError:
        await callback.answer()
        return
    row = await db.reject_user_reschedule(bid)
    if not row:
        await callback.answer("Уже обработано", show_alert=True)
        return
    rej = await append_manager_contact_html(
        db,
        "<b>Запрос на перенос отклонён</b>\n\n"
        "Ваша запись остаётся на прежние дату и время.",
        config,
    )
    try:
        await delete_pending_ui_and_send_main_menu(
            callback.bot,
            db,
            config,
            booking_snapshot=row,
            announcement_html=rej,
        )
    except Exception:
        pass
    try:
        t = callback.message.text or callback.message.caption or ""
        await callback.message.edit_text(
            t + "\n\n<b>❌ Перенос отклонён</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except Exception:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await callback.answer("Отклонено")


