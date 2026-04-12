from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from catalog import SERVICES
from config import Config
from database.db import Database

from services.effective_pricing import EffectivePricing

def studio_mode_kb() -> InlineKeyboardMarkup:
    """После выбора записи со / без звукорежиссёра: почасовая или тарифы."""
    kb = InlineKeyboardBuilder()
    kb.button(text="⏱ Почасовая запись", callback_data="stm:hourly")
    kb.button(text="📦 Тарифы", callback_data="stm:tariff")
    kb.button(text="⬅ К услугам", callback_data="book:pick_product")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def tariff_category_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🌙 Ночная запись", callback_data="trf:c:night")
    kb.button(text="☀️ Дневная запись", callback_data="trf:c:day")
    kb.button(text="⬅ Назад", callback_data="stm:back")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def tariff_day_start_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="С 09:00", callback_data="trf:s:09")
    kb.button(text="С 12:00", callback_data="trf:s:12")
    kb.button(text="⬅ Назад", callback_data="trf:c:back")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def tariff_hours_kb(
    *, night: bool, pricing: EffectivePricing, with_engineer: bool = False
) -> InlineKeyboardMarkup:
    """6 / 8 / 10 / 12 ч с ценами из БД / .env; with_engineer — отдельный прайс со звукорежиссёром."""
    kb = InlineKeyboardBuilder()
    # 2×2: верхний ряд 6 и 10 ч, нижний — 8 и 12 (шире кнопки, цены не обрезаются)
    for h in (6, 10, 8, 12):
        p = pricing.tariff_rub(night=night, hours=h, with_engineer=with_engineer)
        kb.button(text=f"{h} ч — {p} руб", callback_data=f"trf:h:{h}")
    kb.adjust(2, 2)
    back_cd = "trf:c:back" if night else "trf:h:back"
    kb.button(text="⬅ Назад", callback_data=back_cd)
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(2)
    return kb.as_markup()


def booking_products_kb(*, pricing: EffectivePricing) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if pricing.service_no_engineer_enabled:
        kb.button(text="🎤 Запись без звукорежиссёра", callback_data="prod:no_engineer")
    if pricing.service_with_engineer_enabled:
        kb.button(text="🎛️ Запись с звукорежиссёром", callback_data="prod:with_engineer")
    if pricing.service_lyrics_enabled:
        kb.button(text="📝 Текст для песни", callback_data="prod:lyrics")
    if pricing.service_beat_enabled:
        kb.button(text="🎚️ Бит для песни", callback_data="prod:beat")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🎧 Записаться", callback_data="book:start")
    kb.button(text="📅 Моя запись", callback_data="book:my")
    kb.button(text="💳 Прайсы", callback_data="menu:prices")
    kb.button(text="📸 Оборудование и Фото Студии", callback_data="menu:equipment")
    kb.adjust(1)
    return kb.as_markup()


def back_to_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def subscription_kb(channel_link: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="Подписаться", url=channel_link))
    kb.button(text="Проверить подписку", callback_data="sub:check")
    kb.row(InlineKeyboardButton(text="⬅ В меню", callback_data="menu:home"))
    return kb.as_markup()


def month_calendar_kb(
    year: int,
    month: int,
    allowed_days: set[str],
    *,
    prefix: str = "date",
    nav_prefix: str = "cal",
    blocked_days: set[str] | None = None,
    today: date | None = None,
    mark_past_as_blocked: bool = True,
    nav_back_callback: str = "menu:home",
    nav_back_text: str = "⬅ В меню",
    closed_days_highlight: set[str] | None = None,
) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text=f"{month:02d}.{year}", callback_data="noop"))

    weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    kb.row(*[InlineKeyboardButton(text=d, callback_data="noop") for d in weekdays])

    # monthrange: первый день месяца — понедельник = 0 (совпадает с рядом Пн…Вс)
    first_weekday, days_in_month = monthrange(year, month)
    today_d = today or date.today()

    row: list[InlineKeyboardButton] = []
    for _ in range(first_weekday):
        row.append(InlineKeyboardButton(text=" ", callback_data="noop"))

    for day_num in range(1, days_in_month + 1):
        current = date(year, month, day_num).isoformat()
        day_d = date(year, month, day_num)
        if mark_past_as_blocked and day_d < today_d:
            btn = InlineKeyboardButton(text=f"❌{day_num}", callback_data="noop")
        elif blocked_days and current in blocked_days:
            btn = InlineKeyboardButton(text=f"❌{day_num}", callback_data="noop")
        elif current in allowed_days:
            if closed_days_highlight and current in closed_days_highlight:
                label = f"⛔{day_num}"
            else:
                label = f"✅{day_num}"
            btn = InlineKeyboardButton(text=label, callback_data=f"{prefix}:{current}")
        else:
            btn = InlineKeyboardButton(text=str(day_num), callback_data="noop")
        row.append(btn)
        if len(row) == 7:
            kb.row(*row)
            row = []

    if row:
        while len(row) < 7:
            row.append(InlineKeyboardButton(text=" ", callback_data="noop"))
        kb.row(*row)

    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    kb.row(
        InlineKeyboardButton(text="◀", callback_data=f"{nav_prefix}:{prev_year}-{prev_month}"),
        InlineKeyboardButton(text="▶", callback_data=f"{nav_prefix}:{next_year}-{next_month}"),
    )
    kb.row(InlineKeyboardButton(text=nav_back_text, callback_data=nav_back_callback))
    return kb.as_markup()


def slots_pick_kb(slots: list[dict], selected_ids: set[int]) -> InlineKeyboardMarkup:
    """
    ✅ свободно, ❌ занято, ☑ выбрано.
    Два столбца: слева часы с 00:00, справа с 12:00.
    Строки сверху вниз: 00:00/12:00, затем 01:00/13:00 и т.д.
    """
    _sp = "\u2800"  # Braille blank — «пустая» кнопка для Telegram

    def _slot_btn(slot: dict) -> InlineKeyboardButton:
        sid = int(slot["id"])
        label = f"{slot['start_time']}-{slot['end_time']}"
        if not Database.slot_row_is_active(slot["is_active"]):
            return InlineKeyboardButton(text=f"❌ {label}", callback_data="noop")
        if sid in selected_ids:
            return InlineKeyboardButton(text=f"☑ {label}", callback_data=f"slot_pick:{sid}")
        return InlineKeyboardButton(text=f"✅ {label}", callback_data=f"slot_pick:{sid}")

    def _start_hour(slot: dict) -> int:
        h, _ = Database.time_sort_key(Database._coerce_cell_str(slot["start_time"]))
        return int(h)

    left_slots = [s for s in slots if _start_hour(s) < 12]
    right_slots = [s for s in slots if _start_hour(s) >= 12]

    rows: list[list[InlineKeyboardButton]] = []
    n_rows = max(len(left_slots), len(right_slots))
    for i in range(n_rows):
        left = _slot_btn(left_slots[i]) if i < len(left_slots) else InlineKeyboardButton(text=_sp, callback_data="noop")
        right = _slot_btn(right_slots[i]) if i < len(right_slots) else InlineKeyboardButton(text=_sp, callback_data="noop")
        rows.append([left, right])

    rows.append([InlineKeyboardButton(text="✅ Подтвердить", callback_data="slot_confirm")])
    rows.append([InlineKeyboardButton(text="⬅ К календарю", callback_data="book:calendar")])
    rows.append([InlineKeyboardButton(text="⬅ В меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def slots_rs_pick_kb(slots: list[dict], selected_ids: set[int]) -> InlineKeyboardMarkup:
    """Сетка слотов для переноса записи (отдельные callback от обычной записи)."""
    _sp = "\u2800"

    def _slot_btn(slot: dict) -> InlineKeyboardButton:
        sid = int(slot["id"])
        label = f"{slot['start_time']}-{slot['end_time']}"
        if not Database.slot_row_is_active(slot["is_active"]):
            return InlineKeyboardButton(text=f"❌ {label}", callback_data="noop")
        if sid in selected_ids:
            return InlineKeyboardButton(text=f"☑ {label}", callback_data=f"rs_pick:{sid}")
        return InlineKeyboardButton(text=f"✅ {label}", callback_data=f"rs_pick:{sid}")

    def _start_hour(slot: dict) -> int:
        h, _ = Database.time_sort_key(Database._coerce_cell_str(slot["start_time"]))
        return int(h)

    left_slots = [s for s in slots if _start_hour(s) < 12]
    right_slots = [s for s in slots if _start_hour(s) >= 12]

    rows: list[list[InlineKeyboardButton]] = []
    n_rows = max(len(left_slots), len(right_slots))
    for i in range(n_rows):
        left = (
            _slot_btn(left_slots[i])
            if i < len(left_slots)
            else InlineKeyboardButton(text=_sp, callback_data="noop")
        )
        right = (
            _slot_btn(right_slots[i])
            if i < len(right_slots)
            else InlineKeyboardButton(text=_sp, callback_data="noop")
        )
        rows.append([left, right])

    rows.append([InlineKeyboardButton(text="✅ Далее", callback_data="rs_slot_confirm")])
    rows.append([InlineKeyboardButton(text="⬅ К календарю", callback_data="rscal:back")])
    rows.append([InlineKeyboardButton(text="⬅ В меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def services_kb(selected: set[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for code, (title, price) in SERVICES.items():
        mark = "✅" if code in selected else "⬜"
        kb.button(text=f"{mark} {title} ({price} руб)", callback_data=f"srv:{code}")
    kb.button(text="✅ Подтвердить услуги", callback_data="srv:confirm")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def paid_kb(*, online: bool = False) -> InlineKeyboardMarkup:
    """online=True — оплата через ЮKassa (кнопка «Перейти к оплате»)."""
    kb = InlineKeyboardBuilder()
    label = "💳 Перейти к оплате" if online else "✅ Я оплатил"
    kb.button(text=label, callback_data="book:paid")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def my_booking_kb(booking_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отменить запись", callback_data=f"book:cancel:{booking_id}")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def my_bookings_kb(
    rows: list,
    *,
    show_directions: bool = False,
) -> InlineKeyboardMarkup:
    """rows — словари броней с ключами id, status, booking_kind."""
    kb = InlineKeyboardBuilder()
    for b in rows:
        bid = int(b["id"])
        kb.button(text=f"❌ Отменить #{bid}", callback_data=f"book:cancel:{bid}")
        kind = b.get("booking_kind") or "studio"
        st = b.get("status")
        if kind == "studio" and st == "active":
            kb.button(text=f"📅 Перенести #{bid}", callback_data=f"book:rsch:{bid}")
    if show_directions:
        kb.button(text="🗺 Как пройти до студии", callback_data="book:directions")
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def cancel_confirm_kb(booking_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, отменить", callback_data=f"book:cc:yes:{booking_id}")
    kb.button(text="⬅ Не отменять", callback_data=f"book:cc:no:{booking_id}")
    kb.adjust(1)
    return kb.as_markup()


def reschedule_confirm_kb(booking_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Запросить перенос", callback_data=f"book:rsc:send:{booking_id}")
    kb.button(text="⬅ Отмена", callback_data=f"book:rsc:abort:{booking_id}")
    kb.adjust(1)
    return kb.as_markup()


def equipment_back_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅ В меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def equipment_carousel_kb(index: int, total: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if total > 1:
        prev_i = (index - 1) % total
        next_i = (index + 1) % total
        kb.row(
            InlineKeyboardButton(text="◀", callback_data=f"equip:{prev_i}"),
            InlineKeyboardButton(text=f"{index+1}/{total}", callback_data="noop"),
            InlineKeyboardButton(text="▶", callback_data=f"equip:{next_i}"),
        )
    kb.row(InlineKeyboardButton(text="⬅ В меню", callback_data="menu:home"))
    return kb.as_markup()


def now_month() -> tuple[int, int]:
    n = datetime.now()
    return n.year, n.month

