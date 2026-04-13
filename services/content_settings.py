"""Тексты раздела «Оборудование» и блоки после оплаты — из bot_settings с fallback на Config."""

from __future__ import annotations

import os
from html import escape as html_escape

from config import Config
from database.db import Database


def setting_bool(settings: dict[str, str], key: str, default: bool = True) -> bool:
    v = (settings.get(key, "1" if default else "0") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def format_maker_username(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return "—"
    return u if u.startswith("@") else f"@{u}"


def _parse_tariff_time_lines(stored: str | None, fallback_multiline: str) -> list[str]:
    """Список уникальных времён ЧЧ:ММ для кнопок тарифов (настройки из админки)."""
    raw = (stored or "").strip()
    if not raw:
        raw = fallback_multiline
    out: list[str] = []
    for piece in raw.replace(",", "\n").splitlines():
        t = piece.strip()
        if not t:
            continue
        nt = Database.normalize_time_str(t)
        if nt not in out:
            out.append(nt)
    if not out:
        for piece in fallback_multiline.splitlines():
            t = piece.strip()
            if not t:
                continue
            nt = Database.normalize_time_str(t)
            if nt not in out:
                out.append(nt)
    return out


def tariff_day_start_list(settings: dict[str, str]) -> list[str]:
    return _parse_tariff_time_lines(
        settings.get("tariff_day_start_times"),
        "09:00\n12:00",
    )


def tariff_night_start_list(settings: dict[str, str]) -> list[str]:
    return _parse_tariff_time_lines(
        settings.get("tariff_night_start_times"),
        "00:00",
    )


async def equipment_caption_html(db: Database, cfg: Config) -> str:
    s = await db.get_all_settings()
    if setting_bool(s, "equipment_use_custom", False):
        c = (s.get("equipment_custom_html") or "").strip()
        if c:
            return c
    title = (s.get("equipment_title") or "").strip() or cfg.equipment_title
    body = (s.get("equipment_body") or "").strip() or cfg.equipment_text
    mic = (s.get("equipment_mic") or "").strip() or cfg.microphone_name
    card = (s.get("equipment_audiocard") or "").strip() or cfg.audiocard_name
    hp = (s.get("equipment_headphones") or "").strip() or cfg.headphones_name
    mon = (s.get("equipment_monitors") or "").strip() or cfg.monitors_name
    return (
        f"<b>📸 {html_escape(title)}</b>\n\n"
        f"{html_escape(body)}\n\n"
        f"🎙️ Микрофон: {html_escape(mic)}\n"
        f"🎛️ Аудиокарта: {html_escape(card)}\n"
        f"🎧 Наушники: {html_escape(hp)}\n"
        f"🖥️ Мониторы: {html_escape(mon)}\n"
    )


def _ui_photo_path(settings: dict[str, str], cfg: Config, setting_key: str, config_attr: str) -> str:
    """Путь к картинке: из bot_settings, иначе из Config (.env)."""
    raw = (settings.get(setting_key) or "").strip()
    if raw:
        ap = os.path.abspath(raw)
        if os.path.isfile(ap):
            return ap
    return (getattr(cfg, config_attr, None) or "").strip()


def ui_photo_main_menu(settings: dict[str, str], cfg: Config) -> str:
    return _ui_photo_path(settings, cfg, "ui_photo_main_menu_path", "main_menu_photo_path")


def ui_photo_prices(settings: dict[str, str], cfg: Config) -> str:
    return _ui_photo_path(settings, cfg, "ui_photo_prices_path", "prices_photo_path")


def ui_photo_payment(settings: dict[str, str], cfg: Config) -> str:
    return _ui_photo_path(settings, cfg, "ui_photo_payment_path", "payment_photo_path")


def ui_photo_tariff_category(settings: dict[str, str], cfg: Config) -> str:
    return _ui_photo_path(settings, cfg, "ui_photo_tariff_category_path", "tariff_category_photo_path")


def ui_photo_tariff_night(settings: dict[str, str], cfg: Config) -> str:
    return _ui_photo_path(settings, cfg, "ui_photo_tariff_night_path", "tariff_night_photo_path")


def ui_photo_tariff_day(settings: dict[str, str], cfg: Config) -> str:
    return _ui_photo_path(settings, cfg, "ui_photo_tariff_day_path", "tariff_day_photo_path")


def equipment_photo_paths(settings: dict[str, str], cfg: Config) -> list[str]:
    raw = (settings.get("equipment_photos_raw") or "").strip()
    out: list[str] = []
    if raw:
        for line in raw.splitlines():
            p = line.strip()
            if not p:
                continue
            ap = os.path.abspath(p)
            if os.path.isfile(ap):
                out.append(ap)
    if out:
        return out
    return list(cfg.equipment_photos)


async def studio_address_html(db: Database) -> str:
    """HTML из админки; пусто — адрес не показываем."""
    s = await db.get_all_settings()
    return (s.get("studio_address_html") or "").strip()


async def studio_directions_video_file_id(db: Database) -> str:
    s = await db.get_all_settings()
    return (s.get("studio_directions_video_file_id") or "").strip()


async def manager_contact_html(db: Database) -> str:
    """HTML блок контакта менеджера (подсказки клиенту)."""
    s = await db.get_all_settings()
    return (s.get("manager_contact_html") or "").strip()


async def effective_maker_username(db: Database, cfg: Config, *, kind: str) -> str:
    """Контакт текстовика/битмейкера: из bot_settings, иначе из .env."""
    s = await db.get_all_settings()
    key = "textmaker_username" if kind == "lyrics" else "beatmaker_username"
    raw = (s.get(key) or "").strip()
    if raw:
        return raw
    return cfg.textmaker_username if kind == "lyrics" else cfg.beatmaker_username


async def cancel_request_sent_body_html(db: Database, *, studio: bool) -> str:
    """Текст после отправки запроса на отмену (настраивается в «Тексты для клиентов»)."""
    s = await db.get_all_settings()
    raw = (s.get("cancel_request_sent_html") or "").strip()
    if raw:
        return raw
    if studio:
        return (
            "<b>⏳ Запрос на отмену отправлен.</b>\n\n"
            "Ожидайте решения оператора. Слот пока занят."
        )
    return "<b>⏳ Запрос на отмену отправлен.</b>\n\nОжидайте решения оператора."


async def cancel_confirmed_custom_html(db: Database, booking_kind: str) -> str:
    """Если задано в админке — основной HTML после подтверждённой отмены (до предупреждения о возврате и контакта менеджера)."""
    s = await db.get_all_settings()
    key = "cancel_confirmed_service_html" if booking_kind in ("lyrics", "beat") else "cancel_confirmed_studio_html"
    return (s.get(key) or "").strip()


async def cancel_refund_warning_html(db: Database, cfg: Config | None = None) -> str:
    s = await db.get_all_settings()
    raw = (s.get("cancel_refund_warning_html") or "").strip()
    if raw:
        return raw
    if cfg is not None:
        from services.effective_pricing import build_default_settings_dict

        return (build_default_settings_dict(cfg).get("cancel_refund_warning_html") or "").strip()
    return ""


async def append_manager_contact_html(db: Database, text: str, cfg: Config | None = None) -> str:
    """Добавляет к тексту блок «Контакт менеджера» из админки; без подсказки «ответьте на сообщение»."""
    m = await manager_contact_html(db)
    if not m:
        return text
    return f"{text}\n\n{m}"


async def post_payment_contact_block_html(db: Database, cfg: Config, *, kind: str) -> str:
    s = await db.get_all_settings()
    key = "postpay_lyrics_html" if kind == "lyrics" else "postpay_beat_html"
    custom = (s.get(key) or "").strip()
    if custom:
        return custom
    maker = await effective_maker_username(db, cfg, kind=kind)
    return (
        f"<b>Исполнитель:</b> {html_escape(format_maker_username(maker))}\n"
        "Мы свяжемся с вами."
    )
