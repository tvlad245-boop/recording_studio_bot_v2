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


async def post_payment_contact_block_html(db: Database, cfg: Config, *, kind: str) -> str:
    s = await db.get_all_settings()
    key = "postpay_lyrics_html" if kind == "lyrics" else "postpay_beat_html"
    custom = (s.get(key) or "").strip()
    if custom:
        return custom
    maker = cfg.textmaker_username if kind == "lyrics" else cfg.beatmaker_username
    return (
        f"<b>Исполнитель:</b> {html_escape(format_maker_username(maker))}\n"
        "Мы свяжемся с вами."
    )
