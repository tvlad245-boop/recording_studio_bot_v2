"""Цены и флаги услуг: значения из БД (bot_settings) с fallback на Config и catalog.SERVICES."""

from __future__ import annotations

from dataclasses import dataclass

from catalog import SERVICES
from config import Config
from database.db import Database


def _gi(settings: dict[str, str], key: str, default: int) -> int:
    try:
        raw = settings.get(key, str(default))
        return int((raw or str(default)).strip())
    except ValueError:
        return default


def _gb(settings: dict[str, str], key: str, default: bool) -> bool:
    v = (settings.get(key, "1" if default else "0") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def build_default_settings_dict(cfg: Config) -> dict[str, str]:
    """Первичное заполнение bot_settings (только отсутствующие ключи)."""
    d: dict[str, str] = {
        "price_no_engineer": str(SERVICES["no_engineer"][1]),
        "price_with_engineer": str(SERVICES["with_engineer"][1]),
        "price_lyrics": str(SERVICES["lyrics"][1]),
        "price_beat": str(SERVICES["beat"][1]),
        "service_lyrics_enabled": "1",
        "service_beat_enabled": "1",
        "service_no_engineer_enabled": "1",
        "service_with_engineer_enabled": "1",
        "tariff_night_6h": str(cfg.tariff_night_6h),
        "tariff_night_8h": str(cfg.tariff_night_8h),
        "tariff_night_10h": str(cfg.tariff_night_10h),
        "tariff_night_12h": str(cfg.tariff_night_12h),
        "tariff_day_6h": str(cfg.tariff_day_6h),
        "tariff_day_8h": str(cfg.tariff_day_8h),
        "tariff_day_10h": str(cfg.tariff_day_10h),
        "tariff_day_12h": str(cfg.tariff_day_12h),
        "tariff_night_6h_engineer": str(cfg.tariff_night_6h_engineer),
        "tariff_night_8h_engineer": str(cfg.tariff_night_8h_engineer),
        "tariff_night_10h_engineer": str(cfg.tariff_night_10h_engineer),
        "tariff_night_12h_engineer": str(cfg.tariff_night_12h_engineer),
        "tariff_day_6h_engineer": str(cfg.tariff_day_6h_engineer),
        "tariff_day_8h_engineer": str(cfg.tariff_day_8h_engineer),
        "tariff_day_10h_engineer": str(cfg.tariff_day_10h_engineer),
        "tariff_day_12h_engineer": str(cfg.tariff_day_12h_engineer),
        "studio_address_html": "",
        "studio_directions_video_file_id": "",
        "ui_photo_main_menu_path": "",
        "ui_photo_prices_path": "",
        "ui_photo_payment_path": "",
        "ui_photo_tariff_category_path": "",
        "ui_photo_tariff_night_path": "",
        "ui_photo_tariff_day_path": "",
        "subscription_channel_id": "",
        "subscription_channel_link": "",
        "schedule_channel_id": "",
        "payments_inbox_chat_id": "",
        "manager_contact_html": "",
        "textmaker_username": "",
        "beatmaker_username": "",
        "cancel_refund_warning_html": (
            "<i>Если время аренды уже началось, возврат средств за эту запись не производится.</i>"
        ),
        "cancel_request_sent_html": "",
        "cancel_confirmed_studio_html": "",
        "cancel_confirmed_service_html": "",
    }
    return d


@dataclass(frozen=True)
class EffectivePricing:
    price_no_engineer: int
    price_with_engineer: int
    price_lyrics: int
    price_beat: int
    service_lyrics_enabled: bool
    service_beat_enabled: bool
    service_no_engineer_enabled: bool
    service_with_engineer_enabled: bool
    tariff_night_6h: int
    tariff_night_8h: int
    tariff_night_10h: int
    tariff_night_12h: int
    tariff_day_6h: int
    tariff_day_8h: int
    tariff_day_10h: int
    tariff_day_12h: int
    tariff_night_6h_engineer: int
    tariff_night_8h_engineer: int
    tariff_night_10h_engineer: int
    tariff_night_12h_engineer: int
    tariff_day_6h_engineer: int
    tariff_day_8h_engineer: int
    tariff_day_10h_engineer: int
    tariff_day_12h_engineer: int

    def service_title(self, code: str) -> str:
        return SERVICES[code][0]

    def service_price(self, code: str) -> int:
        if code == "no_engineer":
            return self.price_no_engineer
        if code == "with_engineer":
            return self.price_with_engineer
        if code == "lyrics":
            return self.price_lyrics
        if code == "beat":
            return self.price_beat
        return 0

    def service_line(self, code: str) -> tuple[str, int]:
        return self.service_title(code), self.service_price(code)

    def tariff_rub(self, *, night: bool, hours: int, with_engineer: bool = False) -> int:
        if hours not in (6, 8, 10, 12):
            return 0
        if with_engineer:
            if night:
                m = {
                    6: self.tariff_night_6h_engineer,
                    8: self.tariff_night_8h_engineer,
                    10: self.tariff_night_10h_engineer,
                    12: self.tariff_night_12h_engineer,
                }
            else:
                m = {
                    6: self.tariff_day_6h_engineer,
                    8: self.tariff_day_8h_engineer,
                    10: self.tariff_day_10h_engineer,
                    12: self.tariff_day_12h_engineer,
                }
        else:
            if night:
                m = {
                    6: self.tariff_night_6h,
                    8: self.tariff_night_8h,
                    10: self.tariff_night_10h,
                    12: self.tariff_night_12h,
                }
            else:
                m = {
                    6: self.tariff_day_6h,
                    8: self.tariff_day_8h,
                    10: self.tariff_day_10h,
                    12: self.tariff_day_12h,
                }
        return int(m[hours])


async def load_effective_pricing(db: Database, cfg: Config) -> EffectivePricing:
    s = await db.get_all_settings()
    return EffectivePricing(
        price_no_engineer=_gi(s, "price_no_engineer", SERVICES["no_engineer"][1]),
        price_with_engineer=_gi(s, "price_with_engineer", SERVICES["with_engineer"][1]),
        price_lyrics=_gi(s, "price_lyrics", SERVICES["lyrics"][1]),
        price_beat=_gi(s, "price_beat", SERVICES["beat"][1]),
        service_lyrics_enabled=_gb(s, "service_lyrics_enabled", True),
        service_beat_enabled=_gb(s, "service_beat_enabled", True),
        service_no_engineer_enabled=_gb(s, "service_no_engineer_enabled", True),
        service_with_engineer_enabled=_gb(s, "service_with_engineer_enabled", True),
        tariff_night_6h=_gi(s, "tariff_night_6h", cfg.tariff_night_6h),
        tariff_night_8h=_gi(s, "tariff_night_8h", cfg.tariff_night_8h),
        tariff_night_10h=_gi(s, "tariff_night_10h", cfg.tariff_night_10h),
        tariff_night_12h=_gi(s, "tariff_night_12h", cfg.tariff_night_12h),
        tariff_day_6h=_gi(s, "tariff_day_6h", cfg.tariff_day_6h),
        tariff_day_8h=_gi(s, "tariff_day_8h", cfg.tariff_day_8h),
        tariff_day_10h=_gi(s, "tariff_day_10h", cfg.tariff_day_10h),
        tariff_day_12h=_gi(s, "tariff_day_12h", cfg.tariff_day_12h),
        tariff_night_6h_engineer=_gi(s, "tariff_night_6h_engineer", cfg.tariff_night_6h_engineer),
        tariff_night_8h_engineer=_gi(s, "tariff_night_8h_engineer", cfg.tariff_night_8h_engineer),
        tariff_night_10h_engineer=_gi(s, "tariff_night_10h_engineer", cfg.tariff_night_10h_engineer),
        tariff_night_12h_engineer=_gi(s, "tariff_night_12h_engineer", cfg.tariff_night_12h_engineer),
        tariff_day_6h_engineer=_gi(s, "tariff_day_6h_engineer", cfg.tariff_day_6h_engineer),
        tariff_day_8h_engineer=_gi(s, "tariff_day_8h_engineer", cfg.tariff_day_8h_engineer),
        tariff_day_10h_engineer=_gi(s, "tariff_day_10h_engineer", cfg.tariff_day_10h_engineer),
        tariff_day_12h_engineer=_gi(s, "tariff_day_12h_engineer", cfg.tariff_day_12h_engineer),
    )
