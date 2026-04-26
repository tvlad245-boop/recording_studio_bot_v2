"""
Почасовая запись на студию из расписания Yclients (book_times → выбор → POST /records).

Тарифные пакеты (6/8/10/12 ч) пока остаются на локальной SQLite-сетке — только режим «Почасовая».
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from datetime import date, datetime, timedelta
from typing import Any

from config import Config
from services.yclients_client import (
    YclientsError,
    parse_service_ids_csv,
    yclients_book_times,
    yclients_create_record,
    yclients_is_configured,
)

logger = logging.getLogger(__name__)

_DIGITS_PHONE = re.compile(r"\d+")


def yclients_studio_enabled(cfg: Config) -> bool:
    return bool(cfg.yclients_studio) and yclients_is_configured(cfg)


def service_ids_for_book(cfg: Config) -> list[int]:
    return parse_service_ids_csv(cfg.yclients_service_ids_csv)


def normalize_ru_phone(raw: str) -> str:
    """Только цифры, для API Yclients — 11 цифр с 7 (РФ)."""
    s = raw or ""
    digits = "".join(_DIGITS_PHONE.findall(s))
    if not digits:
        return ""
    if digits.startswith("8") and len(digits) >= 11:
        digits = "7" + digits[1:]
    if digits.startswith("9") and len(digits) == 10:
        digits = "7" + digits
    if len(digits) > 11:
        digits = digits[:11]
    return digits


def _seance_length_sec(s: dict[str, Any]) -> int:
    v = s.get("seance_length")
    if v is None:
        v = s.get("length")
    try:
        n = int(v)
    except (TypeError, ValueError):
        n = 0
    return n if n > 0 else 1800


def _parse_hhmm(raw: str) -> tuple[int, int] | None:
    raw = (raw or "").strip()
    m = re.match(r"^(\d{1,2})\s*:\s*(\d{2})\s*$", raw)
    if not m:
        return None
    h, mm = int(m.group(1)), int(m.group(2))
    if 0 <= h < 24 and 0 <= mm < 60:
        return h, mm
    return None


def _seance_time_labels(s: dict[str, Any], cfg: Config | None = None) -> tuple[str, str]:
    """
    Старт/конец для UI (как в time_slots).
    Ожидаем поля time / datetime + seance_length.
    """
    start = ""
    t_raw = s.get("time")
    if isinstance(t_raw, str):
        start = t_raw.strip()
    elif isinstance(t_raw, dict):
        start = str(t_raw.get("time") or t_raw.get("from") or "").strip()

    if not start and s.get("datetime") is not None and cfg is not None:
        from zoneinfo import ZoneInfo

        try:
            tz = ZoneInfo(cfg.timezone)
            dtv = s.get("datetime")
            if isinstance(dtv, (int, float)):
                dtl = datetime.fromtimestamp(int(dtv), tz=tz)
                start = dtl.strftime("%H:%M")
        except (OverflowError, OSError, TypeError, ValueError):
            start = ""
    if not start:
        start = "00:00"
    pm = _parse_hhmm(start) or (0, 0)
    start = f"{pm[0]:02d}:{pm[1]:02d}"
    seclen = _seance_length_sec(s)
    h0, m0 = pm[0], pm[1]
    endm = h0 * 60 + m0 * 1 + (seclen // 60)
    eh = (endm // 60) % 24
    emi = endm % 60
    end = f"{eh:02d}:{emi:02d}"
    return start, end


def seances_to_ui_slots(seances: list[dict[str, Any]], cfg: Config | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i, s in enumerate(seances):
        a, b = _seance_time_labels(s, cfg)
        out.append({"id": -(i + 1), "start_time": a, "end_time": b, "is_active": 1})
    return out


async def fetch_seances_for_day(cfg: Config, day_yyyy_mm_dd: str) -> list[dict[str, Any]]:
    staff = int(cfg.yclients_default_staff_id)
    svc = service_ids_for_book(cfg)
    return await yclients_book_times(
        cfg,
        staff_id=staff,
        date_yyyy_mm_dd=day_yyyy_mm_dd,
        service_ids=svc if svc else None,
    )


async def load_day_seances_and_slots(
    cfg: Config, day_yyyy_mm_dd: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seances = await fetch_seances_for_day(cfg, day_yyyy_mm_dd)
    seances.sort(
        key=lambda s: _parse_hhmm(_seance_time_labels(s, cfg)[0]) or (0, 0),
    )
    return seances, seances_to_ui_slots(seances, cfg)


def slot_rows_for_day(
    data: dict[str, Any], cfg: Config, day: str
) -> list[dict[str, Any]] | None:
    """Если yclients_studio в state — взять слоты из seances, иначе None (звать SQLite)."""
    if not data.get("yclients_studio"):
        return None
    raw = data.get("yclients_seances")
    if not isinstance(raw, list):
        return None
    if not raw:
        return []
    se = [x for x in raw if isinstance(x, dict)]
    return seances_to_ui_slots(se, cfg) if se else []


def compute_billing(
    seances: list[dict[str, Any]],
    selected_neg_ids: list[int],
    hourly_rub: int,
    cfg: Config,
) -> tuple[int, int, str, str, float]:
    """
    total_rub, total_sec, slot_text, hours_label, hour_fraction (для подписи).
    """
    idxs = sorted(-int(sid) - 1 for sid in selected_neg_ids)
    if not idxs or idxs[0] < 0 or idxs[-1] >= len(seances):
        return 0, 0, "", "", 0.0
    chosen = [seances[i] for i in idxs]
    total_sec = sum(_seance_length_sec(s) for s in chosen)
    h_frac = total_sec / 3600.0
    total_rub = max(0, int(math.ceil(h_frac * max(0, int(hourly_rub)))))
    a0, _ = _seance_time_labels(chosen[0], cfg)
    _, b1 = _seance_time_labels(chosen[-1], cfg)
    slot_text = f"{a0} — {b1}"
    hlab = f"{h_frac:.1f}".rstrip("0").rstrip(".")
    return total_rub, total_sec, slot_text, f"{hlab} ч. экв.", h_frac


def datetime_sql_for_seance(cfg: Config, s: dict[str, Any], day: str) -> str:
    """Локальное время филиала — для POST /records."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(cfg.timezone)
    a, _ = _seance_time_labels(s, cfg)
    h, m = _parse_hhmm(a) or (0, 0)
    dtu = s.get("datetime")
    if isinstance(dtu, (int, float)):
        try:
            return datetime.fromtimestamp(int(dtu), tz=tz).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, OverflowError, ValueError):
            pass
    y, mo, d = (int(x) for x in day.split("-"))
    dt = datetime(y, mo, d, h, m, 0, tzinfo=tz)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def staff_id_for_seances(first: dict[str, Any], cfg: Config) -> int:
    for k in ("staff_id", "staff", "id"):
        v = first.get(k)
        if k == "staff" and isinstance(v, dict):
            v = v.get("id")
        try:
            n = int(v)  # type: ignore[arg-type]
            if n >= 0:
                return n
        except (TypeError, ValueError):
            pass
    return int(cfg.yclients_default_staff_id)


def first_service_payload(cfg: Config) -> list[dict[str, Any]]:
    ids = service_ids_for_book(cfg)
    if not ids:
        raise YclientsError("В .env укажите YCLIENTS_SERVICE_IDS (id услуги в Yclients), через запятую")
    return [{"id": int(ids[0]), "quantity": 1}]


async def available_days_in_window(cfg: Config) -> list[str]:
    """Все дни от сегодня до конца окна, где book_times непустой."""
    from database.db import Database

    end = Database.booking_window_end_date()
    start = date.today()
    out: list[str] = []
    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)

    sem = asyncio.Semaphore(4)

    async def one(d: date) -> str | None:
        day_s = d.isoformat()
        async with sem:
            try:
                se = await fetch_seances_for_day(cfg, day_s)
            except YclientsError as e:
                logger.debug("Yclients no day %s: %s", day_s, e)
                return None
        return day_s if se else None

    results = await asyncio.gather(*[one(d) for d in days])
    for r in results:
        if r:
            out.append(r)
    return out


def booking_time_bounds(
    seances: list[dict[str, Any]], selected_neg_ids: list[int], cfg: Config
) -> tuple[str, str]:
    """start_time, end_time для колонок bookings (первый/последний выбранный сеанс)."""
    idxs = sorted(-int(sid) - 1 for sid in selected_neg_ids)
    a0, _ = _seance_time_labels(seances[idxs[0]], cfg)
    _, b1 = _seance_time_labels(seances[idxs[-1]], cfg)
    return a0, b1


def selection_still_fresh(
    selected_neg_ids: list[int],
    before: list[dict[str, Any]],
    fresh: list[dict[str, Any]],
    cfg: Config,
) -> bool:
    if len(before) != len(fresh):
        return False
    idxs = sorted(-int(sid) - 1 for sid in selected_neg_ids)
    for i in idxs:
        if i < 0 or i >= len(before) or i >= len(fresh):
            return False
        a0, b0 = _seance_time_labels(before[i], cfg)
        a1, b1 = _seance_time_labels(fresh[i], cfg)
        if a0 != a1 or b0 != b1:
            return False
    return True


async def create_yclients_studio_record(
    cfg: Config,
    *,
    seances: list[dict[str, Any]],
    selected_neg_ids: list[int],
    day: str,
    client_name: str,
    client_phone_digits: str,
    api_id: str,
) -> tuple[int, dict[str, Any]]:
    idxs = sorted(-int(sid) - 1 for sid in selected_neg_ids)
    if not idxs or idxs[0] < 0 or any(i >= len(seances) for i in idxs):
        raise YclientsError("Слоты: данные устарели, откройте календарь снова")
    chosen = [seances[i] for i in idxs]
    seclen = sum(_seance_length_sec(s) for s in chosen)
    staff_id = staff_id_for_seances(chosen[0], cfg)
    dt_sql = datetime_sql_for_seance(cfg, chosen[0], day)
    services = first_service_payload(cfg)
    data = await yclients_create_record(
        cfg,
        staff_id=staff_id,
        services=services,
        client_phone=client_phone_digits,
        client_name=client_name,
        datetime_sql=dt_sql,
        seance_length_sec=seclen,
        api_id=api_id,
        comment="Запись из Telegram-бота",
    )
    rid = data.get("id")
    if rid is None and isinstance(data.get("record"), dict):
        rid = data["record"].get("id")
    try:
        rec_id = int(rid)  # type: ignore[arg-type]
    except (TypeError, ValueError) as e:
        raise YclientsError("create_record: в ответе нет id записи") from e
    return rec_id, data
