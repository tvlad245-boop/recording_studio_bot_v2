"""
Клиент REST API Yclients (официальная схема v2).

Документация: https://developers.yclients.com/ru/

Авторизация для большинства операций с записями:
    Authorization: Bearer <partner_token>, User <user_token>

Для GET book_times в документации указан Bearer partner_token — при отсутствии partner
пробуем только user token (зависит от прав приложения в кабинете Yclients).

Переменные окружения см. config.Config — ключи никогда не хардкодить в репозитории.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from config import Config

logger = logging.getLogger(__name__)

YCLIENTS_API_BASE = "https://api.yclients.com/api/v1"
YCLIENTS_ACCEPT = "application/vnd.yclients.v2+json"


class YclientsError(Exception):
    """Ошибка ответа API или транспорта."""


def yclients_is_configured(cfg: Config) -> bool:
    return bool(cfg.yclients_company_id and (cfg.yclients_partner_token or cfg.yclients_user_token))


def _auth_header(cfg: Config) -> str:
    p = (cfg.yclients_partner_token or "").strip()
    u = (cfg.yclients_user_token or "").strip()
    if p and u:
        return f"Bearer {p}, User {u}"
    if p:
        return f"Bearer {p}"
    if u:
        return f"Bearer {u}"
    return ""


def _default_headers(cfg: Config) -> dict[str, str]:
    auth = _auth_header(cfg)
    if not auth:
        raise YclientsError("Не заданы YCLIENTS_PARTNER_TOKEN и/или YCLIENTS_USER_TOKEN")
    return {
        "Accept": YCLIENTS_ACCEPT,
        "Content-Type": "application/json",
        "Authorization": auth,
    }


def parse_service_ids_csv(csv: str) -> list[int]:
    out: list[int] = []
    for part in (csv or "").split(","):
        p = part.strip()
        if p.isdigit():
            out.append(int(p))
    return out


async def yclients_book_times(
    cfg: Config,
    *,
    staff_id: int,
    date_yyyy_mm_dd: str,
    service_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """
    GET /book_times/{company_id}/{staff_id}/{date}

    Возвращает список сеансов: time, seance_length (сек), datetime (unix или строка — как отдаёт API).
    """
    if not cfg.yclients_company_id:
        raise YclientsError("YCLIENTS_COMPANY_ID не задан")
    url = f"{YCLIENTS_API_BASE}/book_times/{int(cfg.yclients_company_id)}/{int(staff_id)}/{date_yyyy_mm_dd}"
    params: list[tuple[str, str]] = []
    if service_ids:
        for sid in service_ids:
            params.append(("service_ids[]", str(int(sid))))

    headers = _default_headers(cfg)
    async with httpx.AsyncClient(timeout=45.0) as client:
        r = await client.get(url, headers=headers, params=params or None)

    if r.status_code >= 400:
        logger.warning("yclients book_times HTTP %s: %s", r.status_code, r.text[:500])
        raise YclientsError(f"HTTP {r.status_code}: {r.text[:200]}")

    payload = r.json()
    if not payload.get("success"):
        raise YclientsError(str(payload)[:500])
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


async def yclients_create_record(
    cfg: Config,
    *,
    staff_id: int,
    services: list[dict[str, Any]],
    client_phone: str,
    client_name: str,
    datetime_sql: str,
    seance_length_sec: int,
    api_id: str,
    comment: str = "",
    save_if_busy: bool = False,
    send_sms: bool = False,
) -> dict[str, Any]:
    """
    POST /records/{company_id}

    datetime_sql — формат как в примере Yclients: "2019-01-01 17:00:00" (локальное время филиала).
    attendance: 2 — пользователь подтвердил (см. документацию), для онлайн-записи часто используют 0/2.
    """
    if not cfg.yclients_company_id:
        raise YclientsError("YCLIENTS_COMPANY_ID не задан")
    if not (cfg.yclients_partner_token and cfg.yclients_user_token):
        raise YclientsError("Для создания записи нужны оба токена: partner и user (см. документацию records)")

    url = f"{YCLIENTS_API_BASE}/records/{int(cfg.yclients_company_id)}"
    body: dict[str, Any] = {
        "staff_id": int(staff_id),
        "services": services,
        "client": {
            "phone": client_phone,
            "name": client_name,
            "surname": "",
            "patronymic": "",
            "email": "",
        },
        "save_if_busy": save_if_busy,
        "datetime": datetime_sql,
        "seance_length": int(seance_length_sec),
        "send_sms": send_sms,
        "comment": comment,
        "sms_remain_hours": 0,
        "email_remain_hours": 0,
        "attendance": 0,
        "api_id": api_id,
    }
    headers = _default_headers(cfg)
    async with httpx.AsyncClient(timeout=45.0) as client:
        r = await client.post(url, headers=headers, json=body)

    if r.status_code not in (200, 201):
        logger.warning("yclients create_record HTTP %s: %s", r.status_code, r.text[:800])
        raise YclientsError(f"HTTP {r.status_code}: {r.text[:300]}")

    payload = r.json()
    if not payload.get("success"):
        raise YclientsError(str(payload)[:800])
    data = payload.get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict):
        return data
    raise YclientsError("Неожиданный формат ответа create_record")
