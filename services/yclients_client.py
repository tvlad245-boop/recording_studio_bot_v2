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
    def _clean_token(raw: str) -> str:
        """
        Пользователи часто вставляют в .env не «чистый» токен, а кусок заголовка:
        'Bearer XXX, User YYY' или 'User YYY'. Здесь вычищаем префиксы и мусор.
        """
        s = (raw or "").strip().strip("\ufeff")
        # Неразрывные пробелы и прочий «копипаст» мусор
        s = s.replace("\u00a0", " ").replace("\u2007", " ").replace("\u202f", " ").strip()
        # Убрать кавычки вокруг значения
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            s = s[1:-1].strip()
        low = s.lower()
        if low.startswith("authorization:"):
            s = s.split(":", 1)[1].strip()
            low = s.lower()
        if low.startswith("bearer "):
            s = s[7:].strip()
            low = s.lower()
        if low.startswith("user "):
            s = s[5:].strip()
        return s

    p = _clean_token(cfg.yclients_partner_token or "")
    u = _clean_token(cfg.yclients_user_token or "")
    if p and u:
        auth = f"Bearer {p}, User {u}"
    elif p:
        auth = f"Bearer {p}"
    elif u:
        auth = f"Bearer {u}"
    else:
        return ""

    # httpx требует ASCII для заголовков; если в токен попали русские буквы — дадим понятную ошибку.
    try:
        auth.encode("ascii")
    except UnicodeEncodeError as e:
        raise YclientsError(
            "Токен(ы) Yclients содержат не-ASCII символы. "
            "В .env нужно вставлять только сам токен (латиница/цифры/символы), "
            "без слов 'Bearer'/'User' и без русских букв/кавычек."
        ) from e

    return auth


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
    # Короткий таймаут, чтобы админ-кнопка не «висела» бесконечно.
    timeout = httpx.Timeout(connect=8.0, read=8.0, write=8.0, pool=8.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url, headers=headers, params=params or None)
    except httpx.TimeoutException as e:
        raise YclientsError("Timeout при запросе в Yclients (проверьте доступ к api.yclients.com с сервера).") from e
    except httpx.HTTPError as e:
        raise YclientsError(f"HTTP ошибка при запросе в Yclients: {type(e).__name__}") from e

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
    if not (cfg.yclients_partner_token or cfg.yclients_user_token):
        raise YclientsError("Задайте YCLIENTS_PARTNER_TOKEN и/или YCLIENTS_USER_TOKEN (как для book_times)")

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
