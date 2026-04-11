import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    bot_token: str
    admin_id: int
    # Чат (группа или личка) для заявок на подтверждение оплаты. 0 — использовать admin_id.
    payments_chat_id: int
    channel_id: int
    channel_link: str
    schedule_channel_id: int
    payment_details: str
    timezone: str
    db_path: str

    # UI photos (local paths)
    main_menu_photo_path: str
    prices_photo_path: str
    # Картинка для экранов «Реквизиты / оплата» (текст/бит и запись на студию). Пусто — только текст.
    payment_photo_path: str
    # Тарифы: выбор ночь/день, затем отдельно ночной и дневной сценарии. Пусто — как обычный текст без смены фото.
    tariff_category_photo_path: str
    tariff_night_photo_path: str
    tariff_day_photo_path: str

    # Equipment screen
    equipment_title: str
    equipment_text: str
    microphone_name: str
    audiocard_name: str
    headphones_name: str
    monitors_name: str
    equipment_photos: list[str]

    # Makers (public usernames, for user receipts)
    textmaker_username: str
    beatmaker_username: str

    # Особые тарифы (пакеты 6 / 8 / 10 / 12 ч), руб — см. .env
    # Без звукорежиссёра
    tariff_night_6h: int
    tariff_night_8h: int
    tariff_night_10h: int
    tariff_night_12h: int
    tariff_day_6h: int
    tariff_day_8h: int
    tariff_day_10h: int
    tariff_day_12h: int
    # Со звукорежиссёром (отдельные суммы)
    tariff_night_6h_engineer: int
    tariff_night_8h_engineer: int
    tariff_night_10h_engineer: int
    tariff_night_12h_engineer: int
    tariff_day_6h_engineer: int
    tariff_day_8h_engineer: int
    tariff_day_10h_engineer: int
    tariff_day_12h_engineer: int


def payments_inbox_chat_id(cfg: Config) -> int:
    return cfg.payments_chat_id if cfg.payments_chat_id else cfg.admin_id


def _abs_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    return os.path.abspath(p)


def _int_env(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, str(default)) or str(default)).strip())
    except ValueError:
        return default


def tariff_price_rub(
    cfg: Config, *, night: bool, hours: int, with_engineer: bool = False
) -> int:
    """Цена пакета по ночному/дневному тарифу (6, 8, 10 или 12 ч)."""
    if hours not in (6, 8, 10, 12):
        return 0
    if with_engineer:
        if night:
            m = {
                6: cfg.tariff_night_6h_engineer,
                8: cfg.tariff_night_8h_engineer,
                10: cfg.tariff_night_10h_engineer,
                12: cfg.tariff_night_12h_engineer,
            }
        else:
            m = {
                6: cfg.tariff_day_6h_engineer,
                8: cfg.tariff_day_8h_engineer,
                10: cfg.tariff_day_10h_engineer,
                12: cfg.tariff_day_12h_engineer,
            }
    else:
        if night:
            m = {
                6: cfg.tariff_night_6h,
                8: cfg.tariff_night_8h,
                10: cfg.tariff_night_10h,
                12: cfg.tariff_night_12h,
            }
        else:
            m = {
                6: cfg.tariff_day_6h,
                8: cfg.tariff_day_8h,
                10: cfg.tariff_day_10h,
                12: cfg.tariff_day_12h,
            }
    return int(m[hours])


def _env_first(*keys: str) -> str:
    """Первое непустое значение среди имён переменных (разные варианты в .env)."""
    for k in keys:
        v = os.getenv(k, "").strip().strip("\ufeff")
        if v:
            return v
    return ""


def load_config() -> Config:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("BOT_TOKEN is not set")

    admin_id = int(os.getenv("ADMIN_ID", "0"))
    payments_chat_id = int(os.getenv("PAYMENTS_CHAT_ID", "0"))
    channel_id = int(os.getenv("CHANNEL_ID", "0"))
    channel_link = os.getenv("CHANNEL_LINK", "").strip() or "https://t.me/"
    schedule_channel_id = int(os.getenv("SCHEDULE_CHANNEL_ID", "0"))
    payment_details = os.getenv(
        "PAYMENT_DETAILS",
        "Перевод на карту: 0000 0000 0000 0000\nПолучатель: Studio Name",
    )
    timezone = os.getenv("TZ", "Europe/Moscow")
    db_path = os.getenv("DB_PATH", "studio_bot.db").strip()

    main_menu_photo_path = _abs_path(os.getenv("MAIN_MENU_PHOTO_PATH", ""))
    prices_photo_path = _abs_path(os.getenv("PRICES_PHOTO_PATH", ""))
    payment_photo_path = _abs_path(os.getenv("PAYMENT_PHOTO_PATH", ""))
    tariff_category_photo_path = _abs_path(os.getenv("TARIFF_CATEGORY_PHOTO_PATH", ""))
    tariff_night_photo_path = _abs_path(os.getenv("TARIFF_NIGHT_PHOTO_PATH", ""))
    tariff_day_photo_path = _abs_path(os.getenv("TARIFF_DAY_PHOTO_PATH", ""))

    equipment_title = os.getenv("EQUIPMENT_TITLE", "Оборудование и Фото Студии").strip()
    equipment_text = os.getenv(
        "EQUIPMENT_TEXT",
        "Наша студия звукозаписи — уютное пространство для записи.",
    ).strip()
    microphone_name = os.getenv("MICROPHONE_NAME", "Микрофон").strip()
    audiocard_name = os.getenv("AUDIACARD_NAME", "Аудиокарта").strip()
    headphones_name = os.getenv("HEADPHONES_NAME", "Наушники").strip()
    monitors_name = os.getenv("MONITORS_NAME", "Мониторы").strip()

    equipment_photos = [
        _abs_path(os.getenv("EQUIPMENT_PHOTO_MICROPHONE_PATH", "")),
        _abs_path(os.getenv("EQUIPMENT_PHOTO_AUDIOCARD_PATH", "")),
        _abs_path(os.getenv("EQUIPMENT_PHOTO_HEADPHONES_PATH", "")),
        _abs_path(os.getenv("EQUIPMENT_PHOTO_MONITORS_PATH", "")),
        _abs_path(os.getenv("EQUIPMENT_PHOTO_GENERAL_1_PATH", "")),
        _abs_path(os.getenv("EQUIPMENT_PHOTO_GENERAL_2_PATH", "")),
    ]
    equipment_photos = [p for p in equipment_photos if p]

    textmaker_username = _env_first(
        "TEXTMAKER_USERNAME",
        "TextMaker_Username",
        "TEXT_MAKER_USERNAME",
    )
    beatmaker_username = _env_first(
        "BEATMAKER_USERNAME",
        "BeatMaker_Username",
        "BEAT_MAKER_USERNAME",
    )

    # Тарифы без звукаря: по умолчанию N × HOURLY_BASE_NO_ENGINEER
    _base = _int_env("HOURLY_BASE_NO_ENGINEER", 1000)
    tariff_night_6h = _int_env("TARIFF_NIGHT_6H", 6 * _base)
    tariff_night_8h = _int_env("TARIFF_NIGHT_8H", 8 * _base)
    tariff_night_10h = _int_env("TARIFF_NIGHT_10H", 10 * _base)
    tariff_night_12h = _int_env("TARIFF_NIGHT_12H", 12 * _base)
    tariff_day_6h = _int_env("TARIFF_DAY_6H", 6 * _base)
    tariff_day_8h = _int_env("TARIFF_DAY_8H", 8 * _base)
    tariff_day_10h = _int_env("TARIFF_DAY_10H", 10 * _base)
    tariff_day_12h = _int_env("TARIFF_DAY_12H", 12 * _base)

    # Тарифы со звукорежиссёром: N × HOURLY_BASE_WITH_ENGINEER, если TARIFF_*_ENGINEER не заданы
    _base_we = _int_env("HOURLY_BASE_WITH_ENGINEER", 1800)
    tariff_night_6h_engineer = _int_env("TARIFF_NIGHT_6H_ENGINEER", 6 * _base_we)
    tariff_night_8h_engineer = _int_env("TARIFF_NIGHT_8H_ENGINEER", 8 * _base_we)
    tariff_night_10h_engineer = _int_env("TARIFF_NIGHT_10H_ENGINEER", 10 * _base_we)
    tariff_night_12h_engineer = _int_env("TARIFF_NIGHT_12H_ENGINEER", 12 * _base_we)
    tariff_day_6h_engineer = _int_env("TARIFF_DAY_6H_ENGINEER", 6 * _base_we)
    tariff_day_8h_engineer = _int_env("TARIFF_DAY_8H_ENGINEER", 8 * _base_we)
    tariff_day_10h_engineer = _int_env("TARIFF_DAY_10H_ENGINEER", 10 * _base_we)
    tariff_day_12h_engineer = _int_env("TARIFF_DAY_12H_ENGINEER", 12 * _base_we)

    return Config(
        bot_token=token,
        admin_id=admin_id,
        payments_chat_id=payments_chat_id,
        channel_id=channel_id,
        channel_link=channel_link,
        schedule_channel_id=schedule_channel_id,
        payment_details=payment_details,
        timezone=timezone,
        db_path=db_path,
        main_menu_photo_path=main_menu_photo_path,
        prices_photo_path=prices_photo_path,
        payment_photo_path=payment_photo_path,
        tariff_category_photo_path=tariff_category_photo_path,
        tariff_night_photo_path=tariff_night_photo_path,
        tariff_day_photo_path=tariff_day_photo_path,
        equipment_title=equipment_title,
        equipment_text=equipment_text,
        microphone_name=microphone_name,
        audiocard_name=audiocard_name,
        headphones_name=headphones_name,
        monitors_name=monitors_name,
        equipment_photos=equipment_photos,
        textmaker_username=textmaker_username,
        beatmaker_username=beatmaker_username,
        tariff_night_6h=tariff_night_6h,
        tariff_night_8h=tariff_night_8h,
        tariff_night_10h=tariff_night_10h,
        tariff_night_12h=tariff_night_12h,
        tariff_day_6h=tariff_day_6h,
        tariff_day_8h=tariff_day_8h,
        tariff_day_10h=tariff_day_10h,
        tariff_day_12h=tariff_day_12h,
        tariff_night_6h_engineer=tariff_night_6h_engineer,
        tariff_night_8h_engineer=tariff_night_8h_engineer,
        tariff_night_10h_engineer=tariff_night_10h_engineer,
        tariff_night_12h_engineer=tariff_night_12h_engineer,
        tariff_day_6h_engineer=tariff_day_6h_engineer,
        tariff_day_8h_engineer=tariff_day_8h_engineer,
        tariff_day_10h_engineer=tariff_day_10h_engineer,
        tariff_day_12h_engineer=tariff_day_12h_engineer,
    )

