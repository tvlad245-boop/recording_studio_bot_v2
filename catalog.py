"""Названия услуг и цены по умолчанию (до правок админа в БД)."""

SERVICE_CATALOG: dict[str, tuple[str, int]] = {
    "no_engineer": ("1 час без звукорежиссёра", 1000),
    "with_engineer": ("1 час с звукорежиссёром", 1800),
    "lyrics": ("Текст для вашей песни", 1000),
    "beat": ("Бит для песни", 2000),
}

# Обратная совместимость с keyboards / handlers
SERVICES = SERVICE_CATALOG
