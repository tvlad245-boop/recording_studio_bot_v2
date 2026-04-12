"""
Точка входа для окружений, где команда запуска задана как `python http_wrapper.py`
(типично Docker: WORKDIR /app → /app/http_wrapper.py).
Логика бота — в bot.main().
"""

from __future__ import annotations

import asyncio

from bot import main

if __name__ == "__main__":
    asyncio.run(main())
