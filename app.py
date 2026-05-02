"""
ASGI + script entrypoint for hosting platforms.

- When imported by uvicorn: exposes `app` (FastAPI) for webhooks.
- When executed as a script: starts Telegram bot long-polling (aiogram).

This fits platforms that have one "main file" but run BOTH:
1) an HTTP server (uvicorn) and
2) a bot process.
"""

from __future__ import annotations

import asyncio

from yookassa_webhook import app  # FastAPI app (YooKassa + Yclients)


def _run_bot() -> None:
    # Import lazily to avoid circular imports at module import time.
    from bot import main as bot_main

    asyncio.run(bot_main())


if __name__ == "__main__":
    _run_bot()

