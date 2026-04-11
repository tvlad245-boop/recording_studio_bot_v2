from __future__ import annotations

from typing import Any, Awaitable, Callable

from aiogram import Router
from aiogram.types import TelegramObject

from config import Config
from database.db import Database
from services.effective_pricing import load_effective_pricing


class PricingMiddleware:
    """Подмешивает актуальные цены из bot_settings в data['pricing']."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        db: Database | None = data.get("db")
        cfg: Config | None = data.get("config")
        if db is not None and cfg is not None:
            data["pricing"] = await load_effective_pricing(db, cfg)
        return await handler(event, data)


def register_pricing_middleware(router: Router) -> None:
    router.message.middleware(PricingMiddleware())
    router.callback_query.middleware(PricingMiddleware())
