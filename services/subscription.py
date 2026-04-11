from __future__ import annotations

import logging
from aiogram import Bot


async def is_subscribed(bot: Bot, channel_id: int, user_id: int) -> bool:
    if not channel_id:
        return True
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
    except Exception:
        logging.exception("getChatMember failed (channel_id=%s user_id=%s)", channel_id, user_id)
        return False

    # Обычно достаточно is_member; если нет — проверим status.
    if hasattr(member, "is_member"):
        try:
            return bool(getattr(member, "is_member"))
        except Exception:
            pass
    status = getattr(member, "status", None)
    return status not in {None, "left", "kicked", "banned"}

