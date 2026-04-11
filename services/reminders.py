from __future__ import annotations

from datetime import datetime, timedelta

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from database.db import Database


class ReminderService:
    def __init__(self, scheduler: AsyncIOScheduler, db: Database, bot: Bot) -> None:
        self.scheduler = scheduler
        self.db = db
        self.bot = bot

    async def send_reminder(self, booking_id: int) -> None:
        booking = await self.db.get_booking_by_id(booking_id)
        if not booking or booking["status"] != "active":
            return
        text = (
            "Напоминаем, что вы записаны на студию звукозаписи в "
            f"{booking['start_time']}.\n"
            "Ждём вас ️"
        )
        await self.bot.send_message(booking["user_id"], text)

    async def schedule_for_booking(self, booking: dict) -> None:
        start_dt = self.db.booking_start_datetime(booking)
        remind_at = start_dt - timedelta(hours=24)
        if remind_at <= datetime.now():
            return

        job_id = f"booking_reminder_{booking['id']}"
        self.scheduler.add_job(
            self.send_reminder,
            trigger=DateTrigger(run_date=remind_at),
            args=[booking["id"]],
            id=job_id,
            replace_existing=True,
        )
        await self.db.save_reminder_job(booking["id"], job_id, remind_at)

    async def remove_for_booking(self, booking_id: int) -> None:
        job_id = f"booking_reminder_{booking_id}"
        try:
            self.scheduler.remove_job(job_id)
        except Exception:
            pass
        await self.db.delete_reminder_job(booking_id)

    async def restore_jobs(self) -> None:
        bookings = await self.db.get_all_active_bookings()
        for booking in bookings:
            if booking.get("booking_kind") in ("lyrics", "beat"):
                continue
            if booking.get("day") == "service":
                continue
            await self.schedule_for_booking(booking)

