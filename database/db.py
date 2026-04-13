from __future__ import annotations

import re
from calendar import monthrange

import aiosqlite
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
import json


def default_hourly_slots() -> list[tuple[str, str]]:
    """24 слота по часу: 00:00–01:00 … 23:00–00:00 (конец суток — 00:00 следующего дня)."""
    slots: list[tuple[str, str]] = []
    for h in range(24):
        start = f"{h:02d}:00"
        end_h = (h + 1) % 24
        end = f"{end_h:02d}:00"
        slots.append((start, end))
    return slots


DEFAULT_SLOTS: list[tuple[str, str]] = default_hourly_slots()


@dataclass(frozen=True)
class Booking:
    id: int
    user_id: int
    user_name: str
    phone: str
    day: str
    start_time: str
    end_time: str
    services: str
    total_price: int
    status: str


_TIME_HHMM_RE = re.compile(r"(\d{1,2})\s*:\s*(\d{2})")


class Database:
    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> aiosqlite.Connection:
        return aiosqlite.connect(self.path)

    @staticmethod
    def _configure(db: aiosqlite.Connection) -> None:
        db.row_factory = aiosqlite.Row

    async def init(self) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS work_days (
                    day TEXT PRIMARY KEY,
                    is_closed INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS time_slots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(day, start_time, end_time)
                );

                CREATE TABLE IF NOT EXISTS bookings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    day TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    services TEXT NOT NULL,
                    total_price INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    booked_slot_ids TEXT,
                    booking_kind TEXT DEFAULT 'studio',
                    notes TEXT,
                    tg_username TEXT,
                    requires_engineer INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_bookings_user_status
                ON bookings(user_id, status);

                CREATE INDEX IF NOT EXISTS idx_bookings_day_time
                ON bookings(day, start_time, end_time, status);

                CREATE TABLE IF NOT EXISTS reminder_jobs (
                    booking_id INTEGER PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    run_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_messages (
                    key TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_activity_message (
                    user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    body_html TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                -- ЮKassa: хранение связи payment_id → booking_id/user_id (переживает перезапуски)
                CREATE TABLE IF NOT EXISTS yookassa_payment_links (
                    payment_id TEXT PRIMARY KEY,
                    booking_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );

                -- Идемпотентность webhook: если событие пришло повторно, не обрабатываем второй раз
                CREATE TABLE IF NOT EXISTS yookassa_processed_payments (
                    payment_id TEXT PRIMARY KEY,
                    processed_at TEXT NOT NULL
                );
                """
            )
            await db.commit()
            await self._migrate_bookings_booked_slot_ids()
            await self._migrate_bookings_kind_notes()
            await self._migrate_bookings_tg_username()
            await self._migrate_bookings_requires_engineer()
            await self._migrate_bookings_client_cleanup()
            await self._migrate_bookings_pending_meta()

    async def _migrate_bookings_booked_slot_ids(self) -> None:
        async with self.connect() as db:
            self._configure(db)
            try:
                await db.execute("ALTER TABLE bookings ADD COLUMN booked_slot_ids TEXT")
                await db.commit()
            except aiosqlite.OperationalError:
                pass

    async def _migrate_bookings_kind_notes(self) -> None:
        async with self.connect() as db:
            self._configure(db)
            for stmt in (
                "ALTER TABLE bookings ADD COLUMN booking_kind TEXT DEFAULT 'studio'",
                "ALTER TABLE bookings ADD COLUMN notes TEXT",
            ):
                try:
                    await db.execute(stmt)
                    await db.commit()
                except aiosqlite.OperationalError:
                    pass

    async def _migrate_bookings_tg_username(self) -> None:
        async with self.connect() as db:
            self._configure(db)
            try:
                await db.execute("ALTER TABLE bookings ADD COLUMN tg_username TEXT")
                await db.commit()
            except aiosqlite.OperationalError:
                pass

    async def _migrate_bookings_requires_engineer(self) -> None:
        async with self.connect() as db:
            self._configure(db)
            try:
                await db.execute("ALTER TABLE bookings ADD COLUMN requires_engineer INTEGER NOT NULL DEFAULT 0")
                await db.commit()
            except aiosqlite.OperationalError:
                pass

    async def _migrate_bookings_client_cleanup(self) -> None:
        """JSON: {\"chat_id\": int, \"root\": int | null, \"extra\": [message_id, ...]} — удалить после подтверждения оплаты."""
        async with self.connect() as db:
            self._configure(db)
            try:
                await db.execute("ALTER TABLE bookings ADD COLUMN client_cleanup_json TEXT")
                await db.commit()
            except aiosqlite.OperationalError:
                pass

    async def _migrate_bookings_pending_meta(self) -> None:
        async with self.connect() as db:
            self._configure(db)
            try:
                await db.execute("ALTER TABLE bookings ADD COLUMN pending_meta TEXT")
                await db.commit()
            except aiosqlite.OperationalError:
                pass

    async def update_booking_client_cleanup(self, booking_id: int, payload: str | None) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                "UPDATE bookings SET client_cleanup_json = ? WHERE id = ?",
                (payload, booking_id),
            )
            await db.commit()

    async def get_bot_message(self, key: str) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute("SELECT key, chat_id, message_id FROM bot_messages WHERE key = ?", (key,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def upsert_bot_message(self, key: str, chat_id: int, message_id: int) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                "INSERT OR REPLACE INTO bot_messages(key, chat_id, message_id) VALUES (?, ?, ?)",
                (key, int(chat_id), int(message_id)),
            )
            await db.commit()

    async def delete_bot_message(self, key: str) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("DELETE FROM bot_messages WHERE key = ?", (key,))
            await db.commit()

    async def clear_schedule_channel_bot_messages(self) -> None:
        """Сброс привязки к постам в канале (расписание + задачи). Нужен при смене ID канала или «битом» message_id."""
        async with self.connect() as db:
            self._configure(db)
            for k in ("schedule_week_7d", "tasks_lyrics", "tasks_beat"):
                await db.execute("DELETE FROM bot_messages WHERE key = ?", (k,))
            await db.commit()

    # --- ЮKassa (персистентность + идемпотентность) ---

    async def upsert_yookassa_payment_link(self, payment_id: str, booking_id: int, user_id: int) -> None:
        """payment_id → booking_id/user_id (создаётся при Payment.create)."""
        pid = (payment_id or "").strip()
        if not pid:
            return
        async with self.connect() as db:
            self._configure(db)
            now = datetime.utcnow().isoformat(timespec="seconds")
            await db.execute(
                """
                INSERT OR REPLACE INTO yookassa_payment_links(payment_id, booking_id, user_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (pid, int(booking_id), int(user_id), now),
            )
            await db.commit()

    async def get_yookassa_payment_link(self, payment_id: str) -> dict[str, Any] | None:
        pid = (payment_id or "").strip()
        if not pid:
            return None
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                "SELECT payment_id, booking_id, user_id, created_at FROM yookassa_payment_links WHERE payment_id = ?",
                (pid,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def delete_yookassa_payment_link(self, payment_id: str) -> None:
        pid = (payment_id or "").strip()
        if not pid:
            return
        async with self.connect() as db:
            self._configure(db)
            await db.execute("DELETE FROM yookassa_payment_links WHERE payment_id = ?", (pid,))
            await db.commit()

    async def mark_yookassa_payment_processed(self, payment_id: str) -> bool:
        """
        Атомарно помечает payment_id как обработанный.
        Возвращает True, если это первая обработка; False — если уже было обработано раньше.
        """
        pid = (payment_id or "").strip()
        if not pid:
            return False
        async with self.connect() as db:
            self._configure(db)
            now = datetime.utcnow().isoformat(timespec="seconds")
            try:
                await db.execute(
                    "INSERT INTO yookassa_processed_payments(payment_id, processed_at) VALUES (?, ?)",
                    (pid, now),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def get_user_activity_message(self, user_id: int) -> dict[str, Any] | None:
        """Одно «липкое» сообщение пользователя: накопление успешных заявок."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                "SELECT user_id, chat_id, message_id, body_html FROM user_activity_message WHERE user_id = ?",
                (int(user_id),),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def upsert_user_activity_message(
        self, user_id: int, chat_id: int, message_id: int, body_html: str
    ) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                """
                INSERT OR REPLACE INTO user_activity_message(user_id, chat_id, message_id, body_html)
                VALUES (?, ?, ?, ?)
                """,
                (int(user_id), int(chat_id), int(message_id), body_html),
            )
            await db.commit()

    async def delete_user_activity_message(self, user_id: int) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("DELETE FROM user_activity_message WHERE user_id = ?", (int(user_id),))
            await db.commit()

    async def get_all_settings(self) -> dict[str, str]:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute("SELECT key, value FROM bot_settings")
            rows = await cur.fetchall()
            return {str(r["key"]): str(r["value"]) for r in rows}

    async def set_setting(self, key: str, value: str) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                "INSERT OR REPLACE INTO bot_settings(key, value) VALUES (?, ?)",
                (key, value),
            )
            await db.commit()

    async def ensure_settings_defaults(self, defaults: dict[str, str]) -> None:
        """Добавляет только отсутствующие ключи (не перезаписывает админские правки)."""
        async with self.connect() as db:
            self._configure(db)
            for k, v in defaults.items():
                await db.execute(
                    "INSERT OR IGNORE INTO bot_settings(key, value) VALUES (?, ?)",
                    (k, v),
                )
            await db.commit()

    async def get_active_service_orders(self, kind: str) -> list[dict[str, Any]]:
        """Текст/бит для блока в канале: активные и ожидающие оплаты (до подтверждения админом)."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT *
                FROM bookings
                WHERE booking_kind = ? AND status IN ('active', 'pending_payment')
                ORDER BY id DESC
                """,
                (kind,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def complete_service_order(self, booking_id: int, kind: str) -> dict[str, Any] | None:
        """Активная заявка текст/бит → completed (кнопка «Выполнено» в канале)."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT * FROM bookings
                WHERE id = ? AND status = 'active' AND booking_kind = ?
                """,
                (booking_id, kind),
            )
            row = await cur.fetchone()
            if not row:
                return None
            await db.execute(
                "UPDATE bookings SET status = 'completed' WHERE id = ? AND status = 'active'",
                (booking_id,),
            )
            await db.commit()
            cur2 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            r2 = await cur2.fetchone()
            return dict(r2) if r2 else None

    async def seed_days(self, days_ahead: int) -> None:
        """
        Создаёт рабочие дни и дефолтные слоты на N дней вперёд (включая сегодня).
        """
        if days_ahead < 0:
            days_ahead = 0
        for i in range(days_ahead + 1):
            d = (date.today() + timedelta(days=i)).isoformat()
            await self.add_work_day(d)

    async def add_work_day(self, day: str) -> None:
        """
        Создаёт рабочий день и почасовые слоты.
        Если день не закрыт и нет активных броней — пересобирает слоты под почасовую сетку
        (миграция со старых 3-часовых блоков).
        """
        async with self.connect() as db:
            self._configure(db)
            await db.execute("INSERT OR IGNORE INTO work_days(day, is_closed) VALUES (?, 0)", (day,))
            cur = await db.execute("SELECT is_closed FROM work_days WHERE day = ?", (day,))
            row = await cur.fetchone()
            if row is not None and row["is_closed"]:
                await db.commit()
                return

            cur = await db.execute(
                """
                SELECT 1 FROM bookings
                WHERE day = ? AND status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                LIMIT 1
                """,
                (day,),
            )
            if await cur.fetchone() is not None:
                await db.commit()
                return

            await db.execute("DELETE FROM time_slots WHERE day = ?", (day,))
            for start_time, end_time in DEFAULT_SLOTS:
                await db.execute(
                    """
                    INSERT INTO time_slots(day, start_time, end_time, is_active)
                    VALUES (?, ?, ?, 1)
                    """,
                    (day, start_time, end_time),
                )
            await db.commit()

    async def close_day(self, day: str) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("INSERT OR IGNORE INTO work_days(day, is_closed) VALUES (?, 1)", (day,))
            await db.execute("UPDATE work_days SET is_closed = 1 WHERE day = ?", (day,))
            await db.execute("UPDATE time_slots SET is_active = 0 WHERE day = ?", (day,))
            await db.commit()

    async def is_work_day_closed(self, day: str) -> bool:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute("SELECT is_closed FROM work_days WHERE day = ?", (day,))
            row = await cur.fetchone()
            if row is None:
                return False
            return bool(int(row["is_closed"] or 0))

    async def get_closed_days_in_month(self, year: int, month: int) -> set[str]:
        """Дни месяца, закрытые админом (work_days.is_closed = 1)."""
        _, dim = monthrange(year, month)
        start = date(year, month, 1).isoformat()
        end = date(year, month, dim).isoformat()
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                "SELECT day FROM work_days WHERE is_closed = 1 AND day >= ? AND day <= ?",
                (start, end),
            )
            rows = await cur.fetchall()
            return {str(r["day"]) for r in rows}

    async def open_work_day(self, day: str) -> None:
        """Снять закрытие дня и восстановить почасовые слоты (если нет броней)."""
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                "INSERT OR IGNORE INTO work_days(day, is_closed) VALUES (?, 0)",
                (day,),
            )
            await db.execute("UPDATE work_days SET is_closed = 0 WHERE day = ?", (day,))
            await db.commit()
        await self.add_work_day(day)

    async def day_has_active_studio_bookings(self, day: str) -> bool:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT 1 FROM bookings
                WHERE day = ?
                  AND status IN (
                      'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                  )
                  AND (booking_kind IS NULL OR booking_kind = 'studio')
                LIMIT 1
                """,
                (day,),
            )
            return await cur.fetchone() is not None

    async def add_slot(self, day: str, start_time: str, end_time: str) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("INSERT OR IGNORE INTO work_days(day, is_closed) VALUES (?, 0)", (day,))
            await db.execute(
                """
                INSERT OR REPLACE INTO time_slots(day, start_time, end_time, is_active)
                VALUES (?, ?, ?, 1)
                """,
                (day, start_time, end_time),
            )
            await db.commit()

    async def remove_slot(self, day: str, start_time: str) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                "DELETE FROM time_slots WHERE day = ? AND start_time = ?",
                (day, start_time),
            )
            await db.commit()

    async def get_available_days(self, *, days_ahead: int = 60) -> list[str]:
        async with self.connect() as db:
            self._configure(db)
            if days_ahead < 0:
                days_ahead = 0
            until = f"+{days_ahead} day"
            cur = await db.execute(
                """
                SELECT DISTINCT ts.day
                FROM time_slots ts
                JOIN work_days wd ON wd.day = ts.day
                WHERE wd.is_closed = 0
                  AND ts.is_active = 1
                  AND date(ts.day) BETWEEN date('now') AND date('now', ?)
                ORDER BY ts.day
                """,
                (until,),
            )
            rows = await cur.fetchall()
            return [r["day"] for r in rows]

    async def get_available_slots(self, day: str) -> list[dict[str, Any]]:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT id, start_time, end_time
                FROM time_slots
                WHERE day = ? AND is_active = 1
                ORDER BY start_time
                """,
                (day,),
            )
            rows = await cur.fetchall()
            out = [dict(r) for r in rows]
            out.sort(key=lambda s: Database.time_sort_key(str(s["start_time"])))
            return out

    async def get_all_slots_for_day(self, day: str) -> list[dict[str, Any]]:
        """Все слоты дня (для экрана выбора: ✅ свободен / ❌ занят)."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT id, start_time, end_time, is_active
                FROM time_slots
                WHERE day = ?
                ORDER BY start_time
                """,
                (day,),
            )
            rows = await cur.fetchall()
            out = [dict(r) for r in rows]
            # ORDER BY start_time в SQLite лексикографический: "10:00" < "9:00" — ломает «часы подряд».
            out.sort(key=lambda s: Database.time_sort_key(str(s["start_time"])))
            return out

    async def get_slot_by_id(self, slot_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute("SELECT * FROM time_slots WHERE id = ?", (slot_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def user_has_active_booking(self, user_id: int) -> bool:
        """Любая активная заявка (студия или услуга), включая ожидание оплаты."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT 1 FROM bookings
                WHERE user_id = ? AND status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                LIMIT 1
                """,
                (user_id,),
            )
            return await cur.fetchone() is not None

    async def user_has_active_studio_booking(self, user_id: int) -> bool:
        """Активная запись на студию или ожидание подтверждения оплаты."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT 1 FROM bookings
                WHERE user_id = ? AND status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                  AND (booking_kind IS NULL OR booking_kind = 'studio')
                LIMIT 1
                """,
                (user_id,),
            )
            return await cur.fetchone() is not None

    async def get_user_active_bookings(self, user_id: int) -> list[dict[str, Any]]:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT * FROM bookings
                WHERE user_id = ? AND status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                ORDER BY id DESC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_user_active_booking(self, user_id: int) -> dict[str, Any] | None:
        rows = await self.get_user_active_bookings(user_id)
        return rows[0] if rows else None

    async def get_booking_by_id(self, booking_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    @staticmethod
    def normalize_time_str(t: str) -> str:
        """HH:MM с ведущими нулями (исправляет сравнение 9:00 vs 09:00)."""
        raw = Database._coerce_cell_str(t)
        if not raw:
            return "00:00"
        m = _TIME_HHMM_RE.search(raw)
        if m:
            h = int(m.group(1)) % 24
            mm = max(0, min(59, int(m.group(2))))
            return f"{h:02d}:{mm:02d}"
        parts = raw.split(":")
        h = int(parts[0]) % 24
        mm = int(parts[1]) if len(parts) > 1 else 0
        mm = max(0, min(59, mm))
        return f"{h:02d}:{mm:02d}"

    @staticmethod
    def time_sort_key(t: str) -> tuple[int, int]:
        t = Database.normalize_time_str(str(t))
        hh, mm = t.split(":")
        return int(hh), int(mm)

    @staticmethod
    def slot_row_is_active(val: Any) -> bool:
        """SQLite может отдать is_active как 0/1, bool или строку."""
        if val is None:
            return False
        try:
            return int(val) == 1
        except (TypeError, ValueError):
            return str(val).strip().lower() in ("1", "true", "yes")

    @staticmethod
    def _coerce_cell_str(val: Any) -> str:
        if isinstance(val, (bytes, bytearray)):
            s = val.decode("utf-8", errors="replace")
        else:
            s = str(val or "")
        s = s.strip()
        for ch in ("–", "—", "−"):
            s = s.replace(ch, "-")
        return s.strip()

    @staticmethod
    def slots_selection_is_contiguous_block(
        all_slots_chronological: list[dict[str, Any]],
        selected_ids: set[int],
    ) -> bool:
        """
        Выбранные слоты — один непрерывный блок в сетке дня (соседние строки расписания).
        Надёжнее сравнения строк времени (нет расхождений 9:00/09:00, «00:00» и т.д.).
        """
        if not selected_ids:
            return False
        sel = {int(x) for x in selected_ids}
        indices = [i for i, s in enumerate(all_slots_chronological) if int(s["id"]) in sel]
        if len(indices) != len(sel):
            return False
        indices.sort()
        for j in range(len(indices) - 1):
            if indices[j + 1] != indices[j] + 1:
                return False
        return True

    @staticmethod
    def selection_is_valid_multihour_slot_chain(
        all_slots_chronological: list[dict[str, Any]],
        selected_ids: set[int],
        chosen_slots: list[dict[str, Any]],
    ) -> bool:
        """
        Допустимая цепочка часов, если выполняется хотя бы одно:
        — слоты подряд в полном списке дня (индексы);
        — или конец/начало времён совпадают по цепочке (после нормализации).
        """
        if not selected_ids:
            return False
        sel = {int(x) for x in selected_ids}
        if len(chosen_slots) != len(sel):
            return False
        if Database.slots_selection_is_contiguous_block(all_slots_chronological, sel):
            return True
        ordered = sorted(
            chosen_slots,
            key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"])),
        )
        return Database._hourly_slots_consecutive(ordered)

    @staticmethod
    def _hourly_slots_consecutive(slots_ordered: list[dict[str, Any]]) -> bool:
        if len(slots_ordered) < 1:
            return False
        for i in range(len(slots_ordered) - 1):
            a_end = Database.normalize_time_str(Database._coerce_cell_str(slots_ordered[i]["end_time"]))
            b_start = Database.normalize_time_str(Database._coerce_cell_str(slots_ordered[i + 1]["start_time"]))
            if a_end != b_start:
                return False
        return True

    async def create_booking(
        self,
        *,
        user_id: int,
        user_name: str,
        phone: str,
        tg_username: str | None,
        requires_engineer: bool,
        slot_ids: list[int],
        services: str,
        total_price: int,
        status: str = "active",
    ) -> int | None:
        """
        Создаёт бронь на один или несколько подряд идущих слотов (часов).
        """
        if not slot_ids:
            return None
        async with self.connect() as db:
            self._configure(db)
            await db.execute("BEGIN IMMEDIATE")

            placeholders = ",".join("?" * len(slot_ids))
            cur = await db.execute(
                f"SELECT * FROM time_slots WHERE id IN ({placeholders}) AND is_active = 1",
                tuple(slot_ids),
            )
            rows = await cur.fetchall()
            slots = [dict(r) for r in rows]
            if len(slots) != len(slot_ids):
                await db.rollback()
                return None

            slots.sort(key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"])))
            day = slots[0]["day"]
            if any(s["day"] != day for s in slots):
                await db.rollback()
                return None

            cur_all = await db.execute(
                "SELECT id, start_time, end_time, day FROM time_slots WHERE day = ?",
                (day,),
            )
            all_day = [dict(r) for r in await cur_all.fetchall()]
            all_day.sort(key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"])))
            if not Database.selection_is_valid_multihour_slot_chain(
                all_day, set(int(x) for x in slot_ids), slots
            ):
                await db.rollback()
                return None

            cur = await db.execute(
                """
                SELECT 1 FROM bookings
                WHERE user_id = ? AND status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                  AND (booking_kind IS NULL OR booking_kind = 'studio')
                LIMIT 1
                """,
                (user_id,),
            )
            if await cur.fetchone():
                await db.rollback()
                return None

            start_time = slots[0]["start_time"]
            end_time = slots[-1]["end_time"]
            ids_csv = ",".join(str(int(s["id"])) for s in slots)

            booking_cur = await db.execute(
                """
                INSERT INTO bookings(
                    user_id, user_name, phone, day, start_time, end_time, services, total_price, status, created_at,
                    booked_slot_ids, booking_kind, notes, tg_username, requires_engineer
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'studio', NULL, ?, ?)
                """,
                (
                    user_id,
                    user_name,
                    phone,
                    day,
                    start_time,
                    end_time,
                    services,
                    total_price,
                    status,
                    datetime.utcnow().isoformat(),
                    ids_csv,
                    tg_username,
                    1 if requires_engineer else 0,
                ),
            )

            for s in slots:
                cur2 = await db.execute(
                    "UPDATE time_slots SET is_active = 0 WHERE id = ? AND is_active = 1",
                    (s["id"],),
                )
                if cur2.rowcount != 1:
                    await db.rollback()
                    return None

            await db.commit()
            return int(booking_cur.lastrowid)

    async def create_service_order(
        self,
        *,
        user_id: int,
        user_name: str,
        phone: str,
        tg_username: str | None,
        product: str,
        services_label: str,
        total_price: int,
        notes: str,
        status: str = "active",
    ) -> int:
        """Заявка на текст/бит без слотов календаря (booking_kind = lyrics | beat)."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                INSERT INTO bookings(
                    user_id, user_name, phone, day, start_time, end_time,
                    services, total_price, status, created_at,
                    booked_slot_ids, booking_kind, notes, tg_username
                )
                VALUES (?, ?, ?, 'service', '—', '—', ?, ?, ?, ?, NULL, ?, ?, ?)
                """,
                (
                    user_id,
                    user_name,
                    phone,
                    services_label,
                    total_price,
                    status,
                    datetime.utcnow().isoformat(),
                    product,
                    notes,
                    tg_username,
                ),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def cancel_booking(self, booking_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("BEGIN IMMEDIATE")
            cur = await db.execute(
                """
                SELECT * FROM bookings WHERE id = ? AND status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                """,
                (booking_id,),
            )
            row = await cur.fetchone()
            if not row:
                await db.rollback()
                return None
            booking = dict(row)
            await db.execute(
                "UPDATE bookings SET status = 'cancelled', pending_meta = NULL WHERE id = ?",
                (booking_id,),
            )
            kind = booking.get("booking_kind") or "studio"
            if kind in ("lyrics", "beat"):
                await db.execute("DELETE FROM reminder_jobs WHERE booking_id = ?", (booking_id,))
                await db.commit()
                return booking
            ids_str = booking.get("booked_slot_ids")
            if ids_str:
                for part in str(ids_str).split(","):
                    part = part.strip()
                    if not part:
                        continue
                    await db.execute(
                        "UPDATE time_slots SET is_active = 1 WHERE id = ?",
                        (int(part),),
                    )
            else:
                await db.execute(
                    """
                    UPDATE time_slots
                    SET is_active = 1
                    WHERE day = ? AND start_time = ? AND end_time = ?
                    """,
                    (booking["day"], booking["start_time"], booking["end_time"]),
                )
            await db.execute("DELETE FROM reminder_jobs WHERE booking_id = ?", (booking_id,))
            await db.commit()
            return booking

    async def confirm_booking_payment(self, booking_id: int) -> dict[str, Any] | None:
        """pending_payment → active. Возвращает строку брони или None."""
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                "SELECT * FROM bookings WHERE id = ? AND status = 'pending_payment'",
                (booking_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            await db.execute(
                "UPDATE bookings SET status = 'active' WHERE id = ? AND status = 'pending_payment'",
                (booking_id,),
            )
            await db.commit()
            cur2 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            row2 = await cur2.fetchone()
            return dict(row2) if row2 else None

    async def request_user_cancellation(self, booking_id: int, user_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT * FROM bookings WHERE id = ? AND user_id = ?
                  AND status IN ('active', 'pending_payment')
                  AND (
                    booking_kind IS NULL OR booking_kind = 'studio'
                    OR booking_kind IN ('lyrics', 'beat')
                  )
                """,
                (booking_id, user_id),
            )
            row = await cur.fetchone()
            if not row:
                return None
            b = dict(row)
            meta = json.dumps({"prev": b["status"]})
            await db.execute(
                "UPDATE bookings SET status = 'pending_cancel', pending_meta = ? WHERE id = ?",
                (meta, booking_id),
            )
            await db.commit()
            cur2 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            r2 = await cur2.fetchone()
            return dict(r2) if r2 else None

    async def reject_user_cancellation(self, booking_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                "SELECT * FROM bookings WHERE id = ? AND status = 'pending_cancel'",
                (booking_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            b = dict(row)
            prev = "active"
            try:
                m = json.loads(b.get("pending_meta") or "{}")
                p = m.get("prev", "active")
                if p in ("active", "pending_payment"):
                    prev = p
            except (json.JSONDecodeError, TypeError):
                pass
            await db.execute(
                "UPDATE bookings SET status = ?, pending_meta = NULL WHERE id = ?",
                (prev, booking_id),
            )
            await db.commit()
            cur2 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            r2 = await cur2.fetchone()
            return dict(r2) if r2 else None

    async def reject_user_reschedule(self, booking_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                "SELECT * FROM bookings WHERE id = ? AND status = 'pending_reschedule'",
                (booking_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            await db.execute(
                "UPDATE bookings SET status = 'active', pending_meta = NULL WHERE id = ?",
                (booking_id,),
            )
            await db.commit()
            cur2 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            r2 = await cur2.fetchone()
            return dict(r2) if r2 else None

    async def request_user_reschedule(
        self,
        booking_id: int,
        user_id: int,
        *,
        new_day: str,
        new_slot_ids: list[int],
    ) -> dict[str, Any] | None:
        if not new_slot_ids:
            return None
        async with self.connect() as db:
            self._configure(db)
            await db.execute("BEGIN IMMEDIATE")
            cur = await db.execute(
                """
                SELECT * FROM bookings WHERE id = ? AND user_id = ? AND status = 'active'
                  AND (booking_kind IS NULL OR booking_kind = 'studio')
                """,
                (booking_id, user_id),
            )
            row = await cur.fetchone()
            if not row:
                await db.rollback()
                return None

            placeholders = ",".join("?" * len(new_slot_ids))
            cur2 = await db.execute(
                f"SELECT * FROM time_slots WHERE id IN ({placeholders}) AND is_active = 1",
                tuple(int(x) for x in new_slot_ids),
            )
            rows = [dict(r) for r in await cur2.fetchall()]
            if len(rows) != len(new_slot_ids):
                await db.rollback()
                return None
            rows.sort(
                key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"]))
            )
            if any(s["day"] != new_day for s in rows):
                await db.rollback()
                return None
            cur_all = await db.execute(
                "SELECT id, start_time, end_time, day FROM time_slots WHERE day = ?",
                (new_day,),
            )
            all_day = [dict(r) for r in await cur_all.fetchall()]
            all_day.sort(
                key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"]))
            )
            if not Database.selection_is_valid_multihour_slot_chain(
                all_day, set(int(x) for x in new_slot_ids), rows
            ):
                await db.rollback()
                return None

            slot_text = f"{rows[0]['start_time']} — {rows[-1]['end_time']}"
            meta = json.dumps(
                {
                    "new_day": new_day,
                    "new_slot_ids": new_slot_ids,
                    "slot_text": slot_text,
                }
            )
            await db.execute(
                "UPDATE bookings SET status = 'pending_reschedule', pending_meta = ? WHERE id = ?",
                (meta, booking_id),
            )
            await db.commit()
            cur3 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            r3 = await cur3.fetchone()
            return dict(r3) if r3 else None

    async def approve_user_reschedule(self, booking_id: int) -> dict[str, Any] | None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("BEGIN IMMEDIATE")
            cur = await db.execute(
                "SELECT * FROM bookings WHERE id = ? AND status = 'pending_reschedule'",
                (booking_id,),
            )
            row = await cur.fetchone()
            if not row:
                await db.rollback()
                return None
            booking = dict(row)
            try:
                meta = json.loads(booking.get("pending_meta") or "{}")
            except (json.JSONDecodeError, TypeError):
                await db.rollback()
                return None
            new_day = meta.get("new_day")
            new_slot_ids = meta.get("new_slot_ids")
            if not new_day or not new_slot_ids or not isinstance(new_slot_ids, list):
                await db.rollback()
                return None
            new_slot_ids = [int(x) for x in new_slot_ids]
            placeholders = ",".join("?" * len(new_slot_ids))
            cur2 = await db.execute(
                f"SELECT * FROM time_slots WHERE id IN ({placeholders}) AND is_active = 1",
                tuple(new_slot_ids),
            )
            rows = [dict(r) for r in await cur2.fetchall()]
            if len(rows) != len(new_slot_ids):
                await db.rollback()
                return None
            rows.sort(
                key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"]))
            )
            if any(s["day"] != new_day for s in rows):
                await db.rollback()
                return None

            ids_str = booking.get("booked_slot_ids")
            if ids_str:
                for part in str(ids_str).split(","):
                    part = part.strip()
                    if not part:
                        continue
                    await db.execute(
                        "UPDATE time_slots SET is_active = 1 WHERE id = ?",
                        (int(part),),
                    )
            else:
                await db.execute(
                    """
                    UPDATE time_slots SET is_active = 1
                    WHERE day = ? AND start_time = ? AND end_time = ?
                    """,
                    (booking["day"], booking["start_time"], booking["end_time"]),
                )

            for s in rows:
                curx = await db.execute(
                    "UPDATE time_slots SET is_active = 0 WHERE id = ? AND is_active = 1",
                    (int(s["id"]),),
                )
                if curx.rowcount != 1:
                    await db.rollback()
                    return None

            start_time = rows[0]["start_time"]
            end_time = rows[-1]["end_time"]
            ids_csv = ",".join(str(int(s["id"])) for s in rows)
            await db.execute(
                """
                UPDATE bookings SET day = ?, start_time = ?, end_time = ?,
                  booked_slot_ids = ?, status = 'active', pending_meta = NULL
                WHERE id = ?
                """,
                (new_day, start_time, end_time, ids_csv, booking_id),
            )
            await db.commit()
            cur3 = await db.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,))
            r3 = await cur3.fetchone()
            return dict(r3) if r3 else None

    async def get_day_schedule(self, day: str) -> list[dict[str, Any]]:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT ts.start_time, ts.end_time, ts.is_active,
                       b.id AS booking_id, b.user_name, b.phone, b.services, b.total_price, b.tg_username, b.requires_engineer
                FROM time_slots ts
                LEFT JOIN bookings b
                    ON b.day = ts.day
                   AND b.status IN (
                       'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                   )
                   AND (b.booking_kind IS NULL OR b.booking_kind = 'studio')
                   AND (
                        (b.booked_slot_ids IS NOT NULL
                         AND ',' || b.booked_slot_ids || ',' LIKE '%,' || CAST(ts.id AS TEXT) || ',%')
                     OR (b.booked_slot_ids IS NULL
                         AND b.start_time = ts.start_time
                         AND b.end_time = ts.end_time)
                   )
                WHERE ts.day = ?
                ORDER BY ts.start_time
                """,
                (day,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def save_reminder_job(self, booking_id: int, job_id: str, run_at: datetime) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute(
                "INSERT OR REPLACE INTO reminder_jobs(booking_id, job_id, run_at) VALUES (?, ?, ?)",
                (booking_id, job_id, run_at.isoformat()),
            )
            await db.commit()

    async def delete_reminder_job(self, booking_id: int) -> None:
        async with self.connect() as db:
            self._configure(db)
            await db.execute("DELETE FROM reminder_jobs WHERE booking_id = ?", (booking_id,))
            await db.commit()

    async def get_all_active_bookings(self) -> list[dict[str, Any]]:
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT * FROM bookings
                WHERE status = 'active'
                ORDER BY day, start_time
                """
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def slot_held_by_booking(self, slot_id: int) -> bool:
        """Слот занят подтверждённой или ожидающей оплаты бронью."""
        sid = str(int(slot_id))
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute(
                """
                SELECT 1 FROM bookings
                WHERE status IN (
                    'active', 'pending_payment', 'pending_cancel', 'pending_reschedule'
                )
                  AND (booking_kind IS NULL OR booking_kind = 'studio')
                  AND booked_slot_ids IS NOT NULL
                  AND ',' || booked_slot_ids || ',' LIKE '%,' || ? || ',%'
                LIMIT 1
                """,
                (sid,),
            )
            return await cur.fetchone() is not None

    async def admin_toggle_slot_availability(self, slot_id: int) -> tuple[bool, str]:
        """
        Переключить слот без клиентской брони: свободен → админ.занят, админ.занят → свободен.
        Если слот в брони — отказ.
        """
        async with self.connect() as db:
            self._configure(db)
            cur = await db.execute("SELECT * FROM time_slots WHERE id = ?", (int(slot_id),))
            row = await cur.fetchone()
            if not row:
                return False, "Слот не найден"
            slot = dict(row)
            if Database.slot_row_is_active(slot["is_active"]):
                await db.execute("UPDATE time_slots SET is_active = 0 WHERE id = ?", (int(slot_id),))
                await db.commit()
                return True, "Слот помечен как занят (без клиента)"
            if await self.slot_held_by_booking(int(slot_id)):
                return False, "Слот в активной брони — отмените запись или используйте отмену брони"
            await db.execute("UPDATE time_slots SET is_active = 1 WHERE id = ?", (int(slot_id),))
            await db.commit()
            return True, "Слот снова доступен для клиентов"

    @staticmethod
    def slot_ids_for_consecutive_hours_from(
        slots: list[dict[str, Any]],
        start_hhmm: str,
        hour_count: int,
    ) -> list[int] | None:
        """
        Подряд идущие почасовые слоты, начиная с start_hhmm (например 00:00 или 09:00).
        slots — слоты одного дня, в любом порядке (будут отсортированы).
        """
        if hour_count < 1:
            return None
        ordered = sorted(
            slots,
            key=lambda s: Database.time_sort_key(Database._coerce_cell_str(s["start_time"])),
        )
        by_start = {
            Database.normalize_time_str(Database._coerce_cell_str(s["start_time"])): s for s in ordered
        }
        cur = Database.normalize_time_str(start_hhmm)
        ids: list[int] = []
        for _ in range(hour_count):
            s = by_start.get(cur)
            if not s or not Database.slot_row_is_active(s["is_active"]):
                return None
            ids.append(int(s["id"]))
            cur = Database.normalize_time_str(Database._coerce_cell_str(s["end_time"]))
        return ids

    async def get_days_with_free_tariff_block(
        self,
        *,
        days_ahead: int,
        start_hhmm: str,
        hour_count: int,
        block_weekends: bool,
    ) -> set[str]:
        """
        Дни, где подряд есть hour_count свободных почасовых слотов с start_hhmm.
        block_weekends: для записи со звукорежиссёром — сб/вс исключаются.
        """
        base = set(await self.get_available_days(days_ahead=days_ahead))
        if block_weekends:
            base = {
                d
                for d in base
                if datetime.fromisoformat(d).weekday() < 5
            }
        allowed: set[str] = set()
        for day in base:
            slots = await self.get_all_slots_for_day(day)
            ids = Database.slot_ids_for_consecutive_hours_from(slots, start_hhmm, hour_count)
            if ids is not None:
                allowed.add(day)
        return allowed

    @staticmethod
    def tariff_time_range_label(start_hhmm: str, hour_count: int) -> tuple[str, str]:
        """(start, end) как строки HH:MM для отображения (конец — время окончания последнего часа)."""
        start = Database.normalize_time_str(start_hhmm)
        h0, m0 = Database.time_sort_key(start)
        start_min = h0 * 60 + m0
        end_min = start_min + hour_count * 60
        # Все тарифы укладываются в одни сутки (00:00–12:00 ночь; день с 09/12 до конца дня без перехода)
        eh = (end_min // 60) % 24
        em = end_min % 60
        end_str = f"{eh:02d}:{em:02d}"
        return start, end_str

    @staticmethod
    def booking_start_datetime(booking: dict[str, Any]) -> datetime:
        d = date.fromisoformat(booking["day"])
        t = time.fromisoformat(booking["start_time"])
        return datetime.combine(d, t)

    @staticmethod
    def booking_time_started(booking: dict[str, Any], *, timezone: str) -> bool:
        from zoneinfo import ZoneInfo

        kind = booking.get("booking_kind") or "studio"
        if kind != "studio":
            return False
        day_raw = str(booking.get("day") or "").strip()
        if len(day_raw) != 10 or day_raw == "service":
            return False
        try:
            tz = ZoneInfo(timezone)
            start = Database.booking_start_datetime(booking).replace(tzinfo=tz)
        except (ValueError, TypeError):
            return False
        return datetime.now(tz) >= start

