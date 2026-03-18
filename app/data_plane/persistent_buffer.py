"""Disk-backed telemetry buffer built on top of SQLite."""

import json
from pathlib import Path

import aiosqlite

from app.settings import get_settings

__all__ = ["PersistentEdgeBuffer"]


class PersistentEdgeBuffer:
    """Durable store-and-forward queue using a local SQLite database."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        settings = get_settings()
        self._db_path = Path(db_path or settings.EDGE_BUFFER_DB_PATH)
        self._conn: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        """Open the DB connection and create schema if missing."""
        if self._conn is not None:
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=FULL;")
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload JSON NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await self._conn.commit()

    async def put(self, payload: dict) -> None:
        """Persist a telemetry payload into the local buffer."""
        conn = await self._ensure_connection()
        await conn.execute(
            "INSERT INTO telemetry (payload) VALUES (?)",
            (json.dumps(payload),),
        )
        await conn.commit()

    async def get_batch(self, batch_size: int) -> list[dict]:
        """Read the oldest N records without deleting them."""
        if batch_size <= 0:
            return []

        conn = await self._ensure_connection()
        cursor = await conn.execute(
            """
            SELECT id, payload, created_at
            FROM telemetry
            ORDER BY id ASC
            LIMIT ?
            """,
            (batch_size,),
        )
        rows = await cursor.fetchall()
        await cursor.close()

        return [
            {
                "id": row[0],
                "payload": json.loads(row[1]),
                "created_at": row[2],
            }
            for row in rows
        ]

    async def commit(self, ids: list[int]) -> None:
        """Delete records by ID after successful AMQP ACK/publish."""
        if not ids:
            return

        conn = await self._ensure_connection()
        placeholders = ",".join("?" for _ in ids)
        await conn.execute(
            f"DELETE FROM telemetry WHERE id IN ({placeholders})",
            tuple(ids),
        )
        await conn.commit()

    async def close(self) -> None:
        """Close the underlying SQLite connection."""
        if self._conn is None:
            return

        await self._conn.close()
        self._conn = None

    async def _ensure_connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.initialize()
        assert self._conn is not None
        return self._conn
