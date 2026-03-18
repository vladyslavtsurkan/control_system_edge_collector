"""Tests for SQLite-backed persistent telemetry buffer."""

from app.data_plane.persistent_buffer import PersistentEdgeBuffer


class TestPersistentEdgeBuffer:
    async def test_put_and_get_batch_in_fifo_order(self, tmp_path) -> None:
        db_path = tmp_path / "edge_buffer.db"
        buffer = PersistentEdgeBuffer(db_path)
        await buffer.initialize()

        await buffer.put({"seq": 1})
        await buffer.put({"seq": 2})
        await buffer.put({"seq": 3})

        rows = await buffer.get_batch(2)
        await buffer.close()

        assert [row["payload"]["seq"] for row in rows] == [1, 2]
        assert rows[0]["id"] < rows[1]["id"]

    async def test_commit_removes_only_selected_ids(self, tmp_path) -> None:
        db_path = tmp_path / "edge_buffer.db"
        buffer = PersistentEdgeBuffer(db_path)
        await buffer.initialize()

        await buffer.put({"seq": 1})
        await buffer.put({"seq": 2})
        await buffer.put({"seq": 3})

        rows = await buffer.get_batch(3)
        await buffer.commit([rows[0]["id"], rows[2]["id"]])
        remaining = await buffer.get_batch(10)
        await buffer.close()

        assert len(remaining) == 1
        assert remaining[0]["payload"]["seq"] == 2

    async def test_data_persists_after_reopen(self, tmp_path) -> None:
        db_path = tmp_path / "edge_buffer.db"

        writer = PersistentEdgeBuffer(db_path)
        await writer.initialize()
        await writer.put({"sensor_id": "abc", "payload": {"value": 99}})
        await writer.close()

        reader = PersistentEdgeBuffer(db_path)
        await reader.initialize()
        rows = await reader.get_batch(10)
        await reader.close()

        assert len(rows) == 1
        assert rows[0]["payload"]["payload"]["value"] == 99
