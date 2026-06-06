#
# Copyright 2025 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for the kill-discarded-alive path on the pool facades.

Verifies the two contracts surfaced by review on PR #986:
- ``_kill_sandbox_best_effort`` returns ``True`` only on a confirmed kill, so the
  ``logger.debug("Killed ...")`` line in ``_kill_discarded_alive`` cannot fire on failure.
- ``SandboxPoolAsync._kill_discarded_alive`` runs its per-ID kills concurrently via
  ``asyncio.gather`` instead of serially blocking ``acquire()``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

import pytest

from opensandbox.config.connection import ConnectionConfig
from opensandbox.config.connection_sync import ConnectionConfigSync
from opensandbox.pool import (
    InMemoryAsyncPoolStateStore,
    InMemoryPoolStateStore,
    PoolCreationSpec,
)
from opensandbox.pool_async import SandboxPoolAsync
from opensandbox.sync.pool import SandboxPoolSync


class _RecordingSyncManager:
    """Sync manager fake that tracks kills and can simulate failures."""

    def __init__(self, fail_for: set[str] | None = None) -> None:
        self.killed: list[str] = []
        self._fail_for = fail_for or set()

    def kill_sandbox(self, sandbox_id: str) -> None:
        self.killed.append(sandbox_id)
        if sandbox_id in self._fail_for:
            raise RuntimeError(f"simulated kill failure for {sandbox_id}")

    def close(self) -> None:  # pragma: no cover - SandboxPoolSync may call on shutdown
        return None


class _RecordingAsyncManager:
    """Async manager fake recording call timing so we can detect serial vs parallel kills."""

    def __init__(self, per_call_delay: float = 0.05) -> None:
        self.killed: list[str] = []
        self._delay = per_call_delay
        self._in_flight = 0
        self.max_in_flight = 0

    async def kill_sandbox(self, sandbox_id: str) -> None:
        self._in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self._in_flight)
        try:
            await asyncio.sleep(self._delay)
            self.killed.append(sandbox_id)
        finally:
            self._in_flight -= 1

    async def close(self) -> None:  # pragma: no cover
        return None


def _build_sync_pool(manager: _RecordingSyncManager) -> SandboxPoolSync:
    return SandboxPoolSync(
        pool_name="kill-pool",
        max_idle=0,
        state_store=InMemoryPoolStateStore(),
        connection_config=ConnectionConfigSync(),
        creation_spec=PoolCreationSpec(image="ubuntu:22.04"),
        sandbox_manager_factory=lambda config: manager,  # type: ignore[arg-type,return-value]
    )


def _build_async_pool(manager: _RecordingAsyncManager) -> SandboxPoolAsync:
    async def _factory(_: ConnectionConfig) -> _RecordingAsyncManager:
        return manager

    return SandboxPoolAsync(
        pool_name="kill-pool",
        max_idle=0,
        state_store=InMemoryAsyncPoolStateStore(),
        connection_config=ConnectionConfig(),
        creation_spec=PoolCreationSpec(image="ubuntu:22.04"),
        sandbox_manager_factory=_factory,  # type: ignore[arg-type]
    )


def test_sync_kill_best_effort_returns_true_on_success() -> None:
    manager = _RecordingSyncManager()
    pool = _build_sync_pool(manager)
    pool._sandbox_manager = manager  # type: ignore[assignment]

    assert pool._kill_sandbox_best_effort("sandbox-1") is True
    assert manager.killed == ["sandbox-1"]


def test_sync_kill_best_effort_returns_false_on_failure() -> None:
    manager = _RecordingSyncManager(fail_for={"sandbox-1"})
    pool = _build_sync_pool(manager)
    pool._sandbox_manager = manager  # type: ignore[assignment]

    assert pool._kill_sandbox_best_effort("sandbox-1") is False
    # Manager was called but the failure was swallowed.
    assert manager.killed == ["sandbox-1"]


def test_sync_kill_best_effort_returns_false_when_manager_missing() -> None:
    manager = _RecordingSyncManager()
    pool = _build_sync_pool(manager)
    # Pool never started → _sandbox_manager is None.
    assert pool._sandbox_manager is None
    assert pool._kill_sandbox_best_effort("sandbox-1") is False


def test_sync_kill_discarded_alive_logs_only_on_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = _RecordingSyncManager(fail_for={"sandbox-fail"})
    pool = _build_sync_pool(manager)
    pool._sandbox_manager = manager  # type: ignore[assignment]

    with caplog.at_level(logging.DEBUG, logger="opensandbox.sync.pool"):
        pool._kill_discarded_alive(
            pool_name="kill-pool",
            sandbox_ids=("sandbox-ok", "sandbox-fail"),
            source="acquire",
        )

    debug_messages = [
        record.message for record in caplog.records if record.levelno == logging.DEBUG
    ]
    assert any("sandbox-ok" in msg for msg in debug_messages), (
        f"expected debug log for successful kill, got {debug_messages}"
    )
    assert not any("Killed near-expiry idle sandbox" in msg and "sandbox-fail" in msg for msg in debug_messages), (
        f"failed kill should not produce 'Killed' debug log, got {debug_messages}"
    )


@pytest.mark.asyncio
async def test_async_kill_discarded_alive_runs_kills_concurrently() -> None:
    manager = _RecordingAsyncManager(per_call_delay=0.05)
    pool = _build_async_pool(manager)
    pool._sandbox_manager = manager  # type: ignore[assignment]

    ids = ("a", "b", "c", "d", "e")
    start = asyncio.get_event_loop().time()
    await pool._kill_discarded_alive(
        pool_name="kill-pool", sandbox_ids=ids, source="acquire"
    )
    elapsed = asyncio.get_event_loop().time() - start

    # Serial would take ~5 * 0.05 = 0.25s; parallel should finish well below that.
    assert elapsed < 0.15, (
        f"kills appear to run serially (took {elapsed:.3f}s for {len(ids)} ids); "
        "expected asyncio.gather to overlap them"
    )
    assert manager.max_in_flight >= 2, (
        f"expected concurrent kills, max_in_flight={manager.max_in_flight}"
    )
    assert set(manager.killed) == set(ids)


@pytest.mark.asyncio
async def test_async_kill_best_effort_returns_false_on_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingAsyncManager:
        def __init__(self) -> None:
            self.attempted: list[str] = []

        async def kill_sandbox(self, sandbox_id: str) -> None:
            self.attempted.append(sandbox_id)
            raise RuntimeError("simulated async failure")

        async def close(self) -> None:  # pragma: no cover
            return None

    manager: Any = FailingAsyncManager()
    pool = SandboxPoolAsync(
        pool_name="kill-pool",
        max_idle=0,
        state_store=InMemoryAsyncPoolStateStore(),
        connection_config=ConnectionConfig(),
        creation_spec=PoolCreationSpec(image="ubuntu:22.04"),
        sandbox_manager_factory=lambda _: _async_returns(manager),  # type: ignore[arg-type]
    )
    pool._sandbox_manager = manager

    with caplog.at_level(logging.DEBUG, logger="opensandbox.pool_async"):
        await pool._kill_discarded_alive(
            pool_name="kill-pool", sandbox_ids=("a",), source="acquire"
        )

    assert manager.attempted == ["a"]
    debug_messages = [
        record.message for record in caplog.records if record.levelno == logging.DEBUG
    ]
    assert not any("Killed near-expiry idle sandbox" in msg for msg in debug_messages), (
        f"failed async kill should not produce 'Killed' debug log, got {debug_messages}"
    )


async def _async_returns(value: Any) -> Any:
    return value
