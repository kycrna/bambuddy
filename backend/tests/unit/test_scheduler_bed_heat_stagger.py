"""Farm-wide bed-heat stagger (kycrna fork).

Farm: P2S-L (1), X2D (2), A1 Mini (3), P2S-R (4). Participants are 1, 2 and 4.
A queued start on a participant is held while another participant is warming
its bed, and released once that bed is at temperature. The A1 Mini never
holds and is never held.
"""

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import backend.app.models  # noqa: F401 - populate Base.metadata
from backend.app.core.database import Base
from backend.app.models.library import LibraryFile
from backend.app.models.print_queue import PrintQueueItem
from backend.app.models.printer import Printer
from backend.app.models.settings import Settings
from backend.app.services import bed_heat_stagger as bhs
from backend.app.services.print_scheduler import PrintScheduler

P2S_L, X2D, A1_MINI, P2S_R = 1, 2, 3, 4


def _status(state="IDLE", bed=25.0, target=0.0):
    return SimpleNamespace(state=state, temperatures={"bed": bed, "bed_target": target}, subtask_id=None)


COLD_IDLE = _status()
HEATING = _status("PREPARE", bed=40.0, target=65.0)
AT_TEMP_PRINTING = _status("RUNNING", bed=64.5, target=65.0)


@pytest.fixture
async def ctx():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with session_maker() as db:
        for pid, name, model in (
            (P2S_L, "P2S-L", "P2S"),
            (X2D, "X2D", "X2D"),
            (A1_MINI, "A1 Mini", "A1 Mini"),
            (P2S_R, "P2S-R", "P2S"),
        ):
            db.add(
                Printer(
                    id=pid,
                    name=name,
                    serial_number=f"SN{pid}",
                    ip_address=f"10.0.0.{pid}",
                    access_code="x",
                    model=model,
                    is_active=True,
                )
            )
        db.add(Settings(key="bed_heat_stagger_printer_ids", value="1,2,4"))
        await db.commit()

    try:
        yield SimpleNamespace(session_maker=session_maker, scheduler=PrintScheduler())
    finally:
        await engine.dispose()


async def _add(ctx, *, position, printer_id=None, target_model=None, scheduled_time=None):
    async with ctx.session_maker() as db:
        lib = LibraryFile(
            filename="job.gcode.3mf",
            file_path="/library/job.gcode.3mf",
            file_size=10,
            file_type="gcode.3mf",
            file_metadata={"sliced_for_model": target_model or "P2S"},
        )
        db.add(lib)
        await db.flush()
        item = PrintQueueItem(
            status="pending",
            position=position,
            printer_id=printer_id,
            target_model=target_model,
            library_file_id=lib.id,
            scheduled_time=scheduled_time,
        )
        db.add(item)
        await db.commit()
        return item.id


async def _set(ctx, key, value):
    async with ctx.session_maker() as db:
        row = (await db.execute(select(Settings).where(Settings.key == key))).scalar_one_or_none()
        if row:
            row.value = value
        else:
            db.add(Settings(key=key, value=value))
        await db.commit()


async def _item(ctx, item_id):
    async with ctx.session_maker() as db:
        return (await db.execute(select(PrintQueueItem).where(PrintQueueItem.id == item_id))).scalar_one()


async def _run(ctx, *, idle, statuses=None, offline=()):
    """One check_queue pass on the shared scheduler; returns {item_id: printer_id} dispatched.

    Dispatched items are marked printing afterwards, as the real upload would.
    """
    statuses = statuses or {}
    scheduler = ctx.scheduler
    launched = MagicMock()
    patches = [
        patch("backend.app.services.print_scheduler.async_session", ctx.session_maker),
        patch("backend.app.core.database.async_session", ctx.session_maker),
        patch(
            "backend.app.services.print_scheduler.printer_manager.is_connected",
            MagicMock(side_effect=lambda pid: pid not in offline),
        ),
        patch(
            "backend.app.services.print_scheduler.printer_manager.get_status",
            MagicMock(side_effect=lambda pid: statuses.get(pid, COLD_IDLE)),
        ),
        patch(
            "backend.app.services.print_scheduler.printer_manager.is_awaiting_plate_clear",
            MagicMock(return_value=False),
        ),
        patch(
            "backend.app.services.print_scheduler.ha_sensor_manager.blocked_printers",
            AsyncMock(return_value={}),
        ),
        patch(
            "backend.app.services.notification_service.notification_service.on_queue_job_waiting",
            AsyncMock(),
        ),
        patch(
            "backend.app.services.notification_service.notification_service.on_queue_job_assigned",
            AsyncMock(),
        ),
        patch.object(scheduler, "_is_printer_idle", MagicMock(side_effect=lambda pid, *_a, **_k: pid in idle)),
        patch.object(scheduler, "_check_auto_drying", AsyncMock()),
        patch.object(scheduler, "_check_filament_low", AsyncMock()),
        patch.object(scheduler, "_check_stock_forecast", AsyncMock()),
        patch.object(scheduler, "_ensure_ams_mapping", AsyncMock(return_value=None)),
        patch.object(scheduler, "_block_on_filament_deficit", AsyncMock(return_value=False)),
        patch.object(scheduler, "_block_on_unmatched_filament", AsyncMock(return_value=False)),
        patch.object(scheduler, "_get_smart_plugs", AsyncMock(return_value=[])),
        patch.object(scheduler, "_launch_uploads", launched),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        await scheduler.check_queue()

    if not launched.called:
        return {}
    dispatched = {}
    async with ctx.session_maker() as db:
        for item_id in launched.call_args[0][0]:
            row = (await db.execute(select(PrintQueueItem).where(PrintQueueItem.id == item_id))).scalar_one()
            dispatched[item_id] = row.printer_id
            row.status = "printing"
        await db.commit()
    return dispatched


class TestIndependentJobs:
    @pytest.mark.asyncio
    async def test_two_ready_together_go_one_at_a_time(self, ctx):
        """Two jobs queued separately for P2S-L and P2S-R, both printers idle:
        the first starts, the second waits until P2S-L's bed is warm."""
        left = await _add(ctx, position=1, printer_id=P2S_L)
        right = await _add(ctx, position=2, printer_id=P2S_R)

        assert await _run(ctx, idle={P2S_L, P2S_R}) == {left: P2S_L}
        held = await _item(ctx, right)
        assert held.status == "pending"
        assert held.waiting_reason.startswith("Busy: P2S-R (bed-heat stagger")
        assert "P2S-L" in held.waiting_reason

        # P2S-L is now warming its bed: still held.
        assert await _run(ctx, idle={P2S_R}, statuses={P2S_L: HEATING}) == {}
        assert (await _item(ctx, right)).status == "pending"

        # P2S-L's bed reached target and it is printing: released.
        assert await _run(ctx, idle={P2S_R}, statuses={P2S_L: AT_TEMP_PRINTING}) == {right: P2S_R}

    @pytest.mark.asyncio
    async def test_held_while_another_bed_is_heating(self, ctx):
        """A job queued later on X2D waits while P2S-R (started earlier) heats."""
        job = await _add(ctx, position=1, printer_id=X2D)
        assert await _run(ctx, idle={X2D}, statuses={P2S_R: _status("RUNNING", bed=50, target=65)}) == {}
        assert "P2S-R" in (await _item(ctx, job)).waiting_reason

    @pytest.mark.asyncio
    async def test_scheduled_job_still_held_after_its_time(self, ctx):
        due = datetime.now(timezone.utc) - timedelta(minutes=1)
        job = await _add(ctx, position=1, printer_id=P2S_R, scheduled_time=due)
        assert await _run(ctx, idle={P2S_R}, statuses={X2D: HEATING}) == {}
        assert (await _item(ctx, job)).status == "pending"
        assert await _run(ctx, idle={P2S_R}, statuses={X2D: AT_TEMP_PRINTING}) == {job: P2S_R}

    @pytest.mark.asyncio
    async def test_upload_in_flight_counts_as_starting(self, ctx):
        """Between "Bambuddy is uploading/preheating" and "firmware reports a
        bed target", the printer being started still blocks."""
        ctx.scheduler._inflight[999] = (MagicMock(), X2D)
        job = await _add(ctx, position=1, printer_id=P2S_L)
        assert await _run(ctx, idle={P2S_L}) == {}
        assert (await _item(ctx, job)).status == "pending"

    @pytest.mark.asyncio
    async def test_any_model_job_is_held_without_being_assigned(self, ctx):
        job = await _add(ctx, position=1, target_model="P2S")
        assert await _run(ctx, idle={P2S_L}, statuses={X2D: HEATING}) == {}
        row = await _item(ctx, job)
        assert row.printer_id is None
        assert row.waiting_reason.startswith("Busy:")
        assert await _run(ctx, idle={P2S_L}, statuses={X2D: AT_TEMP_PRINTING}) == {job: P2S_L}


class TestNoBlock:
    @pytest.mark.asyncio
    async def test_single_starting_printer_is_not_held(self, ctx):
        job = await _add(ctx, position=1, printer_id=P2S_L)
        assert await _run(ctx, idle={P2S_L}) == {job: P2S_L}

    @pytest.mark.asyncio
    async def test_printers_that_are_printing_idle_finished_or_failed_do_not_block(self, ctx):
        job = await _add(ctx, position=1, printer_id=P2S_L)
        statuses = {
            X2D: AT_TEMP_PRINTING,
            # Idle / finished / failed with a warm-but-cooling or held bed.
            P2S_R: _status("FINISH", bed=40, target=65),
        }
        assert await _run(ctx, idle={P2S_L}, statuses=statuses) == {job: P2S_L}

    @pytest.mark.asyncio
    async def test_disabled_setting_restores_old_behaviour(self, ctx):
        await _set(ctx, "bed_heat_stagger_enabled", "false")
        left = await _add(ctx, position=1, printer_id=P2S_L)
        right = await _add(ctx, position=2, printer_id=P2S_R)
        assert await _run(ctx, idle={P2S_L, P2S_R}, statuses={X2D: HEATING}) == {left: P2S_L, right: P2S_R}

    @pytest.mark.asyncio
    async def test_no_participants_means_no_holds(self, ctx):
        await _set(ctx, "bed_heat_stagger_printer_ids", "")
        left = await _add(ctx, position=1, printer_id=P2S_L)
        right = await _add(ctx, position=2, printer_id=P2S_R)
        assert await _run(ctx, idle={P2S_L, P2S_R}) == {left: P2S_L, right: P2S_R}


class TestA1MiniIgnored:
    @pytest.mark.asyncio
    async def test_a1_mini_heating_does_not_block(self, ctx):
        job = await _add(ctx, position=1, printer_id=P2S_L)
        assert await _run(ctx, idle={P2S_L}, statuses={A1_MINI: HEATING}) == {job: P2S_L}

    @pytest.mark.asyncio
    async def test_a1_mini_is_never_held(self, ctx):
        job = await _add(ctx, position=1, printer_id=A1_MINI)
        assert await _run(ctx, idle={A1_MINI}, statuses={X2D: HEATING, P2S_L: HEATING}) == {job: A1_MINI}

    @pytest.mark.asyncio
    async def test_a1_mini_starts_alongside_a_participant(self, ctx):
        mini = await _add(ctx, position=1, printer_id=A1_MINI)
        left = await _add(ctx, position=2, printer_id=P2S_L)
        assert await _run(ctx, idle={A1_MINI, P2S_L}) == {mini: A1_MINI, left: P2S_L}


class TestNoDeadlock:
    @pytest.mark.asyncio
    async def test_offline_printer_does_not_block(self, ctx):
        job = await _add(ctx, position=1, printer_id=P2S_L)
        assert await _run(ctx, idle={P2S_L}, statuses={X2D: HEATING}, offline={X2D}) == {job: P2S_L}

    @pytest.mark.asyncio
    async def test_stuck_heater_stops_blocking_after_max_wait(self, ctx):
        await _set(ctx, "bed_heat_stagger_max_wait_minutes", "20")
        job = await _add(ctx, position=1, printer_id=P2S_L)
        stuck = {X2D: _status("RUNNING", bed=80, target=110)}

        assert await _run(ctx, idle={P2S_L}, statuses=stuck) == {}
        # Pretend X2D has been stuck below target for 21 minutes.
        ctx.scheduler._bed_heat_tracker.blocking_since[X2D] -= 21 * 60
        assert await _run(ctx, idle={P2S_L}, statuses=stuck) == {job: P2S_L}

    def test_clock_resets_after_a_printer_stops_blocking(self):
        t = bhs.BedHeatTracker()
        assert t.update({2}, now=0, max_wait_seconds=60) == ({2}, set())
        assert t.update({2}, now=61, max_wait_seconds=60) == (set(), {2})
        assert t.update(set(), now=62, max_wait_seconds=60) == (set(), set())
        assert t.update({2}, now=63, max_wait_seconds=60) == ({2}, set())


class TestTimedStaggerUnchanged:
    @pytest.mark.asyncio
    async def test_multi_printer_send_with_timed_stagger(self, ctx):
        """The print dialog's timed stagger writes a later scheduled_time for
        each group. Group 2 still waits on the clock (no waiting reason), with
        or without the bed-heat rule."""
        await _set(ctx, "bed_heat_stagger_enabled", "false")
        later = datetime.now(timezone.utc) + timedelta(minutes=5)
        mini = await _add(ctx, position=1, printer_id=A1_MINI)
        x2d = await _add(ctx, position=2, printer_id=X2D, scheduled_time=later)

        assert await _run(ctx, idle={A1_MINI, X2D}) == {mini: A1_MINI}
        row = await _item(ctx, x2d)
        assert row.status == "pending"
        assert row.waiting_reason is None

    @pytest.mark.asyncio
    async def test_timed_stagger_groups_with_bed_rule_on(self, ctx):
        later = datetime.now(timezone.utc) + timedelta(minutes=5)
        left = await _add(ctx, position=1, printer_id=P2S_L)
        right = await _add(ctx, position=2, printer_id=P2S_R, scheduled_time=later)

        assert await _run(ctx, idle={P2S_L, P2S_R}) == {left: P2S_L}
        assert (await _item(ctx, right)).waiting_reason is None  # clock, not bed


class TestWarmingUp:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (None, False),
            (_status("IDLE", bed=30, target=65), False),
            (_status("FINISH", bed=30, target=65), False),
            (_status("FAILED", bed=30, target=65), False),
            (_status("PAUSE", bed=30, target=65), False),
            (_status("PREPARE", bed=25, target=0), True),
            (_status("PREPARE", bed=40, target=65), True),
            (_status("PREPARE", bed=63, target=65), False),
            (_status("RUNNING", bed=40, target=65), True),
            (_status("RUNNING", bed=65, target=65), False),
            (_status("RUNNING", bed=70, target=60), False),
            (_status("RUNNING", bed=25, target=0), False),
        ],
    )
    def test_is_warming_up(self, status, expected):
        assert bhs.is_warming_up(status) is expected

    def test_parse_printer_ids(self):
        assert bhs.parse_printer_ids("1, 2,4") == {1, 2, 4}
        assert bhs.parse_printer_ids("[1, 2]") == {1, 2}
        assert bhs.parse_printer_ids("1,x,") == {1}
        assert bhs.parse_printer_ids(None) == set()
