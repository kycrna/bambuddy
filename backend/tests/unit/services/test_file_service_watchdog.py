"""Fork: power-cycle a printer whose file service hung (file_service_watchdog)."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.app.services import file_service_watchdog as wd

PRINTER = SimpleNamespace(id=4, name="P2S-R", ip_address="10.0.0.9", access_code="x", model="P2S")


def _plug():
    return SimpleNamespace(id=1, name="Plug", printer_id=4, enabled=True, ha_entity_id=None)


def _session_with(plugs):
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = plugs
    db.execute = AsyncMock(return_value=result)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=db)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm)


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setattr(wd, "AUTO_POWER_CYCLE", True)
    monkeypatch.setattr(wd, "alert_file_service_hung", AsyncMock())
    monkeypatch.setattr(wd, "OFF_SECONDS", 0)
    monkeypatch.setattr(wd, "BOOT_CHECK_INTERVAL", 0)
    monkeypatch.setattr(wd.asyncio, "sleep", AsyncMock())


@pytest.mark.asyncio
async def test_healthy_printer_is_left_alone(fast):
    with (
        patch.object(wd, "probe_file_service", AsyncMock(return_value=True)),
        patch.object(wd, "async_session") as session,
    ):
        assert await wd.ensure_file_service(PRINTER) is True
    session.assert_not_called()


@pytest.mark.asyncio
async def test_hung_idle_printer_is_power_cycled(fast):
    service = MagicMock(turn_off=AsyncMock(return_value=True), turn_on=AsyncMock(return_value=True))
    with (
        patch.object(wd, "probe_file_service", AsyncMock(side_effect=[False, True])),
        patch.object(wd, "async_session", _session_with([_plug()])),
        patch.object(wd, "printer_manager") as pm,
        patch.object(wd, "smart_plug_manager") as spm,
        patch.object(wd, "publish_plug_state", AsyncMock()),
    ):
        pm.get_status.return_value = SimpleNamespace(state="FINISH")
        pm.is_connected.return_value = True
        spm.get_service_for_plug = AsyncMock(return_value=service)
        assert await wd.ensure_file_service(PRINTER) is True
    service.turn_off.assert_awaited_once()
    service.turn_on.assert_awaited_once()


@pytest.mark.asyncio
async def test_busy_printer_is_never_power_cycled(fast):
    with (
        patch.object(wd, "probe_file_service", AsyncMock(return_value=False)),
        patch.object(wd, "printer_manager") as pm,
        patch.object(wd, "async_session") as session,
    ):
        pm.get_status.return_value = SimpleNamespace(state="RUNNING")
        assert await wd.ensure_file_service(PRINTER) is False
    session.assert_not_called()


@pytest.mark.asyncio
async def test_no_plug_only_logs(fast):
    with (
        patch.object(wd, "probe_file_service", AsyncMock(return_value=False)),
        patch.object(wd, "async_session", _session_with([])),
        patch.object(wd, "printer_manager") as pm,
        patch.object(wd, "smart_plug_manager") as spm,
    ):
        pm.get_status.return_value = SimpleNamespace(state="IDLE")
        assert await wd.ensure_file_service(PRINTER) is False
    spm.get_service_for_plug.assert_not_called()


@pytest.mark.asyncio
async def test_alert_only_when_power_cycle_is_off(monkeypatch):
    monkeypatch.setattr(wd, "AUTO_POWER_CYCLE", False)
    alert = AsyncMock()
    monkeypatch.setattr(wd, "alert_file_service_hung", alert)
    with (
        patch.object(wd, "probe_file_service", AsyncMock(return_value=False)),
        patch.object(wd, "smart_plug_manager") as spm,
    ):
        assert await wd.ensure_file_service(PRINTER) is False
    alert.assert_awaited_once_with(PRINTER)
    spm.get_service_for_plug.assert_not_called()


@pytest.mark.asyncio
async def test_alert_goes_to_telegram_once_per_cooldown(monkeypatch):
    monkeypatch.setattr(wd, "_last_alert", {})
    telegram = SimpleNamespace(name="TG", provider_type="telegram")
    send = AsyncMock()
    with (
        patch.object(wd, "async_session", _session_with([telegram])),
        patch("backend.app.services.notification_service.notification_service._send_to_providers", send),
    ):
        await wd.alert_file_service_hung(PRINTER)
        await wd.alert_file_service_hung(PRINTER)
    send.assert_awaited_once()
    assert send.await_args.args[0] == [telegram]
