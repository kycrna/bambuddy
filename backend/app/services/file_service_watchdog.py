"""Recover a printer whose file service (FTPS, port 990) has hung.

Some printers (seen on a P2S, fork-only) stop finishing the TLS handshake on
port 990 after sitting idle for hours while MQTT keeps working, so the card
looks fine but every upload times out. A power cycle clears it.

Before a queued upload, ``ensure_file_service`` probes the file service. If the
probe hangs it sends an alert to every enabled Telegram notification provider.
With ``AUTO_POWER_CYCLE`` on (Glenn's call 2026-11-16; alert-only 11-14 to 11-16) it also
power-cycles an idle printer through its linked smart plug and waits for the
file service to come back. Either way the upload then runs as it would have.
"""

import asyncio
import logging
import time

from sqlalchemy import select

from backend.app.core.database import async_session
from backend.app.models.printer import Printer
from backend.app.models.smart_plug import SmartPlug
from backend.app.services.bambu_ftp import BambuFTPClient
from backend.app.services.printer_manager import printer_manager
from backend.app.services.smart_plug_manager import publish_plug_state, smart_plug_manager

logger = logging.getLogger(__name__)

# True: power-cycle an idle printer through its plug. False: alert only.
AUTO_POWER_CYCLE = True
# At most one alert per printer in this window.
ALERT_COOLDOWN_SECONDS = 1800.0
_last_alert: dict[int, float] = {}

PROBE_SOCKET_TIMEOUT = 15.0
PROBE_CAP = 25.0
OFF_SECONDS = 10.0
BOOT_WAIT_SECONDS = 240.0
BOOT_CHECK_INTERVAL = 10.0
# Printer states in which cutting power would lose work.
_BUSY_STATES = {"RUNNING", "PRINTING", "PAUSE", "PREPARE", "SLICING"}


async def probe_file_service(printer: Printer) -> bool:
    """True if the printer's FTPS service completes a login within the cap."""

    def _probe() -> bool:
        client = BambuFTPClient(
            printer.ip_address,
            printer.access_code,
            timeout=PROBE_SOCKET_TIMEOUT,
            printer_model=printer.model,
            respect_handshake_cooloff=False,
            records_handshake_cooloff=False,
        )
        if client.connect():
            client.disconnect()
            return True
        return False

    try:
        return await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, _probe), timeout=PROBE_CAP)
    except Exception as e:
        logger.debug("File service probe for %s failed: %s", printer.name, e)
        return False


async def alert_file_service_hung(printer: Printer) -> None:
    """Tell every enabled Telegram provider the printer's file service hung. Never raises."""
    now = time.monotonic()
    last = _last_alert.get(printer.id)
    if last is not None and now - last < ALERT_COOLDOWN_SECONDS:
        return
    _last_alert[printer.id] = now
    try:
        # Imported here: notification_service pulls in much of the app.
        from backend.app.models.notification import NotificationProvider
        from backend.app.services.notification_service import notification_service

        async with async_session() as db:
            providers = list(
                (
                    await db.execute(
                        select(NotificationProvider).where(
                            NotificationProvider.provider_type == "telegram",
                            NotificationProvider.enabled.is_(True),
                        )
                    )
                )
                .scalars()
                .all()
            )
            if not providers:
                logger.warning("%s: file service hung but no enabled Telegram provider to alert", printer.name)
                return
            title = f"{printer.name}: uploads blocked"
            message = (
                f"{printer.name} ({printer.ip_address}) is not answering on its file service (port 990), "
                "so Bambuddy can't send it print files. "
                + (
                    "Bambuddy is power-cycling it through its smart plug (if it is idle and has one) "
                    "and will send the queued job when it is back."
                    if AUTO_POWER_CYCLE
                    else "Queued jobs for it will stall at 0%. Restart the printer, then start the job again."
                )
            )
            await notification_service._send_to_providers(
                providers, title, message, db, "printer_error", printer.id, printer.name
            )
    except Exception as e:
        logger.warning("%s: file service alert failed: %s", printer.name, e)


async def ensure_file_service(printer: Printer) -> bool:
    """Make sure the printer's file service answers, power-cycling it if needed.

    Returns True when the file service answers (after a recovery or without
    one). Never raises.
    """
    try:
        if await probe_file_service(printer):
            return True

        await alert_file_service_hung(printer)
        if not AUTO_POWER_CYCLE:
            logger.warning("%s: file service not answering (port 990); alert sent, not power-cycling", printer.name)
            return False

        state = printer_manager.get_status(printer.id)
        if state is not None and (state.state or "").upper() in _BUSY_STATES:
            logger.warning("%s: file service not answering, but the printer is busy; not power-cycling", printer.name)
            return False

        async with async_session() as db:
            plugs = (await db.execute(select(SmartPlug).where(SmartPlug.printer_id == printer.id))).scalars().all()
            plug = next((p for p in plugs if p.enabled and not (p.ha_entity_id or "").startswith("script.")), None)
            if plug is None:
                logger.warning(
                    "%s: file service not answering and no smart plug linked; power-cycle it by hand", printer.name
                )
                return False
            service = await smart_plug_manager.get_service_for_plug(plug, db)

        logger.warning("%s: file service not answering; power-cycling via plug '%s'", printer.name, plug.name)
        if not await service.turn_off(plug):
            logger.warning("%s: could not turn off plug '%s'", printer.name, plug.name)
            return False
        await publish_plug_state(plug.id, plug.name, plug.printer_id, on=False, ha_entity_id=plug.ha_entity_id)
        await asyncio.sleep(OFF_SECONDS)
        if not await service.turn_on(plug):
            logger.error("%s: plug '%s' did not turn back on; the printer is OFF", printer.name, plug.name)
            return False
        await publish_plug_state(plug.id, plug.name, plug.printer_id, on=True, ha_entity_id=plug.ha_entity_id)

        waited = 0.0
        while waited < BOOT_WAIT_SECONDS:
            await asyncio.sleep(BOOT_CHECK_INTERVAL)
            waited += BOOT_CHECK_INTERVAL
            if not printer_manager.is_connected(printer.id):
                continue
            if await probe_file_service(printer):
                logger.info("%s: file service back after power cycle (%.0fs)", printer.name, waited)
                # Let the printer settle and report a fresh status before the upload.
                await asyncio.sleep(5)
                return True
        logger.warning("%s: file service still not answering %.0fs after power cycle", printer.name, waited)
        return False
    except Exception as e:
        logger.warning("%s: file service check failed: %s", printer.name, e)
        return False
