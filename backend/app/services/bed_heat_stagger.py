"""Farm-wide bed-heat stagger (kycrna fork).

Holds a queued start on a participating printer while another participating
printer is heating its bed, so several large bed heaters never pull peak
power at the same time. Separate from the timed "Stagger printer starts"
option in the print dialog, which only spaces out one multi-printer send by
writing staggered ``scheduled_time`` values.

The rule here only ever delays a start. It never cancels a job, and it never
touches a printer that is not in the participant list.

Everything in this module is pure (no DB, no MQTT) so it can be tested
directly; ``PrintScheduler.check_queue`` supplies the live state.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# A bed within this many °C of its target counts as at temperature. Bambu
# firmware settles within ~1 °C, and a heater that is this close is drawing
# maintenance power, not peak.
BED_AT_TARGET_TOLERANCE_C = 3.0

# Bed targets below this are not a heat request (0 = off; nothing prints on a
# 25 °C bed target).
MIN_HEATING_TARGET_C = 30.0

# Printer states in which a printer is starting or running a print. Anything
# else (IDLE, FINISH, FAILED, PAUSE, offline) never blocks: an idle or finished
# printer is not warming up for a job, whatever its bed is doing.
_ACTIVE_STATES = frozenset({"PREPARE", "SLICING", "RUNNING"})

# States where firmware has accepted a job but may not have set the bed target
# yet. Treated as warming up until the bed reports at target.
_PRE_PRINT_STATES = frozenset({"PREPARE", "SLICING"})

DEFAULT_MAX_WAIT_MINUTES = 20


def parse_printer_ids(raw: str | None) -> set[int]:
    """Parse the stored participant list ("1,2,4" or "[1, 2, 4]") into ids.

    Junk entries are ignored rather than raising: a hand-edited setting must
    not stop the queue.
    """
    if not raw:
        return set()
    ids: set[int] = set()
    for part in raw.replace("[", "").replace("]", "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids


def _bed_temps(status) -> tuple[float | None, float | None]:
    temps = getattr(status, "temperatures", None)
    if not isinstance(temps, dict):
        return None, None
    try:
        bed = temps.get("bed")
        target = temps.get("bed_target")
        return (
            float(bed) if bed is not None else None,
            float(target) if target is not None else None,
        )
    except (TypeError, ValueError):
        return None, None


def bed_at_target(status) -> bool:
    """True when the printer reports a real bed target and the bed has reached it."""
    bed, target = _bed_temps(status)
    if target is None or target < MIN_HEATING_TARGET_C or bed is None:
        return False
    return bed >= target - BED_AT_TARGET_TOLERANCE_C


def is_warming_up(status) -> bool:
    """Is this printer heating its bed to start (or during) a print?

    - Only PREPARE / SLICING / RUNNING printers can be warming up.
    - A bed at (or above) its target is never heating.
    - A bed below a real target is heating.
    - PREPARE / SLICING with no target yet is the moment just after a job was
      accepted, before firmware raises the target: treat as warming up.
    """
    if status is None:
        return False
    if getattr(status, "state", None) not in _ACTIVE_STATES:
        return False
    bed, target = _bed_temps(status)
    if target is not None and target >= MIN_HEATING_TARGET_C:
        if bed is None:
            return True
        return bed < target - BED_AT_TARGET_TOLERANCE_C
    return status.state in _PRE_PRINT_STATES


@dataclass
class BedHeatTracker:
    """Remembers how long each printer has been blocking, so one stuck printer
    (bed never reaches target, a heater fault, a job sat in PREPARE) cannot hold
    the rest of the farm forever."""

    blocking_since: dict[int, float] = field(default_factory=dict)
    timed_out: set[int] = field(default_factory=set)

    def forget(self, printer_id: int) -> None:
        self.blocking_since.pop(printer_id, None)
        self.timed_out.discard(printer_id)

    def update(self, raw_blockers: set[int], now: float, max_wait_seconds: float) -> tuple[set[int], set[int]]:
        """Apply the max-wait cap to this pass's raw blockers.

        Returns ``(blockers, newly_timed_out)``. A printer that stops blocking
        has its clock reset, so the cap applies per warm-up, not per lifetime.
        """
        for pid in list(self.blocking_since):
            if pid not in raw_blockers:
                self.forget(pid)
        newly_timed_out: set[int] = set()
        blockers: set[int] = set()
        for pid in raw_blockers:
            since = self.blocking_since.setdefault(pid, now)
            if now - since >= max_wait_seconds:
                if pid not in self.timed_out:
                    self.timed_out.add(pid)
                    newly_timed_out.add(pid)
                continue
            blockers.add(pid)
        return blockers, newly_timed_out


def raw_blockers(
    participants: set[int],
    *,
    statuses: dict[int, object],
    connected: set[int],
    starting: set[int],
) -> set[int]:
    """Participating printers currently warming up, before the max-wait cap.

    ``starting`` is printers Bambuddy itself is in the middle of starting (an
    upload or preheat in flight, or a print command just sent that the printer
    has not acted on yet). Those count unless their bed already reports at
    target, which closes the gap between "command sent" and "firmware set a
    bed target".

    Offline printers never block: their last status is stale.
    """
    out: set[int] = set()
    for pid in participants:
        if pid not in connected:
            continue
        status = statuses.get(pid)
        if (pid in starting and not bed_at_target(status)) or is_warming_up(status):
            out.add(pid)
    return out


def hold_reason(printer_label: str, blocker_labels: list[str]) -> str:
    """Queue-row wording. Starts with "Busy:" so the queue treats it as a
    plain wait that needs no user action (no notification)."""
    names = ", ".join(sorted(blocker_labels)) or "another printer"
    return f"Busy: {printer_label} (bed-heat stagger: waiting for {names} to finish heating its bed)"
