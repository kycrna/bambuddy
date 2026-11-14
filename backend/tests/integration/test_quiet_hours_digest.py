"""Messages that arrive in quiet hours are held, then sent as one message when
quiet hours end, instead of being dropped."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.app.models.notification import NotificationDigestQueue, NotificationProvider
from backend.app.services.notification_service import notification_service

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _provider(db_session, **kw) -> NotificationProvider:
    provider = NotificationProvider(
        name="phone",
        provider_type="ntfy",
        enabled=True,
        config=json.dumps({"server": "https://ntfy.example", "topic": "t"}),
        quiet_hours_enabled=True,
        quiet_hours_start="22:00",
        quiet_hours_end="07:00",
        **kw,
    )
    db_session.add(provider)
    await db_session.commit()
    return provider


async def _rows(db_session, provider_id: int) -> list[NotificationDigestQueue]:
    db_session.expire_all()
    rows = await db_session.scalars(
        select(NotificationDigestQueue).where(NotificationDigestQueue.provider_id == provider_id)
    )
    return list(rows.all())


async def _run_scheduler(test_engine, *, quiet_now: bool, send_result=(True, "")) -> AsyncMock:
    maker = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    send = AsyncMock(return_value=send_result)
    notification_service._last_digest_check = ""
    with (
        patch("backend.app.core.database.async_session", maker),
        patch.object(notification_service, "_send_ntfy", send),
        patch.object(notification_service, "_is_in_quiet_hours", return_value=quiet_now),
    ):
        await notification_service.check_and_send_digests()
    return send


async def test_a_message_in_quiet_hours_is_held_not_sent(db_session):
    provider = await _provider(db_session)
    send = AsyncMock(return_value=(True, ""))
    with (
        patch.object(notification_service, "_send_ntfy", send),
        patch.object(notification_service, "_is_in_quiet_hours", return_value=True),
    ):
        await notification_service._send_to_providers(
            [provider], "Printer Error", "AMS jam on slot 2", db_session, event_type="printer_error"
        )

    send.assert_not_awaited()
    rows = await _rows(db_session, provider.id)
    assert len(rows) == 1 and rows[0].held_for_quiet_hours is True


async def test_held_messages_wait_while_quiet_hours_last(test_engine, db_session):
    provider = await _provider(db_session)
    db_session.add(
        NotificationDigestQueue(
            provider_id=provider.id, event_type="printer_error", title="Error", message="x", held_for_quiet_hours=True
        )
    )
    await db_session.commit()

    send = await _run_scheduler(test_engine, quiet_now=True)

    send.assert_not_awaited()
    assert len(await _rows(db_session, provider.id)) == 1


async def test_held_messages_go_out_together_when_quiet_hours_end(test_engine, db_session):
    provider = await _provider(db_session, daily_digest_enabled=True, daily_digest_time="23:59")
    for title, msg in (("Printer Error", "AMS jam on slot 2"), ("Print Failed", "Spaghetti detected")):
        db_session.add(
            NotificationDigestQueue(
                provider_id=provider.id,
                event_type="printer_error",
                title=title,
                message=msg,
                held_for_quiet_hours=True,
            )
        )
    # A daily-digest entry stays for the daily digest.
    db_session.add(
        NotificationDigestQueue(provider_id=provider.id, event_type="print_complete", title="Done", message="d")
    )
    await db_session.commit()

    send = await _run_scheduler(test_engine, quiet_now=False)

    send.assert_awaited_once()
    sent_text = json.dumps([send.await_args.args, send.await_args.kwargs], default=str)
    assert "AMS jam on slot 2" in sent_text and "Spaghetti detected" in sent_text
    assert "Done" not in sent_text
    remaining = await _rows(db_session, provider.id)
    assert [r.title for r in remaining] == ["Done"]


async def test_a_failed_send_keeps_held_messages(test_engine, db_session):
    provider = await _provider(db_session)
    db_session.add(
        NotificationDigestQueue(
            provider_id=provider.id, event_type="printer_error", title="Error", message="x", held_for_quiet_hours=True
        )
    )
    await db_session.commit()

    await _run_scheduler(test_engine, quiet_now=False, send_result=(False, "down"))

    assert len(await _rows(db_session, provider.id)) == 1
