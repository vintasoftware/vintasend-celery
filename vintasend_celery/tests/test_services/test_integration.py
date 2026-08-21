"""End-to-end tests for the VintaSend 2.0 background path over Celery.

Each test wires one `NotificationService` -- a background adapter plus a
`CeleryNotificationQueueService` -- and shares that single service between the web side
(`send()`) and the worker side (the Celery task). With `task_always_eager`, `send()` enqueues
the notification id and the task runs `delayed_send` in-process, reloading the notification
from the same backend and delivering it. See `example_app.celery.make_celery_queue_service`.
"""

import uuid
from io import BytesIO

import pytest
from vintasend.constants import NotificationStatus, NotificationTypes
from vintasend.services.dataclasses import NotificationAttachment
from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
from vintasend.services.notification_service import NotificationService, register_context
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import (
    FakeTemplateRenderer,
    FakeTemplateRendererWithException,
)

from example_app.celery import CeleryFakeEmailAdapter, make_celery_queue_service


@pytest.fixture()
def celery_app():
    from example_app.celery import celery_app

    celery_app.conf.update(task_always_eager=True)
    return celery_app


@pytest.fixture()
def notification_backend():
    backend = FakeFileBackend(database_file_name="celery-integration-tests-notifications.json")
    yield backend
    backend.clear()


def build_service(backend, renderer=None):
    renderer = renderer if renderer is not None else FakeTemplateRenderer()
    adapter = CeleryFakeEmailAdapter(template_renderer=renderer, backend=backend)
    service = NotificationService([adapter], backend)
    service.register_queue_service(make_celery_queue_service(service))
    return service, adapter


def make_context(test):
    if test != "test":
        raise ValueError("Invalid test value")
    return {"foo": "bar"}


@pytest.fixture(autouse=True)
def _register_context():
    register_context("test_context")(make_context)


def create_kwargs(**overrides):
    kwargs = {
        "user_id": 1,
        "notification_type": NotificationTypes.EMAIL.value,
        "title": "Test Notification",
        "body_template": "body.html",
        "context_name": "test_context",
        "context_kwargs": {"test": "test"},
        "subject_template": "subject.txt",
        "preheader_template": "preheader.html",
    }
    kwargs.update(overrides)
    return kwargs


def test_background_send_delivers_through_worker(notification_backend, celery_app):
    service, adapter = build_service(notification_backend)

    notification = service.create_notification(**create_kwargs())

    # send() enqueued the id and the eager task ran delayed_send, which delivered it.
    assert len(adapter.sent_emails) == 1
    assert adapter.sent_emails[0][0].id == notification.id
    assert notification_backend.get_notification(notification.id).status == (
        NotificationStatus.SENT.value
    )


def test_send_enqueues_through_queue_service(notification_backend, celery_app):
    from unittest.mock import patch

    service, adapter = build_service(notification_backend)

    # send() hands the id to the queue service instead of delivering in the foreground. With
    # the enqueue suppressed, no worker runs, so nothing is delivered.
    with patch.object(service.notification_queue_service, "enqueue_notification") as mock_enqueue:
        notification = service.create_notification(**create_kwargs())

    mock_enqueue.assert_called_once_with(notification.id)
    assert len(adapter.sent_emails) == 0


def test_background_send_swallows_render_error(notification_backend, celery_app):
    service, adapter = build_service(
        notification_backend, renderer=FakeTemplateRendererWithException()
    )

    # A worker task never re-raises: send_notification logs and returns. The notification is
    # marked failed rather than delivered.
    notification = service.create_notification(**create_kwargs())

    assert len(adapter.sent_emails) == 0
    assert notification_backend.get_notification(notification.id).status == (
        NotificationStatus.FAILED.value
    )


def test_background_send_with_attachments(notification_backend, celery_app):
    service, adapter = build_service(notification_backend)

    attachments = [
        NotificationAttachment(
            file=BytesIO(b"hello world"),
            filename="hello.txt",
            content_type="text/plain",
            description="a greeting",
        ),
    ]
    notification = service.create_notification(**create_kwargs(attachments=attachments))

    # Attachments now ride the background path: the worker reloads the stored attachment from
    # the backend rather than pulling it out of a serialized payload.
    assert len(adapter.sent_emails) == 1
    delivered_attachments = adapter.sent_emails[0][2]
    assert [a["filename"] for a in delivered_attachments] == ["hello.txt"]
    assert notification_backend.get_notification(notification.id).status == (
        NotificationStatus.SENT.value
    )


def test_scheduled_notification_is_not_sent_immediately(notification_backend, celery_app):
    import datetime

    service, adapter = build_service(notification_backend)

    send_after = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=1)
    service.create_notification(**create_kwargs(send_after=send_after))

    # A future notification is persisted but neither enqueued nor delivered yet.
    assert len(adapter.sent_emails) == 0


def test_one_off_notification_background_send(notification_backend, celery_app):
    from vintasend.services.dataclasses import OneOffNotification

    service, adapter = build_service(notification_backend)

    one_off = OneOffNotification(
        id=uuid.uuid4(),
        email_or_phone="test@example.com",
        first_name="John",
        last_name="Doe",
        notification_type=NotificationTypes.EMAIL.value,
        title="One-off",
        body_template="body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="subject.txt",
        preheader_template="preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
    )
    notification_backend.notifications.append(one_off)
    notification_backend._store_notifications()

    service.send(one_off)

    assert len(adapter.sent_emails) == 1
    assert notification_backend.get_notification(one_off.id).status == (
        NotificationStatus.SENT.value
    )
