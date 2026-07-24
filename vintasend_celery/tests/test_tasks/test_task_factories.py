import uuid
from unittest.mock import patch

import pytest
from celery import Celery  # type: ignore
from vintasend.constants import NotificationStatus, NotificationTypes
from vintasend.services.dataclasses import Notification
from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
from vintasend.services.notification_service import NotificationService, register_context
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import (
    FakeTemplateRenderer,
)

from example_app.celery import CeleryFakeEmailAdapter, make_celery_queue_service
from vintasend_celery.tasks.background_tasks import (
    async_send_notification_task_factory,
    send_notification_task_factory,
)
from vintasend_celery.tasks.periodic_tasks import (
    periodic_send_pending_notifications_task_factory,
)


@pytest.fixture()
def celery_app():
    return Celery(
        "task-factory-tests",
        broker="memory://localhost/",
        backend="cache+memory://",
        task_always_eager=True,
    )


@pytest.fixture()
def notification_backend():
    backend = FakeFileBackend(database_file_name="celery-task-factory-tests-notifications.json")
    yield backend
    backend.clear()


def make_context(test):
    return {"foo": "bar"}


@pytest.fixture(autouse=True)
def _register_context():
    register_context("test_context")(make_context)


def build_service(backend):
    adapter = CeleryFakeEmailAdapter(template_renderer=FakeTemplateRenderer(), backend=backend)
    return NotificationService([adapter], backend), adapter


def create_pending_notification(service):
    return service.create_notification(
        user_id=1,
        notification_type=NotificationTypes.EMAIL.value,
        title="Test",
        body_template="body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        subject_template="subject.txt",
        preheader_template="preheader.html",
    )


def persist_pending_notification(backend):
    """Write a due, pending notification straight to the backend, without sending it."""
    notification = Notification(
        id=uuid.uuid4(),
        user_id=1,
        notification_type=NotificationTypes.EMAIL.value,
        title="Test",
        body_template="body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="subject.txt",
        preheader_template="preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
    )
    backend.notifications.append(notification)
    backend._store_notifications()
    return notification


def test_send_notification_task_factory_delivers_by_id(notification_backend, celery_app):
    service, adapter = build_service(notification_backend)
    # Create without a queue service so nothing delivers yet; the notification is left pending.
    notification = create_pending_notification(service)
    assert len(adapter.sent_emails) == 0

    task = send_notification_task_factory(celery_app)
    assert task.name in celery_app.tasks
    # Call the registered task's function directly: a live service cannot be JSON-serialized
    # into a broker message, so it is wired in here rather than passed through delay().
    task.run(notification.id, notification_service=service)

    assert len(adapter.sent_emails) == 1
    assert notification_backend.get_notification(notification.id).status == (
        NotificationStatus.SENT.value
    )


def test_periodic_task_factory_sends_all_pending(notification_backend, celery_app):
    service, adapter = build_service(notification_backend)
    service.register_queue_service(make_celery_queue_service(service))
    persist_pending_notification(notification_backend)
    persist_pending_notification(notification_backend)
    assert len(adapter.sent_emails) == 0

    task = periodic_send_pending_notifications_task_factory(celery_app)
    assert task.name in celery_app.tasks
    # Each pending notification is enqueued and delivered by the eager worker task.
    task.run(notification_service=service)

    assert len(adapter.sent_emails) == 2


def test_async_send_notification_task_factory_drives_coroutine(celery_app):
    task = async_send_notification_task_factory(celery_app)

    with patch(
        "vintasend_celery.tasks.background_tasks.async_send_notification"
    ) as mock_async_send:
        # Return a real coroutine so asyncio.run has something to await.
        async def _noop(notification_id):
            return None

        mock_async_send.side_effect = _noop
        task.delay("notification-id")

    mock_async_send.assert_called_once_with("notification-id")
