import uuid
from unittest.mock import patch

import pytest
from vintasend.constants import NotificationStatus, NotificationTypes
from vintasend.services.dataclasses import Notification
from vintasend.services.notification_backends.stubs.fake_backend import (
    Config,
    FakeFileBackend,
    FakeFileBackendWithNonSerializableKWArgs,
)
from vintasend.services.notification_service import NotificationService, register_context
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import (
    FakeTemplateRenderer,
    FakeTemplateRendererWithException,
)

from example_app.celery import (
    AsyncCeleryFakeEmailAdapter,
    AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs,
)


@pytest.fixture()
def celery_app():
    from example_app.celery import celery_app
    celery_app.conf.update(task_always_eager=True)
    return celery_app


@pytest.fixture(scope="function")
def notification_backend():
    backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    yield backend
    backend.clear()


@pytest.fixture()
def renderer():
    return FakeTemplateRenderer()


@pytest.fixture()
def notification_service(notification_backend, renderer):
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService[AsyncCeleryFakeEmailAdapter, FakeFileBackend](
        [async_adapter],
        notification_backend,
    )

    return notification_service


def create_notification():
    register_context("test_context")(create_notification_context)
    return Notification(
        id=uuid.uuid4(),
        user_id=1,
        notification_type=NotificationTypes.EMAIL.value,
        title="Test Notification",
        body_template="vintasend_django/emails/test/test_templated_email_body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="vintasend_django/emails/test/test_templated_email_subject.txt",
        preheader_template="vintasend_django/emails/test/test_templated_email_preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
    )


def create_notification_context(test):
    if test != "test":
        raise ValueError("Invalid test value")
    return {"foo": "bar"}


def test_send_notification(notification_backend, notification_service, celery_app, celery_worker):
    notification = create_notification()
    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    notification_service.send(notification)
    assert len(notification_backend.notifications) == 1


def test_send_notification_with_render_error(notification_backend, celery_app, celery_worker):
    notification = create_notification()

    renderer = FakeTemplateRendererWithException()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    with patch("vintasend.tasks.background_tasks.logger.exception") as mock_log_exception:
        notification_service.send(notification)

    mock_log_exception.assert_called_once()

    assert len(async_adapter.sent_emails) == 0


def test_backend_with_non_serializable_kwargs(renderer, celery_app, celery_worker):
    notification = create_notification()
    config = Config()
    backend = FakeFileBackendWithNonSerializableKWArgs(
        database_file_name="celery-adapter-tests-notifications.json",
        config=config,
    )
    async_adapter = AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs(
        template_renderer=renderer, backend=backend, config=config
    )

    notification_service = NotificationService(
        [async_adapter],
        backend,
    )

    backend.notifications.append(notification)
    backend._store_notifications()

    notification_service.send(notification)

    assert len(backend.notifications) == 1

    backend.clear()
