import uuid
from unittest import TestCase
from unittest.mock import patch
import pytest

from celery import Celery  # type: ignore
from vintasend.constants import NotificationStatus, NotificationTypes
from vintasend.exceptions import NotificationSendError
from vintasend.services.dataclasses import Notification
from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import FakeTemplateRenderer, FakeTemplateRendererWithException
from vintasend.services.notification_service import NotificationService, register_context
from vintasend_celery.services.notification_adapters.celery_adapter_factory import (
    CeleryNotificationAdapter
)
from vintasend.services.notification_adapters.stubs.fake_adapter import (
    FakeEmailAdapter,
)


celery_app = Celery('tasks', broker='amqp://', backend='rpc://')

class AsyncCeleryFakeEmailAdapter(
    CeleryNotificationAdapter[FakeFileBackend, FakeTemplateRenderer],
    FakeEmailAdapter[FakeFileBackend, FakeTemplateRenderer],
):
    celery_app = celery_app


class AsyncCeleryFakeEmailAdapterTestCase(TestCase):
    def setUp(self):
        celery_app.conf.update(task_always_eager=True)
        self.backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")

        self.renderer = FakeTemplateRenderer()
        self.async_adapter = AsyncCeleryFakeEmailAdapter(template_renderer=self.renderer, backend=self.backend)
        
        self.notification_service = NotificationService[AsyncCeleryFakeEmailAdapter, FakeFileBackend](
            [self.async_adapter],
            self.backend,
        )

    def tearDown(self) -> None:
        FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json").clear()
        return super().tearDown()

    def create_notification(self):
        register_context("test_context")(self.create_notification_context)
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

    def create_notification_context(self, test):
        if test != "test":
            raise ValueError("Invalid test value")
        return {"foo": "bar"}

    def test_send_notification(self):
        notification = self.create_notification()
        self.backend.notifications.append(notification)
        self.backend._store_notifications()
        
        self.notification_service.send(notification)
        assert len(self.backend.notifications) == 1

    def test_send_notification_with_render_error(self):
        notification = self.create_notification()

        renderer = FakeTemplateRendererWithException()
        async_adapter = AsyncCeleryFakeEmailAdapter(template_renderer=renderer, backend=self.backend)
        
        notification_service = NotificationService[
            AsyncCeleryFakeEmailAdapter, 
            FakeFileBackend
        ](
            [async_adapter],
            self.backend,
        )

        self.backend.notifications.append(notification)
        self.backend._store_notifications()

        with patch("vintasend_celery.services.notification_adapters.celery_adapter_factory.logger.exception") as mock_log_exception:
            notification_service.send(notification)

        mock_log_exception.assert_called_once()

        assert len(self.async_adapter.sent_emails) == 0