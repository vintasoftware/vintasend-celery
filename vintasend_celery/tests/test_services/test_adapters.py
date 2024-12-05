from datetime import datetime
from decimal import Decimal
import uuid
from unittest import TestCase
from unittest.mock import patch

from celery import Celery  # type: ignore
from vintasend.constants import NotificationStatus, NotificationTypes
from vintasend.services.dataclasses import Notification
from vintasend.services.notification_backends.stubs.fake_backend import (
    FakeFileBackend, FakeFileBackendWithNonSerializableKWArgs, Config
)
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import (
    FakeTemplateRenderer, FakeTemplateRendererWithException
)
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

class AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs(
    CeleryNotificationAdapter[FakeFileBackendWithNonSerializableKWArgs, FakeTemplateRenderer],
    FakeEmailAdapter[FakeFileBackendWithNonSerializableKWArgs, FakeTemplateRenderer],
):
    celery_app = celery_app
    config: Config

    def serialize_config(self) -> dict:
        return {
            "config_a": str(self.config.config_a),
            "config_b": self.config.config_b.isoformat(),
        }

    def restore_config(self, config: dict) -> Config:
        self.config = Config(
            config_a=Decimal(config["config_a"]),
            config_b=datetime.fromisoformat(config["config_b"]),
        )
        return self.config


class AsyncCeleryFakeEmailAdapterTestCase(TestCase):
    def setUp(self):
        celery_app.conf.update(task_always_eager=True)
        self.backend = FakeFileBackend(
            database_file_name="celery-adapter-tests-notifications.json"
        )

        self.renderer = FakeTemplateRenderer()
        self.async_adapter = AsyncCeleryFakeEmailAdapter(
            template_renderer=self.renderer, backend=self.backend
        )
        
        self.notification_service = NotificationService[
            AsyncCeleryFakeEmailAdapter, FakeFileBackend
        ](
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
        
        notification_service = NotificationService(
            [async_adapter],
            self.backend,
        )

        self.backend.notifications.append(notification)
        self.backend._store_notifications()

        with patch(
            "vintasend.tasks.background_tasks.logger.exception"
        ) as mock_log_exception:
            notification_service.send(notification)

        mock_log_exception.assert_called_once()

        assert len(self.async_adapter.sent_emails) == 0

    def test_backend_with_non_serializable_kwargs(self):
        notification = self.create_notification()
        config = Config()
        backend = FakeFileBackendWithNonSerializableKWArgs(
            database_file_name="celery-adapter-tests-notifications.json",
            config=config,
        )
        async_adapter = AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs(
            template_renderer=self.renderer, backend=backend, config=config
        )
        
        notification_service = NotificationService(
            [async_adapter],
            backend,
        )

        backend.notifications.append(notification)
        backend._store_notifications()

        notification_service.send(notification)

        assert len(backend.notifications) == 1