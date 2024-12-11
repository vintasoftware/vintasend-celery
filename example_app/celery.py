from datetime import datetime
from decimal import Decimal

import pytest
from celery import Celery  # type: ignore
from vintasend.services.notification_adapters.stubs.fake_adapter import (
    FakeEmailAdapter,
)
from vintasend.services.notification_backends.stubs.fake_backend import (
    Config,
    FakeFileBackend,
    FakeFileBackendWithNonSerializableKWArgs,
)
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import (
    FakeTemplateRenderer,
)
from vintasend.tasks.background_tasks import send_notification

from vintasend_celery.services.notification_adapters.celery_adapter_factory import (
    CeleryNotificationAdapter,
)


celery_app = Celery(
    "tasks", broker="memory://localhost/", backend="cache+memory://", task_always_eager=True
)


@celery_app.task
def send_notification_task(*args, **kwargs):
    send_notification(*args, **kwargs)


class AsyncCeleryFakeEmailAdapter(
    CeleryNotificationAdapter[FakeFileBackend, FakeTemplateRenderer],
    FakeEmailAdapter[FakeFileBackend, FakeTemplateRenderer],
):
    send_notification_task = send_notification_task


class AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs(
    CeleryNotificationAdapter[FakeFileBackendWithNonSerializableKWArgs, FakeTemplateRenderer],
    FakeEmailAdapter[FakeFileBackendWithNonSerializableKWArgs, FakeTemplateRenderer],
):
    send_notification_task = send_notification_task
    config: Config

    def serialize_config(self) -> dict[str, str]:
        return {
            "config_a": str(self.config.config_a),
            "config_b": self.config.config_b.isoformat(),
        }

    @staticmethod
    def restore_config(config: dict[str, str]) -> Config:
        return Config(
            config_a=Decimal(config["config_a"]),
            config_b=datetime.fromisoformat(config["config_b"]),
        )


@pytest.fixture(scope="session")
def celery_includes():
    return [
        "proj.tests.tasks",
        "proj.tests.celery_signal_handlers",
    ]
