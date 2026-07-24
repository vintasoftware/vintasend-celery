"""A minimal host wiring used by the test suite as a reference for VintaSend 2.0 + Celery.

The web process sends through a `NotificationService` whose adapter is a background adapter
and whose queue service is a `CeleryNotificationQueueService`. `send()` enqueues the
notification id onto Celery; the Celery task runs VintaSend's `send_notification`, which
reloads the notification from the backend and delivers it through the adapter's `send()`.
Tasks run eagerly here (`task_always_eager`), so the whole round trip happens in-process.

The worker resolves its `NotificationService` in one of two documented ways:

* `NOTIFICATION_SERVICE_FACTORY` -- the production path. The worker builds its own service
  once per process from that setting. It needs a live database session, so the host, not
  VintaSend, supplies the factory.
* passing a `notification_service` to `send_notification` explicitly -- what the helpers here
  do, so the example stays independent of Django/Flask/FastAPI (in a plain-Python process
  VintaSend only reads `NOTIFICATION_SERVICE_FACTORY` from the environment once a framework
  is detected).
"""

import uuid

from celery import Celery  # type: ignore
from vintasend.services.notification_adapters.stubs.fake_adapter import FakeAsyncEmailAdapter
from vintasend.services.notification_service import NotificationService
from vintasend.tasks.background_tasks import send_notification

from vintasend_celery.services.notification_queue_services.celery_queue_service import (
    CeleryNotificationQueueService,
)


celery_app = Celery(
    "tasks", broker="memory://localhost/", backend="cache+memory://", task_always_eager=True
)


class CeleryFakeEmailAdapter(FakeAsyncEmailAdapter):
    """Background fake email adapter: `send()` is the real delivery, run by the worker."""


def make_celery_queue_service(
    notification_service: NotificationService,
) -> CeleryNotificationQueueService:
    """Wire a Celery queue service whose worker task delivers through `notification_service`.

    Registers a Celery task that hands the notification id to `send_notification` along with
    an explicit service, then returns a `CeleryNotificationQueueService` bound to that task.
    A real host would instead register `send_notification` with `send_notification_task_factory`
    and let the worker build its service from `NOTIFICATION_SERVICE_FACTORY`.

    A unique task name keeps repeated wirings on the shared `celery_app` from colliding in the
    task registry -- each call captures its own service.
    """

    @celery_app.task(name=f"vintasend_celery.send_notification_task.{uuid.uuid4().hex}")
    def send_notification_task(notification_id):
        send_notification(notification_id, notification_service=notification_service)

    return CeleryNotificationQueueService(send_notification_task=send_notification_task)
