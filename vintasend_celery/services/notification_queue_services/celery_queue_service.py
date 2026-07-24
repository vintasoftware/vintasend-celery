"""Celery-backed queue services for VintaSend background sending.

In VintaSend 2.0 the queue payload is the notification id and nothing else. When
`NotificationService.send()` meets a background adapter it hands that id to its configured
queue service; the queue service enqueues it and a Celery worker later runs VintaSend's
`send_notification` task, which reloads the notification from the backend and delivers it.

These queue services are the Celery half of that path: `enqueue_notification` calls
`.delay(notification_id)` on the Celery task the host registered for `send_notification`
(build it with `send_notification_task_factory`). The task reference is supplied per host,
either through `__init__` or as a class attribute on a subclass, because the same VintaSend
`send_notification` function is registered against the host's own `Celery` app.
"""

import uuid

from celery import Task  # type: ignore
from vintasend.exceptions import NotificationSendError
from vintasend.services.notification_queue_services.asyncio_base import (
    AsyncIOBaseNotificationQueueService,
)
from vintasend.services.notification_queue_services.base import BaseNotificationQueueService


class CeleryNotificationQueueService(BaseNotificationQueueService):
    """Enqueue notification ids onto Celery for a background worker to deliver.

    The Celery task that runs VintaSend's `send_notification` is supplied per host, since it
    must be registered against that host's own `Celery` app. Pass it to `__init__`, or set it
    as the `send_notification_task` class attribute on a subclass so the service can be
    resolved from a `NOTIFICATION_QUEUE_SERVICE` import string (which instantiates with no
    arguments).
    """

    send_notification_task: Task

    def __init__(self, send_notification_task: Task | None = None) -> None:
        if send_notification_task is not None:
            self.send_notification_task = send_notification_task

    def enqueue_notification(self, notification_id: int | str | uuid.UUID) -> None:
        try:
            self.send_notification_task.delay(notification_id)
        except Exception as e:  # noqa: BLE001
            # The base class requires broker and transport failures (for example
            # kombu.exceptions.OperationalError) to leave here as a NotificationError
            # subclass, so NotificationService.send() can honor raise_on_failed_send instead
            # of letting a raw broker exception escape its adapter loop.
            raise NotificationSendError(
                f"Failed to enqueue notification {notification_id} onto Celery"
            ) from e


class AsyncIOCeleryNotificationQueueService(AsyncIOBaseNotificationQueueService):
    """AsyncIO counterpart of `CeleryNotificationQueueService`.

    For hosts whose `NOTIFICATION_SERVICE_FACTORY` returns an `AsyncIONotificationService`.
    Enqueueing is still a quick, synchronous `.delay()` on the Celery task -- Celery's own
    client is not async -- so this only wraps that call in an `async def` to satisfy the
    AsyncIO queue-service seam. Register the worker task with
    `async_send_notification_task_factory`.
    """

    send_notification_task: Task

    def __init__(self, send_notification_task: Task | None = None) -> None:
        if send_notification_task is not None:
            self.send_notification_task = send_notification_task

    async def enqueue_notification(self, notification_id: int | str | uuid.UUID) -> None:
        try:
            self.send_notification_task.delay(notification_id)
        except Exception as e:  # noqa: BLE001
            raise NotificationSendError(
                f"Failed to enqueue notification {notification_id} onto Celery"
            ) from e
