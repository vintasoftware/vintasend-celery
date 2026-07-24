import asyncio
import logging

from celery import Celery  # type: ignore
from vintasend.tasks.background_tasks import async_send_notification, send_notification


logger = logging.getLogger(__name__)


def send_notification_task_factory(celery_app: Celery):
    """Register VintaSend's `send_notification` as a Celery task on `celery_app`.

    The resulting task takes a single notification id. The worker resolves its own
    `NotificationService` from `NOTIFICATION_SERVICE_FACTORY` (or the host passes one
    explicitly), reloads the notification and delivers it. Hand the returned task to a
    `CeleryNotificationQueueService` so `send()` can enqueue against it.
    """
    return celery_app.task(send_notification)


def async_send_notification_task_factory(celery_app: Celery):
    """Register VintaSend's async `send_notification` as a Celery task on `celery_app`.

    Celery workers run synchronously, so the coroutine is driven with `asyncio.run` inside a
    plain task. Use this when `NOTIFICATION_SERVICE_FACTORY` returns an
    `AsyncIONotificationService`; pair it with an `AsyncIOCeleryNotificationQueueService`.
    """

    @celery_app.task
    def async_send_notification_task(notification_id):
        asyncio.run(async_send_notification(notification_id))

    return async_send_notification_task
