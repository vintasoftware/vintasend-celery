import asyncio
import uuid
from unittest.mock import MagicMock

import pytest
from vintasend.exceptions import NotificationSendError

from vintasend_celery.services.notification_queue_services.celery_queue_service import (
    AsyncIOCeleryNotificationQueueService,
    CeleryNotificationQueueService,
)


def test_enqueue_notification_calls_task_delay():
    task = MagicMock()
    queue_service = CeleryNotificationQueueService(send_notification_task=task)

    notification_id = uuid.uuid4()
    queue_service.enqueue_notification(notification_id)

    task.delay.assert_called_once_with(notification_id)


def test_enqueue_notification_accepts_task_as_class_attribute():
    task = MagicMock()

    class BoundQueueService(CeleryNotificationQueueService):
        send_notification_task = task

    # Resolvable from a NOTIFICATION_QUEUE_SERVICE import string, which instantiates with no
    # arguments.
    queue_service = BoundQueueService()
    queue_service.enqueue_notification(42)

    task.delay.assert_called_once_with(42)


def test_enqueue_notification_wraps_broker_failure():
    task = MagicMock()
    task.delay.side_effect = RuntimeError("broker unreachable")
    queue_service = CeleryNotificationQueueService(send_notification_task=task)

    with pytest.raises(NotificationSendError):
        queue_service.enqueue_notification(uuid.uuid4())


def test_asyncio_enqueue_notification_calls_task_delay():
    task = MagicMock()
    queue_service = AsyncIOCeleryNotificationQueueService(send_notification_task=task)

    notification_id = uuid.uuid4()
    asyncio.run(queue_service.enqueue_notification(notification_id))

    task.delay.assert_called_once_with(notification_id)


def test_asyncio_enqueue_notification_wraps_broker_failure():
    task = MagicMock()
    task.delay.side_effect = RuntimeError("broker unreachable")
    queue_service = AsyncIOCeleryNotificationQueueService(send_notification_task=task)

    with pytest.raises(NotificationSendError):
        asyncio.run(queue_service.enqueue_notification(uuid.uuid4()))
