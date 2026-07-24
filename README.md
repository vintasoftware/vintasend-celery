# VintaSend Celery

Celery integration for VintaSend. It lets a `NotificationService` hand background sends to a
Celery worker.

This package does not implement a delivery adapter. It provides the queue service that enqueues
a notification id and the task factories that register VintaSend's worker entrypoints as Celery
tasks. The worker reloads the notification from your backend and delivers it through your own
background adapter.

## Installation

```shell
pip install vintasend vintasend-celery
```

## How it fits together (VintaSend 2.0)

The background payload is just the notification id:

1. Your adapter subclasses VintaSend's `BackgroundNotificationAdapter` and puts its delivery in
   `send()`.
2. `NotificationService.send()` sees the background adapter and hands the notification id to its
   `notification_queue_service` — the `CeleryNotificationQueueService` from this package.
3. The queue service calls `.delay(notification_id)` on the Celery task that runs VintaSend's
   `send_notification`.
4. The worker rebuilds a `NotificationService` (from `NOTIFICATION_SERVICE_FACTORY`, or one you
   pass in) and calls `delayed_send(notification_id)`, which reloads the notification, generates
   the context at delivery time, and calls your adapter's `send()`.

Attachments work on this path without extra effort: the worker reads them from the backend.

## Quick start

Register the worker task and wire the queue service against it:

```python
from celery import Celery
from vintasend.services.notification_service import NotificationService
from vintasend.tasks.background_tasks import send_notification

from vintasend_celery.services.notification_queue_services.celery_queue_service import (
    CeleryNotificationQueueService,
)
from vintasend_celery.tasks.background_tasks import send_notification_task_factory

celery_app = Celery("myapp", broker="redis://localhost:6379/0")

# Register VintaSend's id-only worker entrypoint as a Celery task.
send_notification_task = send_notification_task_factory(celery_app)

# The web process builds its service with this queue service.
queue_service = CeleryNotificationQueueService(send_notification_task=send_notification_task)

notification_service = NotificationService(
    notification_adapters=[MyBackgroundEmailAdapter(...)],  # subclasses BackgroundNotificationAdapter
    notification_backend=MyBackend(...),
    notification_queue_service=queue_service,
)
```

The worker needs to rebuild the service from scratch to get a live database session, so point
`NOTIFICATION_SERVICE_FACTORY` at a callable that returns a ready `NotificationService`:

```python
# myapp/worker.py
def notification_service_factory():
    return NotificationService(
        notification_adapters=[MyBackgroundEmailAdapter(...)],
        notification_backend=MyBackend(...),
    )
```

```bash
export NOTIFICATION_SERVICE_FACTORY="myapp.worker.notification_service_factory"
```

The web and worker processes must read the same `NOTIFICATION_BACKEND` so the id resolves to the
same notification in both.

### Periodic draining

To flush notifications whose `send_after` has arrived, register the periodic task and schedule it
on Celery beat:

```python
from vintasend_celery.tasks.periodic_tasks import (
    periodic_send_pending_notifications_task_factory,
)

periodic_task = periodic_send_pending_notifications_task_factory(celery_app)
```

### AsyncIO hosts

If your `NOTIFICATION_SERVICE_FACTORY` returns an `AsyncIONotificationService`, use the AsyncIO
queue service and task factory instead:

```python
from vintasend_celery.services.notification_queue_services.celery_queue_service import (
    AsyncIOCeleryNotificationQueueService,
)
from vintasend_celery.tasks.background_tasks import async_send_notification_task_factory

async_task = async_send_notification_task_factory(celery_app)
queue_service = AsyncIOCeleryNotificationQueueService(send_notification_task=async_task)
```

## Upgrading from 1.x

This release is a breaking change. See `RELEASE_NOTES.md` and the repository's
`MIGRATION_TO_2.0.0.md` for the full upgrade procedure, including draining or dual-registering
the Celery queue before deploying the 2.0 worker.
