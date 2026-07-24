# Release Notes

## Version 2.0.0 (2026-07-23)

Reshaped for VintaSend 2.0, whose background path carries only a notification id: the worker
reloads the notification from the backend and delivers it, so the old serialize-into-the-payload
adapter is gone.

### 🚀 Changes

* **New `CeleryNotificationQueueService`** (and `AsyncIOCeleryNotificationQueueService`) under
  `vintasend_celery.services.notification_queue_services.celery_queue_service`. This is the piece
  a host now wires as its `notification_queue_service`: `enqueue_notification(notification_id)`
  calls `.delay(notification_id)` on the Celery task that runs VintaSend's `send_notification`.
  Broker failures are re-raised as `NotificationSendError`, so `raise_on_failed_send` keeps
  working.
* **New `async_send_notification_task_factory`** for AsyncIO hosts, alongside the existing
  `send_notification_task_factory` and `periodic_send_pending_notifications_task_factory`. The
  factories register VintaSend's id-only entrypoints as Celery tasks.
* **Removed `CeleryNotificationAdapter`, `PlaceholderAttachmentFile`, and the
  `notification_to_dict` / `notification_from_dict` serialization helpers.** With the id-only
  payload there is nothing to serialize. A background adapter now subclasses VintaSend's
  `BackgroundNotificationAdapter` (or `AsyncIOBackgroundNotificationAdapter`) directly and puts
  its delivery in `send()`.
* **Attachments now work on the background path** for free, because the worker loads the real
  notification (and its stored attachments) from the backend instead of a serialized payload.

### ⚠️ Backwards compatibility

This is a breaking release; upgrade in lockstep with `vintasend` 2.0.0. Hosts must:

* register a `CeleryNotificationQueueService` as the service's `notification_queue_service` (or
  set `NOTIFICATION_QUEUE_SERVICE`);
* point `NOTIFICATION_SERVICE_FACTORY` at a callable that builds the worker's
  `NotificationService`, or pass one to `send_notification` explicitly;
* move any background-delivery code out of the old adapter's `delayed_send` into `send()`, and
  delete the eight `serialize_*` / `restore_*` methods;
* drain or dual-register the Celery queue before deploying the 2.0 worker, since the task
  signature changed from many parameters to a single notification id.

See the repository's `MIGRATION_TO_2.0.0.md` for the full procedure.

## Version 1.2.0 (2026-06-14)

* Release coordination bump to track `vintasend` / `vintasend-django` 1.2.0 (in-app list-all,
  pagination + count helpers, and bulk mark-as-read). No code changes — this package ships only
  Celery adapter/tasks and does not subclass the notification backend ABC, so the new abstract
  methods do not affect it.

## Version 1.1.3 (2026-06-12)

* Bump vintasend to 1.1.3

## Version 1.1.2 (2026-06-12)

* Bump vintasend to 1.1.2

## Version 1.0.1 (2025-09-16)

### 🚀 Major Features

* Support for one-off notifications (to non-users)
* Support for attachments

## Version 0.1.4 (Initial Release)

Initial version of VintaSend with core notification functionality.
