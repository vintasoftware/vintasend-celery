import datetime
from typing import cast, Generic, TypeVar

from celery import Celery  # type: ignore

from vintasend.services.dataclasses import Notification, NotificationContextDict
from vintasend.services.notification_adapters.async_base import AsyncBaseNotificationAdapter, NotificationDict
from vintasend.services.notification_backends.base import BaseNotificationBackend
from vintasend.services.notification_template_renderers.base import BaseNotificationTemplateRenderer
from vintasend_celery.tasks.background_tasks import send_notification_task_factory


B = TypeVar("B", bound=BaseNotificationBackend)
T = TypeVar("T", bound=BaseNotificationTemplateRenderer)

class CeleryNotificationAdapter(Generic[B, T], AsyncBaseNotificationAdapter[B, T]):
    celery_app: Celery

    def delayed_send(self, notification_dict: dict, context_dict: dict) -> None:
        notification = self.notification_from_dict(notification_dict)
        context = NotificationContextDict(**context_dict)
        super().send(notification, context)  # type: ignore

    def notification_to_dict(self, notification: "Notification") -> NotificationDict:
        non_serializable_fields = ["send_after"]
        serialized_notification = {}
        for field in notification.__dataclass_fields__.keys():
            if field in non_serializable_fields:
                continue
            serialized_notification[field] = getattr(notification, field)

        serialized_notification["send_after"] = (
            notification.send_after.isoformat() if notification.send_after else None
        )

        return cast(NotificationDict, serialized_notification)

    def notification_from_dict(self, notification_dict: dict) -> "Notification":
        notification_dict["send_after"] = (
            datetime.datetime.fromisoformat(notification_dict["send_after"])
            if notification_dict["send_after"]
            else None
        )
        return Notification(**notification_dict)

    def send(self, notification: "Notification", context: "NotificationContextDict") -> None:
        send_notification_task = send_notification_task_factory(self.celery_app)
        send_notification_task.delay(
            notification=self.notification_to_dict(notification),
            context=context,
            backend=self.backend.backend_import_str,
            adapters=[
                (
                    self.adapter_import_str,
                    self.template_renderer.template_renderer_import_str,
                )
            ],
            backend_kwargs=self.backend.backend_kwargs,
            config=self.serialize_config(),
        )
