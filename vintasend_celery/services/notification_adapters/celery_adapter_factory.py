import datetime
import uuid
from typing import Generic, TypeVar, cast

from celery import Task  # type: ignore
from vintasend.services.dataclasses import Notification, NotificationContextDict
from vintasend.services.notification_adapters.async_base import (
    AsyncBaseNotificationAdapter,
    NotificationDict,
)
from vintasend.services.notification_backends.base import BaseNotificationBackend
from vintasend.services.notification_template_renderers.base import BaseNotificationTemplateRenderer


B = TypeVar("B", bound=BaseNotificationBackend)
T = TypeVar("T", bound=BaseNotificationTemplateRenderer)


class CeleryNotificationAdapter(Generic[B, T], AsyncBaseNotificationAdapter[B, T]):
    send_notification_task: Task

    def delayed_send(self, notification_dict: NotificationDict, context_dict: dict) -> None:
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

    def _convert_to_uuid(self, value: str) -> uuid.UUID | str:
        try:
            return uuid.UUID(value)
        except ValueError:
            return value

    def notification_from_dict(self, notification_dict: NotificationDict) -> "Notification":
        send_after = (
            datetime.datetime.fromisoformat(notification_dict["send_after"])
            if notification_dict["send_after"]
            else None
        )
        return Notification(
            id=(
                self._convert_to_uuid(notification_dict["id"])
                if isinstance(notification_dict["id"], str)
                else notification_dict["id"]
            ),
            user_id=(
                self._convert_to_uuid(notification_dict["user_id"])
                if isinstance(notification_dict["user_id"], str)
                else notification_dict["user_id"]
            ),
            context_kwargs={
                key: self._convert_to_uuid(value) if isinstance(value, str) else value
                for key, value in notification_dict["context_kwargs"].items()
            },
            notification_type=notification_dict["notification_type"],
            title=notification_dict["title"],
            body_template=notification_dict["body_template"],
            context_name=notification_dict["context_name"],
            subject_template=notification_dict["subject_template"],
            preheader_template=notification_dict["preheader_template"],
            status=notification_dict["status"],
            context_used=notification_dict["context_used"],
            send_after=send_after,
        )

    def send(self, notification: "Notification", context: "NotificationContextDict") -> None:
        self.send_notification_task.delay(
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
