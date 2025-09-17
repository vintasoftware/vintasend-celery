import datetime
import uuid
from typing import Any, Generic, TypeVar

from celery import Task  # type: ignore
from vintasend.services.dataclasses import (
    AttachmentFile,
    Notification,
    NotificationContextDict,
    OneOffNotification,
    StoredAttachment,
)
from vintasend.services.notification_adapters.async_base import (
    AsyncBaseNotificationAdapter,
    NotificationDict,
    OneOffNotificationDict,
)
from vintasend.services.notification_backends.base import BaseNotificationBackend
from vintasend.services.notification_template_renderers.base import BaseNotificationTemplateRenderer


B = TypeVar("B", bound=BaseNotificationBackend)
T = TypeVar("T", bound=BaseNotificationTemplateRenderer)


class PlaceholderAttachmentFile(AttachmentFile):
    """
    Placeholder for AttachmentFile.
    All file operations raise NotImplementedError and must be handled by the caller.
    Use this class only when the actual file is expected to be retrieved by the backend.
    """
    def __init__(self, attachment_id):
        self.attachment_id = attachment_id

    def read(self) -> bytes:
        """
        Attempting to read will raise NotImplementedError.
        Caller must handle this exception and retrieve the file from the backend.
        """
        raise NotImplementedError("File must be retrieved from backend")

    def stream(self):
        """
        Attempting to stream will raise NotImplementedError.
        Caller must handle this exception and retrieve the file from the backend.
        """
        raise NotImplementedError("File must be retrieved from backend")

    def url(self, expires_in: int = 3600) -> str:
        """
        Attempting to get a URL will raise NotImplementedError.
        Caller must handle this exception and retrieve the file from the backend.
        """
        raise NotImplementedError("File must be retrieved from backend")

    def delete(self) -> None:
        """
        Attempting to delete will raise NotImplementedError.
        Caller must handle this exception and perform deletion via the backend.
        """
        raise NotImplementedError("File must be retrieved from backend")


class CeleryNotificationAdapter(Generic[B, T], AsyncBaseNotificationAdapter[B, T]):
    send_notification_task: Task

    def delayed_send(self, notification_dict: "NotificationDict | OneOffNotificationDict", context_dict: dict) -> None:
        # Convert the typed dict to a regular dict for our internal processing
        notification_dict_any = dict(notification_dict)
        notification = self.notification_from_dict(notification_dict_any)
        context = NotificationContextDict(**context_dict)
        super().send(notification, context)  # type: ignore

    def notification_to_dict(self, notification: "Notification | OneOffNotification") -> dict[str, Any]:
        """Convert a notification (regular or one-off) to a dictionary for serialization."""
        non_serializable_fields = ["send_after", "attachments", "created_at", "updated_at"]
        serialized_notification = {}

        for field in notification.__dataclass_fields__.keys():
            if field in non_serializable_fields:
                continue
            serialized_notification[field] = getattr(notification, field)

        # Handle send_after serialization
        serialized_notification["send_after"] = (
            notification.send_after.isoformat() if notification.send_after else None
        )

        # Handle attachments separately - for now we'll add a custom field
        # This will be handled by the background task when processing
        if hasattr(notification, 'attachments') and notification.attachments:
            serialized_notification["_attachments"] = [
                self._serialize_attachment(attachment) for attachment in notification.attachments
            ]
        else:
            serialized_notification["_attachments"] = []

        # Add a field to distinguish between regular and one-off notifications
        serialized_notification["_notification_type"] = (
            "one_off" if isinstance(notification, OneOffNotification) else "regular"
        )

        # Ensure adapter_extra_parameters exists for all notification types
        if "adapter_extra_parameters" not in serialized_notification:
            default_value: dict[str, Any] | None = {} if isinstance(notification, OneOffNotification) else None
            serialized_notification["adapter_extra_parameters"] = getattr(notification, 'adapter_extra_parameters', default_value)

        # Ensure context_used exists
        if "context_used" not in serialized_notification:
            serialized_notification["context_used"] = getattr(notification, 'context_used', None)

        return serialized_notification

    def _serialize_attachment(self, attachment) -> dict:
        """Serialize an attachment (NotificationAttachment or StoredAttachment) for transmission."""
        from vintasend.services.dataclasses import NotificationAttachment, StoredAttachment

        if isinstance(attachment, StoredAttachment):
            # Handle already stored attachments
            return {
                "id": str(attachment.id),
                "filename": attachment.filename,
                "content_type": attachment.content_type,
                "size": attachment.size,
                "checksum": attachment.checksum,
                "created_at": attachment.created_at.isoformat(),
                "description": attachment.description,
                "is_inline": attachment.is_inline,
                "storage_metadata": attachment.storage_metadata,
                # Note: We cannot serialize the file object itself
                # The backend will need to retrieve it when needed
            }
        elif isinstance(attachment, NotificationAttachment):
            # Handle new notification attachments that need to be processed
            # For now, we'll create a simplified representation
            # The actual file processing should happen in the backend

            # Calculate size safely based on file type
            size = 0
            try:
                if hasattr(attachment.file, 'tell') and hasattr(attachment.file, 'seek'):
                    # File-like object
                    current_pos = attachment.file.tell()  # Save current position
                    attachment.file.seek(0, 2)  # Seek to end
                    size = attachment.file.tell()  # Get size
                    attachment.file.seek(current_pos)  # Restore position
                elif isinstance(attachment.file, bytes):
                    size = len(attachment.file)
                elif isinstance(attachment.file, str):
                    # File path
                    import os
                    if os.path.exists(attachment.file):
                        size = os.path.getsize(attachment.file)
                # For other types, leave size as 0
            except Exception:
                # If we can't determine size, that's ok
                size = 0

            return {
                "id": f"temp_{uuid.uuid4()}",  # Temporary ID, guaranteed unique
                "filename": attachment.filename,
                "content_type": attachment.content_type,
                "size": size,
                "checksum": None,  # Will be calculated by backend
                "created_at": datetime.datetime.now().isoformat(),
                "description": attachment.description,
                "is_inline": attachment.is_inline,
                "storage_metadata": {},
                "_is_notification_attachment": True,  # Flag to indicate this needs processing
            }
        else:
            raise ValueError(f"Unsupported attachment type: {type(attachment)}")

    def _deserialize_attachment(self, attachment_dict: dict) -> StoredAttachment:
        """Deserialize an attachment dictionary back to StoredAttachment."""
        # WARNING: PlaceholderAttachmentFile methods raise NotImplementedError. Handle accordingly.
        return StoredAttachment(
            id=attachment_dict["id"],
            filename=attachment_dict["filename"],
            content_type=attachment_dict["content_type"],
            size=attachment_dict["size"],
            checksum=attachment_dict["checksum"],
            created_at=datetime.datetime.fromisoformat(attachment_dict["created_at"]),
            description=attachment_dict["description"],
            is_inline=attachment_dict["is_inline"],
            storage_metadata=attachment_dict["storage_metadata"],
            file=PlaceholderAttachmentFile(attachment_dict["id"])
        )

    def _convert_to_uuid(self, value: str) -> uuid.UUID | str:
        try:
            return uuid.UUID(value)
        except ValueError:
            return value

    def notification_from_dict(self, notification_dict: dict[str, Any]) -> "Notification | OneOffNotification":
        """Convert a dictionary back to a notification (regular or one-off)."""
        send_after = (
            datetime.datetime.fromisoformat(notification_dict["send_after"])
            if notification_dict["send_after"]
            else None
        )

        # Deserialize attachments if present
        attachments = []
        if notification_dict.get("_attachments"):
            attachments = [
                self._deserialize_attachment(attachment_dict)
                for attachment_dict in notification_dict["_attachments"]
            ]

        # Check if this is a one-off notification
        if notification_dict.get("_notification_type") == "one_off" or "email_or_phone" in notification_dict:
            return OneOffNotification(
                id=(
                    self._convert_to_uuid(notification_dict["id"])
                    if isinstance(notification_dict["id"], str)
                    else notification_dict["id"]
                ),
                email_or_phone=notification_dict["email_or_phone"],
                first_name=notification_dict["first_name"],
                last_name=notification_dict["last_name"],
                notification_type=notification_dict["notification_type"],
                title=notification_dict["title"],
                body_template=notification_dict["body_template"],
                context_name=notification_dict["context_name"],
                context_kwargs={
                    key: self._convert_to_uuid(value) if isinstance(value, str) else value
                    for key, value in notification_dict["context_kwargs"].items()
                },
                subject_template=notification_dict["subject_template"],
                preheader_template=notification_dict["preheader_template"],
                status=notification_dict["status"],
                send_after=send_after,
                adapter_extra_parameters=notification_dict.get("adapter_extra_parameters", {}),
                attachments=attachments,
            )
        else:
            # Regular notification
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
                context_used=notification_dict.get("context_used"),
                send_after=send_after,
                attachments=attachments,
            )

    def send(self, notification: "Notification | OneOffNotification", context: "NotificationContextDict") -> None:
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
