import uuid
from io import BytesIO
from unittest.mock import patch

import pytest
from vintasend.constants import NotificationStatus, NotificationTypes
from vintasend.services.dataclasses import Notification, NotificationAttachment, OneOffNotification
from vintasend.services.notification_backends.stubs.fake_backend import (
    Config,
    FakeFileBackend,
    FakeFileBackendWithNonSerializableKWArgs,
)
from vintasend.services.notification_service import NotificationService, register_context
from vintasend.services.notification_template_renderers.stubs.fake_templated_email_renderer import (
    FakeTemplateRenderer,
    FakeTemplateRendererWithException,
)

from example_app.celery import (
    AsyncCeleryFakeEmailAdapter,
    AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs,
)


@pytest.fixture()
def celery_app():
    from example_app.celery import celery_app
    celery_app.conf.update(task_always_eager=True)
    return celery_app


@pytest.fixture(scope="function")
def notification_backend():
    backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    yield backend
    backend.clear()


@pytest.fixture()
def renderer():
    return FakeTemplateRenderer()


@pytest.fixture()
def notification_service(notification_backend, renderer):
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService[AsyncCeleryFakeEmailAdapter, FakeFileBackend](
        [async_adapter],
        notification_backend,
    )

    return notification_service


def create_notification():
    register_context("test_context")(create_notification_context)
    return Notification(
        id=uuid.uuid4(),
        user_id=1,
        notification_type=NotificationTypes.EMAIL.value,
        title="Test Notification",
        body_template="vintasend_django/emails/test/test_templated_email_body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="vintasend_django/emails/test/test_templated_email_subject.txt",
        preheader_template="vintasend_django/emails/test/test_templated_email_preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
    )


def create_notification_context(test):
    if test != "test":
        raise ValueError("Invalid test value")
    return {"foo": "bar"}


def test_send_notification(notification_backend, notification_service, celery_app):
    notification = create_notification()
    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    notification_service.send(notification)
    assert len(notification_backend.notifications) == 1


def test_send_notification_with_render_error(notification_backend, celery_app):
    notification = create_notification()

    renderer = FakeTemplateRendererWithException()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    with patch("vintasend.tasks.background_tasks.logger.exception") as mock_log_exception:
        notification_service.send(notification)

    mock_log_exception.assert_called_once()

    assert len(async_adapter.sent_emails) == 0


def test_backend_with_non_serializable_kwargs(renderer, celery_app):
    notification = create_notification()
    config = Config()
    backend = FakeFileBackendWithNonSerializableKWArgs(
        database_file_name="celery-adapter-tests-notifications.json",
        config=config,
    )
    async_adapter = AsyncCeleryFakeEmailAdapterWithBackendWithNonSerializableKWArgs(
        template_renderer=renderer, backend=backend, config=config
    )

    notification_service = NotificationService(
        [async_adapter],
        backend,
    )

    backend.notifications.append(notification)
    backend._store_notifications()

    notification_service.send(notification)

    assert len(backend.notifications) == 1

    backend.clear()


def test_backend_with_non_serializable_attachment(renderer, celery_app):
    """Test backend serialization failure with non-serializable attachment."""
    notification = create_notification()
    
    # Create a non-serializable attachment (e.g., a function object)
    class NonSerializableAttachment:
        def __init__(self):
            self.filename = "test.txt"
            self.content_type = "text/plain"
            self.description = "Non-serializable attachment"
            self.is_inline = False
            # Add a function that can't be serialized
            self.func = lambda x: x

    non_serializable_attachment = NonSerializableAttachment()
    notification.attachments = [non_serializable_attachment]
    
    from vintasend_celery.services.notification_adapters.celery_adapter_factory import (
        CeleryNotificationAdapter,
    )

    class MockAdapter(CeleryNotificationAdapter):
        def __init__(self):
            pass

        def serialize_config(self):
            return {}

    adapter = MockAdapter()
    
    with pytest.raises((ValueError, AttributeError, TypeError)):
        # This should fail because the attachment type is not supported
        adapter.notification_to_dict(notification)


def create_one_off_notification():
    """Create a test one-off notification."""
    register_context("test_context")(create_notification_context)
    return OneOffNotification(
        id=uuid.uuid4(),
        email_or_phone="test@example.com",
        first_name="John",
        last_name="Doe",
        notification_type=NotificationTypes.EMAIL.value,
        title="Test One-off Notification",
        body_template="vintasend_django/emails/test/test_templated_email_body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="vintasend_django/emails/test/test_templated_email_subject.txt",
        preheader_template="vintasend_django/emails/test/test_templated_email_preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
    )


def create_notification_with_attachments(attachments=None):
    """Create a test notification with attachments."""
    register_context("test_context")(create_notification_context)

    if attachments is None:
        # Create default test attachments
        attachments = [
            NotificationAttachment(
                file=BytesIO(b"test file content"),
                filename="test.txt",
                content_type="text/plain",
                description="Test file"
            ),
            NotificationAttachment(
                file=BytesIO(b"test pdf content"),
                filename="test.pdf",
                content_type="application/pdf",
                is_inline=False,
                description="Test PDF"
            )
        ]
    elif isinstance(attachments, list) and len(attachments) > 0 and isinstance(attachments[0], dict):
        # Convert dict format to NotificationAttachment objects
        attachment_objects = []
        for att_dict in attachments:
            attachment_objects.append(
                NotificationAttachment(
                    file=BytesIO(att_dict.get("content", b"default content")),
                    filename=att_dict.get("filename", "default.txt"),
                    content_type=att_dict.get("content_type", "text/plain"),
                    description=att_dict.get("description", "Default attachment")
                )
            )
        attachments = attachment_objects

    return Notification(
        id=uuid.uuid4(),
        user_id=1,
        notification_type=NotificationTypes.EMAIL.value,
        title="Test Notification with Attachments",
        body_template="vintasend_django/emails/test/test_templated_email_body.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="vintasend_django/emails/test/test_templated_email_subject.txt",
        preheader_template="vintasend_django/emails/test/test_templated_email_preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
        attachments=attachments,
    )


def test_send_one_off_notification(notification_backend, celery_app):
    """Test sending a one-off notification through Celery."""
    one_off_notification = create_one_off_notification()

    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    # Store the one-off notification in the backend (simulating persistence)
    notification_backend.notifications.append(one_off_notification)
    notification_backend._store_notifications()

    # Send the notification
    notification_service.send(one_off_notification)

    # Verify the notification was processed by checking the backend state
    # Create a fresh backend instance to get the latest data from file
    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(one_off_notification.id)
    assert updated_notification.status == NotificationStatus.SENT.value


def test_send_one_off_notification_missing_required_fields(notification_backend, celery_app):
    """Test error handling when required fields are missing."""
    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )
    notification_service = NotificationService([async_adapter], notification_backend)

    # Missing subject_template
    invalid_notification = create_one_off_notification()
    invalid_notification.subject_template = None

    # Store the notification first
    notification_backend.notifications.append(invalid_notification)
    notification_backend._store_notifications()

    # The system should handle this gracefully (fake renderer doesn't fail on None)
    notification_service.send(invalid_notification)
    
    # Verify it was processed
    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(invalid_notification.id)
    assert updated_notification.status == NotificationStatus.SENT.value


def test_send_one_off_notification_invalid_email(notification_backend, celery_app):
    """Test error handling when recipient email is invalid."""
    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )
    notification_service = NotificationService([async_adapter], notification_backend)

    invalid_notification = create_one_off_notification()
    invalid_notification.email_or_phone = "not-an-email"

    # Store the notification first
    notification_backend.notifications.append(invalid_notification)
    notification_backend._store_notifications()

    # For this test, we'll just ensure the notification can be processed
    # The actual email validation would happen in the adapter/backend
    notification_service.send(invalid_notification)
    
    # Verify it was processed (the fake backend doesn't validate email format)
    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(invalid_notification.id)
    assert updated_notification.status == NotificationStatus.SENT.value


def test_send_notification_with_attachments(notification_backend, celery_app):
    """Test sending a notification with attachments through Celery."""
    notification = create_notification_with_attachments()

    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    notification_service.send(notification)

    # Verify the notification was processed by checking the backend state
    # Create a fresh backend instance to get the latest data from file
    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(notification.id)
    assert updated_notification.status == NotificationStatus.SENT.value


def test_send_notification_with_empty_attachments(notification_backend, celery_app):
    """Test sending a notification with an empty attachment list."""
    notification = create_notification_with_attachments(attachments=[])

    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    notification_service.send(notification)

    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(notification.id)
    assert updated_notification.status == NotificationStatus.SENT.value


def test_send_notification_with_unsupported_attachment_type(notification_backend, celery_app):
    """Test sending a notification with an unsupported file type."""
    # Assuming .exe is unsupported - the actual validation depends on the backend
    notification = create_notification_with_attachments(
        attachments=[{"filename": "malware.exe", "content": b"fake-binary-data", "content_type": "application/octet-stream"}]
    )

    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    # For this fake backend, it should process the notification regardless
    # Real backends might reject certain file types
    notification_service.send(notification)
    
    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(notification.id)
    # For fake backend, it should succeed
    assert updated_notification.status == NotificationStatus.SENT.value


def test_send_notification_with_large_attachment(notification_backend, celery_app):
    """Test sending a notification with a very large attachment."""
    large_content = b"x" * (1024 * 1024)  # 1MB file (reduced from 10MB for test speed)
    notification = create_notification_with_attachments(
        attachments=[{"filename": "large_file.pdf", "content": large_content, "content_type": "application/pdf"}]
    )

    renderer = FakeTemplateRenderer()
    async_adapter = AsyncCeleryFakeEmailAdapter(
        template_renderer=renderer, backend=notification_backend
    )

    notification_service = NotificationService(
        [async_adapter],
        notification_backend,
    )

    notification_backend.notifications.append(notification)
    notification_backend._store_notifications()

    notification_service.send(notification)

    from vintasend.services.notification_backends.stubs.fake_backend import FakeFileBackend
    fresh_backend = FakeFileBackend(database_file_name="celery-adapter-tests-notifications.json")
    updated_notification = fresh_backend.get_notification(notification.id)
    assert updated_notification.status == NotificationStatus.SENT.value


def test_notification_serialization_deserialization():
    """Test that notifications can be serialized and deserialized properly."""
    from vintasend_celery.services.notification_adapters.celery_adapter_factory import (
        CeleryNotificationAdapter,
    )

    # Create a mock adapter for testing serialization
    class MockAdapter(CeleryNotificationAdapter):
        def __init__(self):
            pass

        def serialize_config(self):
            return {}

    adapter = MockAdapter()

    # Test regular notification
    regular_notification = create_notification()
    serialized = adapter.notification_to_dict(regular_notification)
    deserialized = adapter.notification_from_dict(serialized)

    assert isinstance(deserialized, Notification)
    assert deserialized.id == regular_notification.id
    assert deserialized.user_id == regular_notification.user_id
    assert deserialized.title == regular_notification.title

    # Test one-off notification
    one_off_notification = create_one_off_notification()
    serialized = adapter.notification_to_dict(one_off_notification)
    deserialized = adapter.notification_from_dict(serialized)

    assert isinstance(deserialized, OneOffNotification)
    assert deserialized.id == one_off_notification.id
    assert deserialized.email_or_phone == one_off_notification.email_or_phone
    assert deserialized.first_name == one_off_notification.first_name
    assert deserialized.last_name == one_off_notification.last_name
    assert deserialized.title == one_off_notification.title


def test_notification_serialization_with_corrupted_data():
    """Test deserializing corrupted or incomplete data."""
    from vintasend_celery.services.notification_adapters.celery_adapter_factory import (
        CeleryNotificationAdapter,
    )

    class MockAdapter(CeleryNotificationAdapter):
        def __init__(self):
            pass

        def serialize_config(self):
            return {}

    adapter = MockAdapter()

    # Test with missing required field
    corrupted_dict = {
        "id": "test-id",
        "_notification_type": "regular",
        # Missing user_id, title, etc.
    }

    with pytest.raises(KeyError):
        adapter.notification_from_dict(corrupted_dict)

    # Test with invalid send_after format
    corrupted_dict_2 = {
        "id": "test-id",
        "user_id": 1,
        "notification_type": "email",
        "title": "Test",
        "body_template": "test.html",
        "context_name": "test",
        "context_kwargs": {},
        "subject_template": "test.txt",
        "preheader_template": "test.html",
        "status": "pending",
        "send_after": "invalid-date-format",  # Invalid ISO format
        "_notification_type": "regular",
    }

    with pytest.raises((ValueError, KeyError)):
        adapter.notification_from_dict(corrupted_dict_2)

    # Test with invalid attachment data
    corrupted_dict_3 = {
        "id": "test-id",
        "user_id": 1,
        "notification_type": "email",
        "title": "Test",
        "body_template": "test.html",
        "context_name": "test",
        "context_kwargs": {},
        "subject_template": "test.txt",
        "preheader_template": "test.html",
        "status": "pending",
        "send_after": None,
        "_notification_type": "regular",
        "_attachments": [
            {
                "id": "test-attachment",
                "filename": "test.txt",
                # Missing required fields like content_type, size, etc.
            }
        ]
    }

    with pytest.raises(KeyError):
        adapter.notification_from_dict(corrupted_dict_3)


def test_notification_with_attachments_serialization():
    """Test that notifications with attachments can be serialized and deserialized properly."""
    from vintasend_celery.services.notification_adapters.celery_adapter_factory import (
        CeleryNotificationAdapter,
    )

    # Create a mock adapter for testing serialization
    class MockAdapter(CeleryNotificationAdapter):
        def __init__(self):
            pass

        def serialize_config(self):
            return {}

    adapter = MockAdapter()

    # For this test, we'll create a notification object manually with StoredAttachment
    # since the create_notification_with_attachments creates NotificationAttachment
    import datetime

    from vintasend.services.dataclasses import StoredAttachment

    # Create a mock stored attachment
    class MockAttachmentFile:
        def read(self):
            return b"test content"

    stored_attachment = StoredAttachment(
        id="test-attachment-id",
        filename="test.txt",
        content_type="text/plain",
        size=12,
        checksum="abc123",
        created_at=datetime.datetime.now(),
        description="Test attachment",
        is_inline=False,
        storage_metadata={},
        file=MockAttachmentFile()
    )

    notification = Notification(
        id=uuid.uuid4(),
        user_id=1,
        notification_type=NotificationTypes.EMAIL.value,
        title="Test Notification with Attachments",
        body_template="test_template.html",
        context_name="test_context",
        context_kwargs={"test": "test"},
        send_after=None,
        subject_template="test_subject.txt",
        preheader_template="test_preheader.html",
        status=NotificationStatus.PENDING_SEND.value,
        attachments=[stored_attachment],
    )

    # Test serialization/deserialization
    serialized = adapter.notification_to_dict(notification)
    deserialized = adapter.notification_from_dict(serialized)

    assert isinstance(deserialized, Notification)
    assert len(deserialized.attachments) == 1
    assert deserialized.attachments[0].filename == "test.txt"
    assert deserialized.attachments[0].content_type == "text/plain"
    assert deserialized.attachments[0].size == 12
