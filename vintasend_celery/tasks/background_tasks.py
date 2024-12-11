import logging

from celery import Celery  # type: ignore
from vintasend.tasks.background_tasks import send_notification


logger = logging.getLogger(__name__)


def send_notification_task_factory(celery_app: Celery):
    return celery_app.task(send_notification)
