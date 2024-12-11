from celery import Celery  # type: ignore
from vintasend.tasks.periodic_tasks import periodic_send_pending_notifications


def periodic_send_pending_notifications_task_factory(celery_app: Celery):
    return celery_app.task(periodic_send_pending_notifications)
