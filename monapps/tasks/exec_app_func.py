import logging
from typing import Optional

from celery import shared_task
from django_celery_beat.models import PeriodicTask

from apps.applications.models import Application
from common.complex_types import AppFunction
from services.app_func_executor import AppFuncExecutor
from app_functions.app_functions import app_function_map
from services.new_dfr_creator import NewDfrCreator

logger = logging.getLogger("#exec_app_func")


@shared_task(bind=True, name="evaluate.app_func")
def exec_app_func(self) -> None:
    if (task := discover_task(self)) is None:
        return
    if (app := discover_app(task)) is None:
        return
    if (app_func := discover_app_func(app)) is None:
        return

    NewDfrCreator(app).execute()
    AppFuncExecutor(app, app_func, task).execute()


def discover_task(ctx) -> Optional[PeriodicTask]:
    task_name = ctx.request.periodic_task_name
    try:
        return PeriodicTask.objects.get(name=task_name)
    except (PeriodicTask.DoesNotExist, PeriodicTask.MultipleObjectsReturned):
        logger.error(f"Cannon get the task instance for the name '{task_name}'")
        return


def discover_app(task: PeriodicTask) -> Optional[Application]:
    app = getattr(task, "application", None)
    if app is None:
        logger.error(f"No application for the task '{task.name}'")
        return
    return app


def discover_app_func(app: Application) -> Optional[AppFunction]:

    app_func_cluster = app_function_map.get(app.type.func_name)
    if app_func_cluster is None:
        logger.error(f"No '{app.type.func_name}' in the app function map")
        return
    app_func_dict = app_func_cluster.get(app.func_version)
    if app_func_dict is None:
        logger.error(f"No version '{app.func_version}' for '{app.type.func_name}'")
        return
    app_func = app_func_dict.get("function")
    if app_func is None:
        logger.error(f"No app function for '{app.type.func_name}' and '{app.func_version}'")
        return
    return app_func
