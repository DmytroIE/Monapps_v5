from os import environ

from kombu import Exchange, Queue

CELERY_BROKER_URL = environ.get('CELERY_BROKER_URL')
# CELERY_RESULT_BACKEND = environ.get('CELERY_RESULT_BACKEND')

CELERY_TIMEZONE = environ.get('TZ', 'UTC')

# CELERY_RESULT_PERSISTENT = True
# CELERY_TASK_TRACK_STARTED = True
# CELERY_TASK_TIME_LIMIT = 30 * 60
# CELERY_ACCEPT_CONTENT = ['json']
# CELERY_TASK_SERIALIZER = 'json'

# CELERY_BROKER_HEARTBEAT_CHECKRATE = 10
# CELERY_EVENT_QUEUE_EXPIRES = 10
# CELERY_EVENT_QUEUE_TTL = 10
# CELERY_TASK_SOFT_TIME_LIMIT = 60

# CELERY_BROKER_TRANSPORT_OPTIONS = {
#     'max_retries': 4,
#     'interval_start': 0,
#     'interval_step': 0.5,
#     'interval_max': 3,
# }

CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

default_exchange = Exchange("default", type="direct")
evaluate_exchange = Exchange("evaluate", type="direct")
update_exchange = Exchange("update", type="direct")

CELERY_TASK_ROUTES = {
    "evaluate.*": {"queue": "evaluate", "routing_key": "evaluate"},
    "update.*": {"queue": "update", "routing_key": "update"},
}

CELERY_TASK_QUEUES = (
    Queue(name="default", exchange=default_exchange, routing_key="default"),
    Queue(name="evaluate", exchange=evaluate_exchange, routing_key="evaluate"),
    Queue(name="update", exchange=update_exchange, routing_key="update"),
)

CELERY_TASK_DEFAULT_QUEUE = "default"
CELERY_TASK_DEFAULT_EXCHANGE = "default"
CELERY_TASK_DEFAULT_ROUTING_KEY = "default"

CELERY_IMPORTS = ("tasks",)
CELERY_IGNORE_RESULT = True

# https://docs.celeryq.dev/en/stable/userguide/configuration.html#logging
CELERY_WORKER_HIJACK_ROOT_LOGGER = False

CELERY_WORKER_TASK_LOG_FORMAT = "%(task_name)s>> %(message)s"
