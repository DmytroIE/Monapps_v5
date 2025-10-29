import os
from pathlib import Path

from .additional_settings import *

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("SECRET_KEY", "django-insecure-^rvt^va4@v3_*60+!vi%)t0#5xec0$g2x^@fa-!utsor&108=(")

DEBUG = False

ALLOWED_HOSTS: list = os.environ.get("DJANGO_ALLOWED_HOSTS", "").split(",")

PROJECT_TITLE = os.environ.get("PROJECT_TITLE", "Template")

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_celery_beat",
    "rest_framework",
    "corsheaders",
    "apps.datatypes",
    "apps.applications",
    "apps.assets",
    "apps.datafeeds",
    "apps.datastreams",
    "apps.devices",
    "apps.dfreadings",
    "apps.dsreadings",
    "apps.mqtt_sub",
    "apps.wait_for_db",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

REST_FRAMEWORK = {
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.AllowAny",
    ]
}

ROOT_URLCONF = "monapps.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "monapps.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": os.environ.get("SQL_ENGINE", "django.db.backends.sqlite3"),
        "NAME": os.environ.get("POSTGRES_DB", BASE_DIR / "db.sqlite3"),
        "USER": os.environ.get("POSTGRES_USER"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD"),
        "HOST": os.environ.get("POSTGRES_HOST"),
        "PORT": os.environ.get("POSTGRES_PORT"),
    },
}


# Password validation
# https://docs.djangoproject.com/en/5.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "en-us"

TIME_ZONE = os.environ.get("TZ", "UTC")

USE_I18N = True

USE_TZ = True

STATIC_URL = "static/"


# if (SENTRY_DSN := os.environ.get('SENTRY_DSN')) and ENABLE_SENTRY:
#     # More information on site https://sentry.io/
#     from sentry_sdk import init
#     from sentry_sdk.integrations.celery import CeleryIntegration
#     from sentry_sdk.integrations.django import DjangoIntegration
#     from sentry_sdk.integrations.redis import RedisIntegration
#     from sentry_sdk.integrations.logging import LoggingIntegration

#     init(
#         dsn=SENTRY_DSN,
#         integrations=[
#             DjangoIntegration(),
#             RedisIntegration(),
#             CeleryIntegration(),
#             LoggingIntegration(
#                 level=logging.INFO,
#                 event_level=logging.ERROR,
#             ),
#         ],
#         # Set traces_sample_rate to 1.0 to capture 100%
#         # of transactions for performance monitoring.
#         # We recommend adjusting this value in production.
#         traces_sample_rate=float(os.environ.get('SENTRY_TRACES_SAMPLE_RATE', '1.0')),
#         environment=os.environ.get('SENTRY_ENV', 'development'),
#         sample_rate=float(os.environ.get('SENTRY_SAMPLE_RATE', '1.0')),
#         # If you wish to associate users to errors (assuming you are using
#         # django.contrib.auth) you may enable sending PII data.
#         send_default_pii=True,
#     )


if os.environ.get("DEV_MODE", "0") == "1":
    from .settings_dev import *

else:
    # from .settings_prod import *
    # raise Exception("DEV_MODE is not set to 1")
    pass
