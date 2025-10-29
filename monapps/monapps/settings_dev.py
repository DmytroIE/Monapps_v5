DEBUG = True
ALLOWED_HOSTS = ["*"]
CORS_ORIGIN_ALLOW_ALL = True

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "filters": ["OnlyLocalModulesFilter"],
        },
    },
    "filters": {
        "OnlyLocalModulesFilter": {
            "()": "utils.log_filters.OnlyLocalModulesFilter",
        },
    },
    "formatters": {
        "simple": {"format": "|%(levelname)s|\t|%(asctime)s|\t|%(module)s|\t'%(message)s'"},
    },
    "root": {
        "handlers": ["console"],
        "level": "DEBUG",
    },
}

print("Using DEV settings")
