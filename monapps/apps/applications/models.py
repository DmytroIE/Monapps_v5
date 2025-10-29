from django.db import models
from django_celery_beat.models import PeriodicTask, IntervalSchedule

from apps.assets.models import Asset
from common.abstract_classes import PublishingOnSaveModel
from common.constants import (
    StatusTypes,
    CurrStateTypes,
    StatusUse,
    CurrStateUse,
    HealthGrades,
    AppPurps,
    DEFAULT_TIME_RESAMPLE,
    DEFAULT_TIME_STATUS_STALE,
    DEFAULT_TIME_CURR_STATE_STALE,
    DEFAULT_TIME_APP_HEALTH_ERROR,
)
from utils.ts_utils import create_now_ts_ms


class AppType(models.Model):

    class Meta:
        db_table = "app_types"

    name = models.CharField(max_length=200, unique=True)
    description = models.TextField(max_length=1000, blank=True)
    main_purp = models.CharField(default=AppPurps.NONE, choices=AppPurps.choices)

    func_name = models.CharField(max_length=200)

    def __str__(self):
        return f"AppType '{self.name}'"


class Application(PublishingOnSaveModel):

    class Meta:
        db_table = "applications"

    published_fields = {
        "cursor_ts",
        "status",
        "is_status_stale",
        "last_status_update_ts",
        "curr_state",
        "is_curr_state_stale",
        "last_curr_state_update_ts",
        "health",
        "is_enabled",
        "is_catching_up",
    }

    type = models.ForeignKey(AppType, on_delete=models.PROTECT)
    time_resample = models.BigIntegerField(default=DEFAULT_TIME_RESAMPLE)

    settings = models.JSONField(default=dict, blank=True)  # application settings according to JSON schema
    state = models.JSONField(default=dict, blank=True)  # for retaining the state between calculations

    errors = models.JSONField(default=dict, blank=True)
    warnings = models.JSONField(default=dict, blank=True)

    cursor_ts = models.BigIntegerField()
    is_enabled = models.BooleanField(default=False)

    invoc_interval = models.ForeignKey(
        IntervalSchedule, on_delete=models.PROTECT, related_name="norm_speed_apps", related_query_name="norm_speed_app"
    )
    catch_up_interval = models.ForeignKey(
        IntervalSchedule,
        on_delete=models.PROTECT,
        related_name="catching_up_apps",
        related_query_name="catching_up_app",
    )
    is_catching_up = models.BooleanField(default=False)

    func_version = models.CharField(max_length=200, default="1.0.0")

    status = models.IntegerField(default=None, choices=StatusTypes.choices, blank=True, null=True)
    curr_state = models.IntegerField(default=None, choices=CurrStateTypes.choices, blank=True, null=True)
    last_status_update_ts = models.BigIntegerField(default=None, blank=True, null=True)
    last_curr_state_update_ts = models.BigIntegerField(default=None, blank=True, null=True)
    status_use = models.IntegerField(default=StatusUse.AS_WARNING, choices=StatusUse.choices, blank=False, null=False)
    curr_state_use = models.IntegerField(
        default=CurrStateUse.AS_WARNING, choices=CurrStateUse.choices, blank=False, null=False
    )

    time_status_stale = models.BigIntegerField(default=DEFAULT_TIME_STATUS_STALE)
    time_curr_state_stale = models.BigIntegerField(default=DEFAULT_TIME_CURR_STATE_STALE)
    is_status_stale = models.BooleanField(default=False)
    is_curr_state_stale = models.BooleanField(default=False)

    task = models.OneToOneField(PeriodicTask, on_delete=models.SET_NULL, null=True, blank=True, default=None)

    health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)
    # should be > 4 * time_change + time_resample if there is a datafeed CONT + AVG + restoration enabled
    time_health_error = models.BigIntegerField(default=DEFAULT_TIME_APP_HEALTH_ERROR)

    parent = models.ForeignKey(
        Asset,
        on_delete=models.SET_NULL,
        default=None,
        null=True,
        blank=True,
        related_name="applications",
        related_query_name="application",
    )

    created_ts = models.BigIntegerField(editable=False)

    @property
    def name(self):
        return self.type.name

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__is_enabled = self.is_enabled

    def save(self, **kwargs):
        if not self.pk:
            # https://stackoverflow.com/questions/1737017/django-auto-now-and-auto-now-add
            self.created_ts = create_now_ts_ms()
        if self.__is_enabled != self.is_enabled and not self.is_enabled:
            self.health = HealthGrades.UNDEFINED
            if "update_fields" in kwargs:
                kwargs["update_fields"] = set([*kwargs["update_fields"], "health"])

        super().save(**kwargs)
        self.__is_enabled = self.is_enabled

    def __str__(self):
        return f"Application {self.pk} '{self.type.name}'"

    def get_native_df_qs(self):
        return self.datafeeds.exclude(datastream__isnull=True)

    def get_derived_df_qs(self):
        return self.datafeeds.filter(datastream__isnull=True)
