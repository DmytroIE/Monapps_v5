from django.db import models

from apps.devices.models import Device
from apps.datatypes.models import DataType, MeasUnit
from common.abstract_classes import PublishingOnSaveModel
from common.constants import VariableTypes, HealthGrades
from utils.ts_utils import create_now_ts_ms


class Datastream(PublishingOnSaveModel):
    """
    Represents a "data stream" from a sensor that is part of a digital device.
    Has "health" that represents how regularly data is coming.
    """

    class Meta:
        db_table = "datastreams"
        constraints = [
            models.UniqueConstraint(fields=["name", "parent_id"], name="unique_name_device"),
        ]

    published_fields = {"health", "last_valid_reading_ts", "is_enabled"}

    name = models.CharField(max_length=200)  # TODO: should be unique within one device, create a validator

    data_type = models.ForeignKey(
        DataType, on_delete=models.PROTECT, related_name="dt_datastreams", related_query_name="dt_datastream"
    )
    meas_unit = models.ForeignKey(
        MeasUnit, on_delete=models.PROTECT, related_name="+", related_query_name="+", null=True, blank=True
    )  # null=True, blank=True is for dimensionless datastreams

    is_rbe = models.BooleanField(default=False)  # report by exception, for dss with 'time_update==null' it 99% will be True

    parent = models.ForeignKey(
        Device, on_delete=models.CASCADE, related_name="datastreams", related_query_name="datastream"
    )

    time_update = models.BigIntegerField(default=None, blank=True, null=True)  # null for non-periodic datastreams
    time_change = models.BigIntegerField(
        default=None, blank=True, null=True
    )  # not null for CONT + AVG (and maybe for totalizers)

    # 'till_now_margin' is applicable only for rbe datastreams
    till_now_margin = models.BigIntegerField(default=0)

    # a datastream can be deactivated, if all are deactivated, then the parent device is also deactivated
    is_enabled = models.BooleanField(default=True)

    errors = models.JSONField(default=dict, blank=True)
    warnings = models.JSONField(default=dict, blank=True)

    health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)  # aggregated health
    # msg_health is health derived from errors/warnings
    msg_health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)
    # will remain Undefined for non-periodic ds
    nd_health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)
    time_nd_health_error = models.BigIntegerField(default=300000)
    health_next_eval_ts = models.BigIntegerField(default=0)  # 0 will initiate health eval right after a device creation

    max_rate_of_change = models.FloatField(default=1.0)  # TODO: units per second, can't be <= 0, create a validator
    max_plausible_value = models.FloatField(default=1000000.0)  # TODO: should be > min_plausible_value
    min_plausible_value = models.FloatField(default=-1000000.0)  # TODO: should be < max_plausible_value

    ts_to_start_with = models.BigIntegerField(default=0)  # can be even bigger than 'last_valid_reading_ts'

    # the timestamp of the last valid reading
    last_valid_reading_ts = models.BigIntegerField(default=None, null=True, blank=True)  # only valid reading

    created_ts = models.BigIntegerField(editable=False)

    @property
    def is_value_interger(self) -> bool:
        return self.data_type.var_type != VariableTypes.CONTINUOUS

    def __str__(self):
        return f"Datastream {self.pk} {self.name}"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__is_enabled = self.is_enabled

    def save(self, **kwargs):
        if not self.pk:
            # https://stackoverflow.com/questions/1737017/django-auto-now-and-auto-now-add
            self.created_ts = create_now_ts_ms()
        if self.__is_enabled != self.is_enabled and not self.is_enabled:
            self.health = HealthGrades.UNDEFINED
            self.msg_health = HealthGrades.UNDEFINED
            self.nd_health = HealthGrades.UNDEFINED
            if "update_fields" in kwargs:
                kwargs["update_fields"] = set([*kwargs["update_fields"], "health", "msg_health", "nd_health"])

        super().save(**kwargs)
        self.__is_enabled = self.is_enabled
