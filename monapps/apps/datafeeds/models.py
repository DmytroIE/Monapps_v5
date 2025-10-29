from django.db import models

from apps.applications.models import Application
from apps.datastreams.models import Datastream
from apps.datatypes.models import DataType, MeasUnit
from common.abstract_classes import PublishingOnSaveModel
from common.constants import AugmentationPolicy, VariableTypes, DfTypes


class Datafeed(PublishingOnSaveModel):
    """
    Represents a filtered and resampled data obtained from a certain datastream.
    Used in the application function.
    """

    class Meta:
        db_table = "datafeeds"
        constraints = [
            models.UniqueConstraint(fields=["name", "parent_id"], name="unique_name_app"),
        ]

    published_fields = {"last_reading_ts"}

    name = models.CharField(max_length=200)
    parent = models.ForeignKey(
        Application, on_delete=models.CASCADE, related_name="datafeeds", related_query_name="datafeed"
    )
    datastream = models.ForeignKey(
        Datastream,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="datafeeds",
        related_query_name="datafeed",
    )

    # 'data_type' should be the same as in the corresponding datastream
    data_type = models.ForeignKey(
        DataType, on_delete=models.PROTECT, related_name="dt_datafeeds", related_query_name="dt_datafeed"
    )
    meas_unit = models.ForeignKey(
        MeasUnit, on_delete=models.PROTECT, related_name="+", related_query_name="+", null=True, blank=True
    )  # null=True, blank=True is for dimensinless datafeeds

    df_type = models.CharField(default=DfTypes.NONE, choices=DfTypes.choices)

    is_rest_on = models.BooleanField(default=True)  # works for CONTINUOUS+AVG
    is_aug_on = models.BooleanField(default=True)  # works for data from RBE datastreams that are not CONTINUOUS+AVG
    aug_policy = models.IntegerField(
        default=AugmentationPolicy.TILL_LAST_DF_READING, choices=AugmentationPolicy.choices
    )

    ts_to_start_with = models.BigIntegerField(default=0)
    last_reading_ts = models.BigIntegerField(default=None, null=True, blank=True)

    @property
    def is_value_interger(self) -> bool:
        return self.data_type.var_type != VariableTypes.CONTINUOUS

    @property
    def time_resample(self) -> int:
        return self.parent.time_resample

    def __str__(self):
        return f"Datafeed {self.pk} {self.name}"
