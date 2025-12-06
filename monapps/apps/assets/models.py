from django.db import models

from common.abstract_classes import PublishingOnSaveModel
from common.constants import StatusTypes, CurrStateTypes, HealthGrades, StatusUse, CurrStateUse, AssetTypes
from common.constants import reeval_fields


def get_reeval_fields():
    return list(reeval_fields)


class Asset(PublishingOnSaveModel):
    """
    Represents a monitored asset (a heat exchanger, a pipe, a whole workshop, etc).
    An asset can contain other assets and also applications/devices as leaves of the tree.
    Each asset has "health"/"current state"/"status" defined by its children.
    """

    class Meta:
        db_table = "assets"

    published_fields = {
        "status",
        "last_status_update_ts",
        "curr_state",
        "last_curr_state_update_ts",
        "health",
    }
    name = models.CharField(max_length=200, unique=True)
    description = models.TextField(max_length=1000, blank=True)
    custom_fields = models.JSONField(default=dict, blank=True)  # like {"power": 25, "weight": 43.2, ...}
    asset_type = models.CharField(default=AssetTypes.GENERIC, choices=AssetTypes.choices)

    status = models.IntegerField(default=None, choices=StatusTypes.choices, blank=True, null=True)
    curr_state = models.IntegerField(default=None, choices=CurrStateTypes.choices, blank=True, null=True)
    last_status_update_ts = models.BigIntegerField(default=None, blank=True, null=True)
    last_curr_state_update_ts = models.BigIntegerField(default=None, blank=True, null=True)
    status_use = models.IntegerField(default=StatusUse.AS_WARNING, choices=StatusUse.choices, blank=False, null=False)
    curr_state_use = models.IntegerField(
        default=CurrStateUse.AS_WARNING, choices=CurrStateUse.choices, blank=False, null=False
    )

    health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)

    next_upd_ts = models.BigIntegerField(default=0)  # 0 will initiate update right after asset creation
    # list of fields to be reevaluated, populated by the children
    reeval_fields = models.JSONField(default=get_reeval_fields, blank=True)

    parent = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        default=None,
        null=True,
        blank=True,
        related_name="assets",
        related_query_name="asset",
    )

    def __str__(self):
        return f"Asset {self.pk} {self.name}"
