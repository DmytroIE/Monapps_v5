from django.db import models

from apps.datafeeds.models import Datafeed

from common.constants import NotToUseDfrTypes
from utils.ts_utils import create_dt_from_ts_ms


class DfReading(models.Model):
    class Meta:
        db_table = "df_readings"

    pk = models.CompositePrimaryKey("datafeed_id", "time")
    time = models.BigIntegerField()
    datafeed = models.ForeignKey(Datafeed, on_delete=models.PROTECT)
    db_value = models.FloatField()
    restored = models.BooleanField(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.not_to_use: None | NotToUseDfrTypes = None

    @property
    def value(self) -> float | int:
        if self.datafeed.is_value_interger:
            return int(self.db_value)
        else:
            return self.db_value

    @value.setter
    def value(self, value: float) -> None:
        if self.datafeed.is_value_interger:
            self.db_value = round(value, 0)
        else:
            self.db_value = value

    def __str__(self):
        dt_str = create_dt_from_ts_ms(self.time).strftime("%Y/%m/%d %H:%M:%S")
        if self.datafeed.is_value_interger:
            return f"DFR df:{self.datafeed.pk} ts:{dt_str} val: {self.value} {'R' if self.restored else ''}"
        else:
            return f"DFR df:{self.datafeed.pk} ts:{dt_str} val: {self.value:.3f} {'R' if self.restored else ''}"
