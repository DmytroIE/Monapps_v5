from django.db import models

from common.constants import VariableTypes, DataAggrTypes


class DataType(models.Model):

    class Meta:
        db_table = "datatypes"

    name = models.CharField(default="Temperature", max_length=200, unique=True)
    agg_type = models.IntegerField(default=DataAggrTypes.AVG, choices=DataAggrTypes.choices)
    var_type = models.IntegerField(default=VariableTypes.CONTINUOUS, choices=VariableTypes.choices)
    # 'is_totalizer' works only with agg_type = SUM
    is_totalizer = models.BooleanField(default=False)  # only for 'agg_type' = SUM

    # For var_type = VariableTypes.NOMINAL or VariableTypes.ORDINAL
    category_map = models.JSONField(default=dict, blank=True)  # For example {"0": "OFF", "1": "ON", ...}

    def __str__(self):
        return f"Datatype {self.name}"


class MeasUnit(models.Model):
    class Meta:
        db_table = "measunits"

    name = models.CharField(max_length=200, unique=True)
    symbol = models.CharField(max_length=15)
    data_type = models.ForeignKey(
        DataType, on_delete=models.PROTECT, related_name="dt_measunits", related_query_name="dt_measunit"
    )
    k = models.FloatField(default=1.0)
    b = models.FloatField(default=0.0)

    def __str__(self):
        return f"MeasUnit {self.name}"

    def to_base_unit(self, value):
        return self.k * value + self.b
