from rest_framework import serializers
from apps.datastreams.models import Datastream
from utils.db_field_utils import get_parent_full_id, get_instance_full_id


class DsSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()
    dataTypeName = serializers.SerializerMethodField()
    measUnit = serializers.SerializerMethodField()
    aggType = serializers.SerializerMethodField()
    varType = serializers.SerializerMethodField()
    isTotalizer = serializers.SerializerMethodField()

    def get_id(self, instance):
        return get_instance_full_id(instance)

    def get_parentId(self, instance):
        return get_parent_full_id(instance)

    def get_dataTypeName(self, instance):
        return instance.data_type.name

    def get_measUnit(self, instance):
        if instance.meas_unit is None:
            return ""
        return instance.meas_unit.symbol

    def get_aggType(self, instance):
        return instance.data_type.agg_type

    def get_varType(self, instance):
        return instance.data_type.var_type
    
    def get_isTotalizer(self, instance):
        return instance.data_type.is_totalizer

    isEnabled = serializers.BooleanField(source="is_enabled")
    isRbe = serializers.BooleanField(source="is_rbe")
    timeUpdate = serializers.IntegerField(source="time_update")
    timeChange = serializers.IntegerField(source="time_change")
    maxRateOfChange = serializers.FloatField(source="max_rate_of_change")
    maxPlausibleValue = serializers.FloatField(source="max_plausible_value")
    minPlausibleValue = serializers.FloatField(source="min_plausible_value")
    lastValidReadingTs = serializers.IntegerField(source="last_valid_reading_ts")

    class Meta:
        model = Datastream
        fields = [
            "id",
            "name",
            "isEnabled",
            "isRbe",
            "errors",
            "warnings",
            "health",
            "timeUpdate",
            "timeChange",
            "maxRateOfChange",
            "maxPlausibleValue",
            "minPlausibleValue",
            "lastValidReadingTs",
            "dataTypeName",
            "measUnit",
            "aggType",
            "varType",
            "isTotalizer",
            "parentId",
        ]
