from rest_framework import serializers

from apps.datafeeds.models import Datafeed
from utils.db_field_utils import get_parent_full_id, get_instance_full_id


class DfSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()
    timeResample = serializers.SerializerMethodField()
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

    def get_timeResample(self, instance):
        return instance.parent.time_resample
    
    def get_isTotalizer(self, instance):
        return instance.data_type.is_totalizer

    dfType = serializers.CharField(source="df_type")
    isRestOn = serializers.BooleanField(source="is_rest_on")
    isAugOn = serializers.BooleanField(source="is_aug_on")
    lastReadingTs = serializers.IntegerField(source="last_reading_ts")
    datastreamPk = serializers.IntegerField(source="datastream_id")

    class Meta:
        model = Datafeed
        fields = [
            "id",
            "name",
            "dfType",
            "isRestOn",
            "isAugOn",
            "lastReadingTs",
            "datastreamPk",
            "timeResample",
            "dataTypeName",
            "measUnit",
            "aggType",
            "varType",
            "isTotalizer",
            "parentId",
        ]
