from rest_framework import serializers

from apps.assets.models import Asset
from utils.db_field_utils import get_parent_full_id, get_instance_full_id


class AssetSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()

    def get_id(self, instance):
        return get_instance_full_id(instance)

    def get_parentId(self, instance):
        return get_parent_full_id(instance)

    customFields = serializers.JSONField(source="custom_fields")
    assetType = serializers.CharField(source="asset_type")
    currState = serializers.IntegerField(source="curr_state")
    lastStatusUpdateTs = serializers.IntegerField(source="last_status_update_ts")
    lastCurrStateUpdateTs = serializers.IntegerField(source="last_curr_state_update_ts")
    statusUse = serializers.IntegerField(source="status_use")
    currStateUse = serializers.IntegerField(source="curr_state_use")

    class Meta:
        model = Asset
        fields = [
            "id",
            "name",
            "description",
            "customFields",
            "assetType",
            "status",
            "currState",
            "lastStatusUpdateTs",
            "lastCurrStateUpdateTs",
            "statusUse",
            "currStateUse",
            "health",
            "parentId",
        ]
