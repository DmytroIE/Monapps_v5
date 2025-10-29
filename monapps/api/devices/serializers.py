from rest_framework import serializers

from apps.devices.models import Device
from utils.db_field_utils import get_parent_full_id, get_instance_full_id


class DevSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()

    def get_id(self, instance):
        return get_instance_full_id(instance)

    def get_parentId(self, instance):
        return get_parent_full_id(instance)

    devUi = serializers.CharField(source="dev_ui")

    class Meta:
        model = Device
        fields = [
            "id",
            "name",
            "devUi",
            "description",
            "characteristics",
            "errors",
            "warnings",
            "health",
            "parentId",
        ]
