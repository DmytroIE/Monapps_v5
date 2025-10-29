from rest_framework import serializers


class DsrSerializer(serializers.Serializer):

    t = serializers.IntegerField(source="time")
    v = serializers.SerializerMethodField()

    def get_v(self, instance):
        if hasattr(instance, "value"):
            val = instance.value
            if isinstance(val, float):
                val = round(val, 3)
            return val
        else:
            return None

    class Meta:
        fields = ["t", "v"]
