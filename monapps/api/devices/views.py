from apps.devices.models import Device
from .serializers import DevSerializer
from rest_framework import generics


class DevRetrieve(generics.RetrieveAPIView):
    queryset = Device.objects.all()
    serializer_class = DevSerializer


class DevList(generics.ListAPIView):
    queryset = Device.objects.all()
    serializer_class = DevSerializer
