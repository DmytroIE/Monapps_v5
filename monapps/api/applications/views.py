from apps.applications.models import Application
from .serializers import AppSerializer
from rest_framework import generics


class AppRetrieve(generics.RetrieveAPIView):
    queryset = Application.objects.all()
    serializer_class = AppSerializer


class AppList(generics.ListAPIView):
    queryset = Application.objects.all()
    serializer_class = AppSerializer
