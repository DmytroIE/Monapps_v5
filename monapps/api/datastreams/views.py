from apps.datastreams.models import Datastream
from .serializers import DsSerializer
from rest_framework import generics


class DsRetrieve(generics.RetrieveAPIView):
    queryset = Datastream.objects.all()
    serializer_class = DsSerializer


class DsList(generics.ListAPIView):
    queryset = Datastream.objects.all()
    serializer_class = DsSerializer
