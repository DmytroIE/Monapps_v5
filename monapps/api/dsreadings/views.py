from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import DsrSerializer
from apps.dsreadings.models import (
    DsReading,
    UnusedDsReading,
    NonRocDsReading,
    InvalidDsReading,
    NoDataMarker,
    UnusedNoDataMarker,
)
from api.api_utils.get_readings import create_http_response


class ListDsReadings(APIView):
    def get(self, request, **kwargs):
        return create_http_response(DsReading, self.request.query_params, DsrSerializer, **kwargs)


class ListInvalidDsReadings(APIView):
    def get(self, request, **kwargs):
        return create_http_response(InvalidDsReading, self.request.query_params, DsrSerializer, **kwargs)


class ListUnusedDsReadings(APIView):
    def get(self, request, **kwargs):
        return create_http_response(UnusedDsReading, self.request.query_params, DsrSerializer, **kwargs)


class ListNonRocDsReadings(APIView):
    def get(self, request, **kwargs):
        return create_http_response(NonRocDsReading, self.request.query_params, DsrSerializer, **kwargs)


class ListNoDataMarkers(APIView):
    def get(self, request, **kwargs):
        return create_http_response(NoDataMarker, self.request.query_params, DsrSerializer, **kwargs)


class ListUnusedNoDataMarkers(APIView):
    def get(self, request, **kwargs):
        return create_http_response(UnusedNoDataMarker, self.request.query_params, DsrSerializer, **kwargs)
