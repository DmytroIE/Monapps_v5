from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import DfrSerializer
from apps.datafeeds.models import Datafeed
from apps.dfreadings.models import DfReading
from monapps.additional_settings.custom_settings import MAX_READINGS_PER_API_CALL
from api.api_utils.get_readings import create_http_response


class ListDfReadings(APIView):

    def get(self, request, **kwargs):
        return create_http_response(DfReading, self.request.query_params, DfrSerializer, **kwargs)
