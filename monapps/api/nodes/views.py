from rest_framework.views import APIView
from rest_framework.response import Response
from apps.applications.models import Application
from api.applications.serializers import AppSerializer
from apps.assets.models import Asset
from api.assets.serializers import AssetSerializer
from apps.devices.models import Device
from api.devices.serializers import DevSerializer
from apps.datastreams.models import Datastream
from api.datastreams.serializers import DsSerializer
from apps.datafeeds.models import Datafeed
from api.datafeeds.serializers import DfSerializer
from utils.db_field_utils import get_instance_full_id


type_map = {
    "datastream": (Datastream, DsSerializer),
    "datafeed": (Datafeed, DfSerializer),
    "device": (Device, DevSerializer),
    "application": (Application, AppSerializer),
    "asset": (Asset, AssetSerializer),
}


def get_tuple(instance, srlzr):
    instance_data = srlzr(instance).data
    full_id = get_instance_full_id(instance)
    return full_id, instance_data


def get_resp_dict_item(model, srlzr):
    return {k: v for (k, v) in (get_tuple(instance, srlzr) for instance in model.objects.all())}


class ListNodes(APIView):

    def get(self, request, format=None):
        response = {}
        if "type" in request.query_params:
            # https://stackoverflow.com/questions/44419509/django-filter-django-rest-framework-drf-handling-query-params-get-vs-getlist
            for type_name in request.query_params.getlist("type"):
                m_s_tuple = type_map.get(type_name)
                if m_s_tuple is not None:
                    model, srlzr = m_s_tuple
                    response = get_resp_dict_item(model, srlzr)
        else:
            response = {}
            for type_name, m_s_tuple in type_map.items():
                model, srlzr = m_s_tuple
                response.update(**get_resp_dict_item(model, srlzr))

        return Response(response)
