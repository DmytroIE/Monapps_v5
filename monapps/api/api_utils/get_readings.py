from typing import TypedDict
from django.db.models import Model
from django.http.request import QueryDict
from apps.datastreams.models import Datastream
from apps.datafeeds.models import Datafeed
from rest_framework.serializers import Serializer
from rest_framework.response import Response
from apps.dsreadings.models import (
    DsReading,
    UnusedDsReading,
    NonRocDsReading,
    InvalidDsReading,
    NoDataMarker,
    UnusedNoDataMarker,
)
from apps.dfreadings.models import DfReading
from monapps.additional_settings.custom_settings import MAX_READINGS_PER_API_CALL


class ReadingDict(TypedDict):
    id: str
    readingType: str
    totalNumReadings: int
    firstReadingTs: int | None
    lastReadingTs: int | None
    batch: list


reading_to_base_model_map = {
    "dfreading": Datafeed,
    "dsreading": Datastream,
    "invaliddsreading": Datastream,
    "unuseddsreading": Datastream,
    "nonrocdsreading": Datastream,
    "nodatamarker": Datastream,
    "unusednodatamarker": Datastream,
}

shortened_names_map = {
    "dfreading": "dfReadings",
    "dsreading": "dsReadings",
    "invaliddsreading": "invDsReadings",
    "unuseddsreading": "unusDsReadings",
    "nonrocdsreading": "norcDsReadings",
    "nodatamarker": "ndMarkers",
    "unusednodatamarker": "unusNdMarkers",
}


def get_readings(
    reading_model: Model,
    base_model_name: str,
    base_model_instance_pk: int,
    query_params: QueryDict,
    serializer: Serializer,
) -> ReadingDict:

    reading_model_name = reading_model._meta.model_name
    if reading_model_name not in reading_to_base_model_map:
        raise Exception(f"Unknown reading model: {reading_model_name}")

    reading_dict: ReadingDict = {}
    shortened_reading_model_name = shortened_names_map[reading_model_name]
    reading_dict["id"] = f"{base_model_name} {base_model_instance_pk}"
    reading_dict["readingType"] = shortened_reading_model_name

    filter_dict = {base_model_name: base_model_instance_pk}
    qs = reading_model.objects.filter(**filter_dict).order_by("time")

    tot_num_readings = qs.count()
    reading_dict["totalNumReadings"] = tot_num_readings

    if tot_num_readings == 0:
        reading_dict["batch"] = []
        reading_dict["firstReadingTs"] = None
        reading_dict["lastReadingTs"] = None
    else:
        first_reading = qs.first()
        firstDfrTs = first_reading.time if first_reading is not None else None
        reading_dict["firstReadingTs"] = firstDfrTs

        last_reading = qs.last()
        lastDfrTs = last_reading.time if last_reading is not None else None
        reading_dict["lastReadingTs"] = lastDfrTs

        if "gt" in query_params:
            gt = int(query_params.get("gt"))
            qs = qs.filter(time__gt=gt)
        elif "gte" in query_params:
            gte = int(query_params.get("gte"))
            qs = qs.filter(time__gte=gte)

        if "qty" in query_params:
            qty = int(query_params.get("qty"))
            qs = qs[:qty]
        elif "lte" in query_params:
            lte = int(query_params.get("lte"))
            qs = qs.filter(time__lte=lte)

        qs = qs[:MAX_READINGS_PER_API_CALL]  # limit the number of readings in the response

        readings = serializer(qs, many=True)
        reading_dict["batch"] = readings.data

    return reading_dict


def create_http_response(reading_model, query_params: QueryDict, reading_serializer: Serializer, **kwargs) -> Response:

    base_model = reading_to_base_model_map[reading_model._meta.model_name]
    base_model_name = base_model._meta.model_name
    try:
        base_model_instance_pk = int(kwargs.get("pk"))
        base_model.objects.get(pk=base_model_instance_pk)
    except base_model.DoesNotExist:
        return Response(
            {"error": f"{base_model_name.capitalize() if base_model_name is not None else ""} not found"}, status=404
        )
    except ValueError:
        return Response({"error": f"Invalid {base_model_name} pk"}, status=400)
    try:
        reading_dict = get_readings(
            reading_model, base_model_name, base_model_instance_pk, query_params, reading_serializer
        )
        return Response(reading_dict, status=200)
    except ValueError:
        return Response({"error": "Invalid query parameters"}, status=400)
    except Exception as e:
        return Response({"error": str(e)}, status=500)
