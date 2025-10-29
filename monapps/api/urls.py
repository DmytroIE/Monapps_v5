from django.urls import path, include
from api.dsreadings.views import (
    ListDsReadings,
    ListUnusedDsReadings,
    ListInvalidDsReadings,
    ListNonRocDsReadings,
    ListUnusedNoDataMarkers,
    ListNoDataMarkers,
)
from api.dfreadings.views import ListDfReadings


urlpatterns = [
    path("assets/", include("api.assets.urls")),
    path("devices/", include("api.devices.urls")),
    path("applications/", include("api.applications.urls")),
    path("datastreams/", include("api.datastreams.urls")),
    path("datafeeds/", include("api.datafeeds.urls")),
    path("nodes/", include("api.nodes.urls")),
    path("health/", include("api.health_check.urls")),
    path("dfreadings/<int:pk>/", ListDfReadings.as_view()),
    path("dsreadings/<int:pk>/", ListDsReadings.as_view()),
    path("unusdsreadings/<int:pk>/", ListUnusedDsReadings.as_view()),
    path("invdsreadings/<int:pk>/", ListInvalidDsReadings.as_view()),
    path("norcdsreadings/<int:pk>/", ListNonRocDsReadings.as_view()),
    path("unusndmarkers/<int:pk>/", ListUnusedNoDataMarkers.as_view()),
    path("ndmarkers/<int:pk>/", ListNoDataMarkers.as_view()),
]
