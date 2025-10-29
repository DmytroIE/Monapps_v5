from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView

urlpatterns = [
    # redirects from the start page to the admin main page
    path("", RedirectView.as_view(url="admin/")),
    path("admin/", admin.site.urls),
    path("api/", include("api.urls")),
]
