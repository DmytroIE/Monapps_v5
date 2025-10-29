from django.urls import path
from .views import ListDfReadings

urlpatterns = [
    path("<int:pk>/", ListDfReadings.as_view()),
]
