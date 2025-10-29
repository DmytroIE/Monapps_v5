from django.urls import path
from .views import DevRetrieve, DevList

urlpatterns = [
    path("<int:pk>/", DevRetrieve.as_view()),
    path("", DevList.as_view()),
]
