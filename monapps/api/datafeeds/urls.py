from django.urls import path
from .views import DfRetrieve, DfList

urlpatterns = [
    path("<int:pk>/", DfRetrieve.as_view()),
    path("", DfList.as_view()),
]
