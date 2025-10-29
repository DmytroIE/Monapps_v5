from django.urls import path
from .views import AppRetrieve, AppList

urlpatterns = [
    path("<int:pk>/", AppRetrieve.as_view()),
    path("", AppList.as_view()),
]
