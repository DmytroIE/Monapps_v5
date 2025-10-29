from django.urls import path
from .views import DsRetrieve, DsList

urlpatterns = [
    path("<int:pk>/", DsRetrieve.as_view()),
    path("", DsList.as_view()),
]
