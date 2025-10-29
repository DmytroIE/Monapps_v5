from django.urls import path
from .views import ListNodes

urlpatterns = [
    path('', ListNodes.as_view()),
]
