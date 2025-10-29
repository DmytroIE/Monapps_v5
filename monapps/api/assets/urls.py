from django.urls import path
from .views import AssetRetrieve, AssetList

urlpatterns = [
    path("<int:pk>/", AssetRetrieve.as_view()),
    path("", AssetList.as_view()),
]
