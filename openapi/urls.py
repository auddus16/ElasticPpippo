from django.urls import path, include
from . import views

urlpatterns = [
    path('index/', views.index),
    path('index/<str:html>', views.gotoHTML),
]