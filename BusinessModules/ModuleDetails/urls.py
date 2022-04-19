from django.urls import path
from . import views

urlpatterns = [
    path('login/', views.get_user_login, name="get_login"),
    path('user_credentials/', views.get_user_credentials, name="user_credentials"),
    path('user_rights/', views.get_user_rights, name="user_rights"),
    path('logout/',views.get_user_logout,name="user_logout")
]