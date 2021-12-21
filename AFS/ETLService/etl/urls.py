from django.urls import path
from . import views

urlpatterns = [
    path('read_file/', views.read_file_view, name="read_file"),
    path('reading_file/', views.reading_file_view, name="reading_file"),
    path('validate_file/', views.validate_file_view, name="validate_file"),
    path('store_file/', views.get_store_files_view, name="get_store_files")
]