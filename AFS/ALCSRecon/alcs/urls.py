from django.urls import path
from .views import *

urlpatterns = [
    path('generic/file_uploads/', FileUploadsViewGeneric.as_view(), name="file_upload_generic")
]