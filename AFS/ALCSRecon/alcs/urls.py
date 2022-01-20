from django.urls import path
from .views import *

urlpatterns = [
    path('generic/file_uploads/', FileUploadsViewGeneric.as_view(), name="file_upload_generic"),
    path('generic/client_details/', MasterClientsDetailsViewGeneric.as_view(), name="client_details_generic")
]