from django.urls import path
from .views import *

# REST API

# Generic
urlpatterns = [
    path('generic/file_uploads/', FileUploadsViewGeneric.as_view(), name="file_upload_generic"),
    path('generic/client_details/', MasterClientsDetailsViewGeneric.as_view(), name="client_details_generic"),
    path('generic/reco_settings/', RecoSettingsViewGeneric.as_view(), name="reco_settings_generic")
]

# Normal API
urlpatterns += [
    path('common/get_store_files/', get_store_files, name="get_store_files")
]