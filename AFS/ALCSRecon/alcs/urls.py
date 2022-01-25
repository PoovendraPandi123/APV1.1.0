from django.urls import path, include
from .views import *
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('file_uploads/', FileUploadsViewSet, basename='file_uploads_view')

# REST API

# Generic
urlpatterns = [
    path('generic/file_uploads/', FileUploadsViewGeneric.as_view(), name="file_upload_generic"),
    path('generic/client_details/', MasterClientsDetailsViewGeneric.as_view(), name="client_details_generic"),
    path('generic/reco_settings/', RecoSettingsViewGeneric.as_view(), name="reco_settings_generic"),
    path('generic/internal_records/', InternalRecordsViewGeneric.as_view(), name="internal_records_generic")
]

# Normal API
urlpatterns += [
    path('common/get_store_files/', get_store_files, name="get_store_files")
]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]