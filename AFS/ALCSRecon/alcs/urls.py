from django.urls import path, include
from .views import *
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('file_uploads', FileUploadsViewSet, basename='file_uploads_view')
router.register('client_details', MasterClientDetailsViewSet, basename="client_details_view")

# REST API

# Generic
urlpatterns = [
    path('generic/file_uploads/', FileUploadsViewGeneric.as_view(), name="file_upload_generic"),
    path('generic/client_details/', MasterClientsDetailsViewGeneric.as_view(), name="client_details_generic"),
    path('generic/reco_settings/', RecoSettingsViewGeneric.as_view(), name="reco_settings_generic"),
    path('generic/internal_records/', InternalRecordsViewGeneric.as_view(), name="internal_records_generic"),
    path('generic/send_mail_client/', SendMailClientViewGeneric.as_view(), name="send_mail_client_generic")
]

# Normal API
urlpatterns += [
    path('common/get_store_files/', get_store_files, name="get_store_files"),
    path('common/get_upload_files/', get_upload_files, name="get_upload_files"),
    path('common/get_daily_letters_report/', get_daily_letters_report, name="get_daily_letters_report"),
    path('common/get_utr_file_update/', get_utr_file_update, name='get_utr_file_update')
]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]