from django.urls import path, include
from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('vendor_master', views.VendorMasterViewSet, basename='vendor_master_view')
router.register('file_uploads', views.ReconFileUploadsViewSet, basename="file_uploads_view")

urlpatterns = [
    path('get_file_upload/', views.get_file_upload, name="get_file_upload")
]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]