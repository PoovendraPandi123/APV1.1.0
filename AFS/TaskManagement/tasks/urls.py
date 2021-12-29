from django.urls import path, include
from . import views
from rest_framework.routers import DefaultRouter
from rest_framework_swagger.views import get_swagger_view

schema_view = get_swagger_view(title='Task Management')

router = DefaultRouter()
# router.register('jobs', views.JobViewSet, basename="jobs_view")
router.register('job_executions', views.JobExecutionViewSet, basename="job_executions")

urlpatterns = [
    path('swagger/', schema_view, name="api_documentation"),
    path('generic/jobs/', views.JobViewGeneric.as_view(), name="jobs_generic")
]

# For View Sets
urlpatterns += [
    path('', include(router.urls))
]