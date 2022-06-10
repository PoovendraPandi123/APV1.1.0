from django.urls import path, include
from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('source', views.SourceViewSet, basename="sources_view")
router.register('source_definition', views.SourceDefinitionViewSet, basename="source_definition_view")

urlpatterns = [
    path('generic/sources/', views.SourceViewGeneric.as_view(), name="generic_source_view"),
    path('generic/source_definitions/', views.SourceDefinitionsViewGeneric.as_view(), name="generic_source_definitions_view"),
    path('generic/file_uploads/', views.FileUploadsViewGeneric.as_view(), name="file_uploads_view"),
    path('generic/target_files/', views.TargetFilesViewGeneric.as_view(), name="target_files_view"),
    path('get_edit_sources/', views.get_edit_sources, name="get_edit_sources"),
    path('get_create_source_definitions/', views.get_create_source_definitions, name="get_create_source_definitions"),
    path('get_create_target_definitions/', views.get_create_target_definitions, name="get_create_target_definitions")
]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]