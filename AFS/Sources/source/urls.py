from django.urls import path, include
from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('source', views.SourceViewSet, basename="sources_view") # Create Sources, Retrieve Sources, Get Source Definitions
router.register('aggregator', views.AggregatorViewSet, basename="aggregators_view"),
router.register('transformations', views.TransformationsViewSet, basename="transformations_view")
# router.register('transformation_types', views.TransformationTypesViewSet, basename="transformation_types_view"),
# router.register('transformation_operators', views.TransformationOperatorsViewSet, basename="transformation_operators_view")
# router.register('source_definitions', views.SourceDefinitionViewSet, basename="source_definitions_view")

urlpatterns = [
    path('get_database_values/', views.get_database_values, name="get_database_values"),
    path('generic/transformation_types/', views.TransformationTypesGeneric.as_view(), name="transformation_types"),
    path('generic/transformation_operators/', views.TransformationOperatorsGeneric.as_view(), name="transformation_operators"),
    path('generic/aggregator_transformations/', views.AggregatorTransformationsGeneric.as_view(), name="aggregator_transformations")
    # path('generic/source/<str:id>/', views.SourceViewGeneric.as_view(), name="Sources"),
    # path('source_definition/<str:id>/', views.SourceDefinitionViewGenericNo.as_view(), name="Source Definitions"),
]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]