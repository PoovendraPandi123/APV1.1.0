from django.urls import path, include
from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('source', views.SourceViewSet, basename="sources_view")
router.register('source_definition', views.SourceDefinitionViewSet, basename="source_definition_view")

urlpatterns = [

]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]