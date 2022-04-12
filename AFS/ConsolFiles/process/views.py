import logging
import json
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
import uuid
from django.db import connection
import pandas as pd
from rest_framework import viewsets
from .serializers import *

# Create your views here.

logger = logging.getLogger("consolidation_files")

class SourceViewSet(viewsets.ModelViewSet):
    queryset = Sources.objects.all()
    serializer_class = SourceSerializer

    def perform_create(self, serializer):
        serializer.save(source_code = str(uuid.uuid4()))

class SourceDefinitionViewSet(viewsets.ModelViewSet):
    queryset = SourceDefinitions.objects.all()
    serializer_class = SourceDefintionSerializer