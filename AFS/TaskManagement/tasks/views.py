import logging
from rest_framework import viewsets
from .models import *
from .serializers import *
from rest_framework import generics

# Create your views here.

logger = logging.getLogger("task_management")

class JobViewSet(viewsets.ModelViewSet):
    queryset = Jobs.objects.all()
    serializer_class = JobSerializer

class JobViewGeneric(generics.ListAPIView):
    serializer_class = JobSerializer

    def get_queryset(self):
        queryset = Jobs.objects.all()
        m_processing_layer_id = self.request.query_params.get('m_processing_layer_id', '')
        m_processing_sub_layer_id = self.request.query_params.get('m_processing_sub_layer_id', '')
        processing_layer_id = self.request.query_params.get('processing_layer_id', '')
        if m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id:
            return queryset.filter(m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
        else:
            return queryset.filter(job_id = 0)

class JobExecutionViewSet(viewsets.ModelViewSet):
    queryset = JobExecutions.objects.all()
    serializer_class = JobExecutionSerializer