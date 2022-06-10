import logging
import json
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
import uuid
from django.db import connection
import pandas as pd
from rest_framework import viewsets
from rest_framework.generics import ListAPIView
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

class SourceViewGeneric(ListAPIView):
    serializer_class = SourceSerializer

    def get_queryset(self):
        queryset = Sources.objects.all()
        try:
            tenants_id = self.request.query_params.get("tenants_id", "")
            groups_id = self.request.query_params.get("groups_id", "")
            entities_id = self.request.query_params.get("entities_id", "")
            m_processing_layer_id = self.request.query_params.get("m_processing_layer_id", "")
            m_processing_sub_layer_id = self.request.query_params.get("m_processing_sub_layer_id", "")
            processing_layer_id = self.request.query_params.get("processing_layer_id", "")
            is_active = self.request.query_params.get("is_active", "")

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and is_active:
                if is_active == "yes":
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)

                elif is_active == 'no':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 0)

            return queryset.filter(tenants_id=0)

        except Exception:
            logger.error("Error in Source View Generic", exc_info=True)
            return queryset.filter(tenants_id = 0)

class SourceDefinitionsViewGeneric(ListAPIView):
    serializer_class = SourceDefintionSerializer

    def get_queryset(self):
        queryset = SourceDefinitions.objects.all()
        try:
            tenants_id = self.request.query_params.get("tenants_id", "")
            groups_id = self.request.query_params.get("groups_id", "")
            entities_id = self.request.query_params.get("entities_id", "")
            m_processing_layer_id = self.request.query_params.get("m_processing_layer_id", "")
            m_processing_sub_layer_id = self.request.query_params.get("m_processing_sub_layer_id", "")
            processing_layer_id = self.request.query_params.get("processing_layer_id", "")
            sources_id = self.request.query_params.get("sources_id", "")
            is_active = self.request.query_params.get("is_active", "")

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and sources_id and is_active:
                if is_active == 'yes':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1, sources_id = sources_id)

                elif is_active == 'no':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 0, sources_id = sources_id)

            return queryset.filter(sources_id=0)
        except Exception:
            logger.error("Error in Source Definitions View Generic", exc_info=True)
            return queryset.filter(sources_id = 0)

class FileUploadsViewGeneric(ListAPIView):
    serializer_class = FileUploadsSerializer

    def get_queryset(self):
        queryset = FileUploads.objects.all()
        try:
            tenants_id = self.request.query_params.get("tenants_id", "")
            groups_id = self.request.query_params.get("groups_id", "")
            entities_id = self.request.query_params.get("entities_id", "")
            m_processing_layer_id = self.request.query_params.get("m_processing_layer_id", "")
            m_processing_sub_layer_id = self.request.query_params.get("m_processing_sub_layer_id", "")
            processing_layer_id = self.request.query_params.get("processing_layer_id", "")
            is_active = self.request.query_params.get("is_active", "")
            status = self.request.query_params.get("status", "")

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and is_active and status:
                return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id, is_active=1, status=status)

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and is_active:
                if is_active == 'yes':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)

                elif is_active == 'no':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 0)

            return queryset.filter(tenants_id = 0)
        except Exception:
            logger.error("Error in File Uploads View Generic!!!", exc_info=True)
            return queryset.filter(tenants_id = 0)

class TargetFilesViewGeneric(ListAPIView):
    serializer_class = TargetFilesSerializer

    def get_queryset(self):
        queryset = TargetFiles.objects.all()
        try:
            tenants_id = self.request.query_params.get("tenants_id", "")
            groups_id = self.request.query_params.get("groups_id", "")
            entities_id = self.request.query_params.get("entities_id", "")
            m_processing_layer_id = self.request.query_params.get("m_processing_layer_id", "")
            m_processing_sub_layer_id = self.request.query_params.get("m_processing_sub_layer_id", "")
            processing_layer_id = self.request.query_params.get("processing_layer_id", "")
            is_active = self.request.query_params.get("is_active", "")

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and is_active:
                if is_active == 'yes':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)

                elif is_active == 'no':
                    return queryset.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 0)

            return queryset.filter(tenants_id = 0)
        except Exception:
            logger.error("Error in Target Files View Generic!!!", exc_info=True)
            return queryset.filter(tenants_id = 0)

@csrf_exempt
def get_edit_sources(request, *args, **kwargs):
    try:
        pass

        """
            update the source_config with the new 'column start row' and 'key words' keys.
        """
    except Exception:
        logger.error("Error in Target Files View Generic!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_create_source_definitions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

        for key, value in data.items():
            if key == "tenants_id":
                tenants_id = value
            elif key == "groups_id":
                groups_id = value
            elif key == "entities_id":
                entities_id = value
            elif key == "m_processing_layer_id":
                m_processing_layer_id = value
            elif key == "m_processing_sub_layer_id":
                m_processing_sub_layer_id = value
            elif key == "processing_layer_id":
                processing_layer_id = value
            elif key == "user_id":
                user_id = value
            elif key == "sources_id":
                sources_id = value
            elif key == "source_def_list":
                for i in range(0, len(value)):
                    attribute_name_list = value['attribute_name_list']
                    attribute_position_list = value['attribute_position_list']
                    attribute_data_type_list = value['attribute_data_type_list']
                    attribute_date_format_list = value['attribute_date_format_list']
                    attribute_min_length_list = value['attribute_min_length_list']
                    attribute_max_length_list = value['attribute_max_length_list']
                    is_unique_list = value['is_unique_list']


            """
                Check Source id exists in Source Def table if exists make is_active = 0 for all records.
            
            """

            for i in range(0, len(attribute_name_list)):
                SourceDefinitions.objects.create(
                    tenants_id = tenants_id,
                    groups_id = groups_id,
                    entities_id = entities_id,
                    m_processing_layer_id = m_processing_layer_id,
                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id,
                    attribute_name = attribute_name_list[i],

                )

            pass


    except Exception:
        logger.error("Error in Get Create Source Definitions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_create_target_definitions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

    except Exception:
        logger.error("Error in Get Create Target Definitions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})