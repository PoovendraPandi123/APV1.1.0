import json
import re

from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import uuid
from rest_framework import viewsets
from rest_framework.generics import ListAPIView
from .serializers import *
from datetime import datetime
from pathlib import Path
from . import keyword_check as kc
from . import read_file as rf
from . packages import validate_file as vf
from django.db import connection
import pandas as pd

# from AFS.Scripts.read_file import get_data_from_file
# import numpy as np
# import shutil
# import mysql.connector

# Create your views here.

logger = logging.getLogger("consolidation_files")

def execute_sql_query(query, object_type):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            if object_type == "table":
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers":column_names, "data":rows}
                # print(table_output)
                output = json.dumps(table_output)
                return output
            elif object_type == "Normal":
                return "Success"
            elif object_type in["update", "create"]:
                return None
            else:
                rows = cursor.fetchall()
                column_header = [col[0] for col in cursor.description]
                df = pd.DataFrame(rows)
                return [df, column_header]
    except Exception as e:
        logger.info(query)
        logger.error(str(e))
        logger.error("Error in Executing SQL Query", exc_info=True)
        return None

def dict_fetch_all(cursor):
    "Return all rows from cursor as a dictionary"
    try:
        column_header = [col[0] for col in cursor.description]
        return [dict(zip(column_header, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error("Error in converting cursor data to dictionary", exc_info=True)

def get_grid_transform(header, header_column):
    try:
        column_defs = []
        for header in header["headers"]:
            column_defs.append({
                "field": header
            })

        column_header_defs = []
        for header in header_column["headers"]:
            column_header_defs.append({
                "headerName": header
            })

        for i in range(0, len(column_defs)):
            column_defs[i]["headerName"] = column_header_defs[i]["headerName"]
            column_defs[i]["sortable"] = "true"

        return column_defs
    except Exception as e:
        logger.error("Error in Getting Grid Transformation!!!", exc_info=True)


class SourceViewSet(viewsets.ModelViewSet):
    queryset = Sources.objects.all()
    serializer_class = SourceSerializer

    def perform_create(self, serializer):
        serializer.save(source_code=str(uuid.uuid4()), created_date=str(datetime.today()),
                        modified_date=str(datetime.today()))


class SourceDefinitionViewSet(viewsets.ModelViewSet):
    queryset = SourceDefinitions.objects.all()
    serializer_class = SourceDefintionSerializer


class TargetFilesViewSet(viewsets.ModelViewSet):
    queryset = TargetFiles.objects.all()
    serializer_class = TargetFilesSerializer

    def perform_create(self, serializer):
        serializer.save(created_date=str(datetime.today()), modified_date=str(datetime.today()))


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
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=1)

                elif is_active == 'no':
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=0)

            return queryset.filter(tenants_id=0)

        except Exception:
            logger.error("Error in Source View Generic", exc_info=True)
            return queryset.filter(tenants_id=0)


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
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=1, sources_id=sources_id)

                elif is_active == 'no':
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=0, sources_id=sources_id)

            return queryset.filter(sources_id=0)
        except Exception:
            logger.error("Error in Source Definitions View Generic", exc_info=True)
            return queryset.filter(sources_id=0)


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
                return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                       m_processing_layer_id=m_processing_layer_id,
                                       m_processing_sub_layer_id=m_processing_sub_layer_id,
                                       processing_layer_id=processing_layer_id, is_active=1, status=status, gst_month__isnull=False)

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and is_active:
                if is_active == 'yes':
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=1).order_by('-id')[:100]

                elif is_active == 'no':
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=0).order_by('-id')[:100]

            return queryset.filter(tenants_id=0)
        except Exception:
            logger.error("Error in File Uploads View Generic!!!", exc_info=True)
            return queryset.filter(tenants_id=0)


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
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=1)

                elif is_active == 'no':
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, is_active=0)

            return queryset.filter(tenants_id=0)
        except Exception:
            logger.error("Error in Target Files View Generic!!!", exc_info=True)
            return queryset.filter(tenants_id=0)

class TargetFilesDefinitionsViewGeneric(ListAPIView):
    serializer_class = TargetFilesDefinitionsSerializer

    def get_queryset(self):
        queryset = TargetFileDefinitions.objects.all()
        try:
            tenants_id = self.request.query_params.get("tenants_id", "")
            groups_id = self.request.query_params.get("groups_id", "")
            entities_id = self.request.query_params.get("entities_id", "")
            m_processing_layer_id = self.request.query_params.get("m_processing_layer_id", "")
            m_processing_sub_layer_id = self.request.query_params.get("m_processing_sub_layer_id", "")
            processing_layer_id = self.request.query_params.get("processing_layer_id", "")
            target_files_id = self.request.query_params.get("target_files_id", "")
            is_active = self.request.query_params.get("is_active", "")

            if tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id and target_files_id and is_active:
                if is_active == "yes":
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, target_files_id = target_files_id, is_active=1)

                elif is_active == "no":
                    return queryset.filter(tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id,
                                           m_processing_layer_id=m_processing_layer_id,
                                           m_processing_sub_layer_id=m_processing_sub_layer_id,
                                           processing_layer_id=processing_layer_id, target_files_id = target_files_id, is_active=0)

            return  queryset.filter(tenants_id = 0)

        except Exception:
            logger.error("Error in Target Files Definitions View Generic!!!", exc_info=True)
            return queryset.filter(tenants_id=0)


@csrf_exempt
def get_edit_sources(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "sources_id":
                    sources_id = value
                if key == "table_start_row":
                    table_start_row = value
                if key == "keywords":
                    keywords = value

            keywords_unique_check = kc.KeywordsUniqueCheck(keyword = keywords.lower())
            keywords_unique_check_output = keywords_unique_check.get_keyword_unique_check_output()

            if keywords_unique_check_output:
                if Sources.objects.filter(id=sources_id).exists():

                    source_v = Sources.objects.filter(id=sources_id)

                    for setting in source_v:
                        setting.key_words["keywords"] = keywords.split(",")
                        setting.source_config["column_start_row"] = int(table_start_row)
                        setting.save()

                return JsonResponse({"Status": "Success"})

            return JsonResponse({"Status": "Not Unique", "Message": "Value is not Unique!!!"})
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Target Files View Generic!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_convert_field_unique(field_unique_string):
    if str(field_unique_string) == "1":
        return True
    return False

@csrf_exempt
def get_create_source_definitions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "tenants_id":
                    tenants_id = value
                if key == "groups_id":
                    groups_id = value
                if key == "entities_id":
                    entities_id = value
                if key == "m_processing_layer_id":
                    m_processing_layer_id = value
                if key == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = value
                if key == "processing_layer_id":
                    processing_layer_id = value
                if key == "user_id":
                    user_id = value
                if key == "sources_id":
                    sources_id = value
                if key == "source_def_list":
                    source_def_list = value

            if SourceDefinitions.objects.filter(sources_id = sources_id, is_active = True).exists():
                source_def_v = SourceDefinitions.objects.filter(sources_id = sources_id, is_active = True)
                # print("source_definition_list", source_def_v)
                for setting in source_def_v:
                    setting.is_active = False
                    # print("inside loop source_list", setting)
                    setting.save()

            # print("source_def_list", source_def_list)

            for source_def in source_def_list:
                SourceDefinitions.objects.create(
                    tenants_id = tenants_id,
                    groups_id = groups_id,
                    entities_id = entities_id,
                    m_processing_layer_id = m_processing_layer_id,
                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id,
                    attribute_name = source_def["fieldName"],
                    attribute_position = source_def["fieldPosition"],
                    attribute_data_type = source_def["fieldDataType"],
                    attribute_date_format = source_def["fieldDateFormat"],
                    attribute_pattern = None,
                    attribute_enums = None,
                    attribute_min_length = source_def["fieldMinimumLength"],
                    attribute_max_length = source_def["fieldMaximumLength"],
                    attribute_formula = None,
                    attribute_reference_field = None,
                    is_validate = True,
                    is_required = True,
                    is_editable = False,
                    is_unique = get_convert_field_unique(source_def["fieldUnique"]),
                    is_active = True,
                    created_by = user_id,
                    created_date = str(datetime.today()),
                    modified_by = user_id,
                    modified_date = str(datetime.today()),
                    sources_id = sources_id
                )

            return JsonResponse({"Status": "Success", "Message": "Source Definitions Created Successfully!!!"})
        return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Create Source Definitions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_storage_reference_config(target_def_list):
    try:
        count_for_text = 0
        count_for_int = 0
        count_for_dec = 0
        count_for_date = 0

        storage_column_name_list = []
        for i in range(0, len(target_def_list)):
            if target_def_list[i]["fieldType"] == 'char':
                count_for_text = count_for_text + 1
                name = 'reference_text_' + str(count_for_text)

            elif target_def_list[i]["fieldType"] == 'integer':
                count_for_int = count_for_int + 1
                name = 'reference_int_' + str(count_for_int)

            elif target_def_list[i]["fieldType"] == 'decimal':
                count_for_dec = count_for_dec + 1
                name = 'reference_dec_' + str(count_for_dec)

            elif target_def_list[i]["fieldType"] == 'date':
                count_for_date = count_for_date + 1
                name = 'reference_date_' + str(count_for_date)

            storage_column_name_list.append(name)

        return storage_column_name_list

    except Exception:
        logger.error("Error in Get Storage Config Function!!!", exc_info=True)
        return ""

@csrf_exempt
def get_create_target_definitions(request, *args, **kwargs):
    try:
        if request.method == "POST":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "tenants_id":
                    tenants_id = value
                if key == "groups_id":
                    groups_id = value
                if key == "entities_id":
                    entities_id = value
                if key == "m_processing_layer_id":
                    m_processing_layer_id = value
                if key == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = value
                if key == "processing_layer_id":
                    processing_layer_id = value
                if key == "target_files_id":
                    target_files_id = value
                if key == "target_def_list":
                    target_def_list = value
                if key == "user_id":
                    user_id = value


            if TargetFileDefinitions.objects.filter(target_files_id = target_files_id, is_active = 1).exists():
                target_def_v = TargetFileDefinitions.objects.filter(target_files_id = target_files_id, is_active = 1)
                for setting in target_def_v:
                    setting.is_active = False
                    setting.save()

            storage_column_fields = get_storage_reference_config(target_def_list)

            module_settings = ModuleSettings.objects.filter(setting_key = 'storage_query', is_active = 1)

            if len(storage_column_fields) > 0:

                for settings in module_settings:
                    storage_query = settings.setting_value["storage_query"]

                storage_query_proper = storage_query.replace("{STORAGE_REF_FIELDS}", str(storage_column_fields)).replace("[", "").replace("]", "")

                for i in range(0, len(target_def_list)):
                    TargetFileDefinitions.objects.create(
                        tenants_id = tenants_id,
                        groups_id = groups_id,
                        entities_id = entities_id,
                        m_processing_layer_id = m_processing_layer_id,
                        m_processing_sub_layer_id = m_processing_sub_layer_id,
                        processing_layer_id = processing_layer_id,
                        field_name = target_def_list[i]["fieldName"],
                        field_sequence = str(target_def_list[i]["fieldPosition"]),
                        field_type = target_def_list[i]["fieldType"],
                        files_config = None,
                        storage_reference_column = storage_column_fields[i],
                        is_active = True,
                        created_by = user_id,
                        created_date = str(datetime.today()),
                        modified_by = user_id,
                        modified_date = str(datetime.today()),
                        target_files_id = target_files_id
                    )

                target_files = TargetFiles.objects.filter(id = target_files_id, is_active = 1)

                for target_file in target_files:
                    target_file.files_config = {
                        "storage_query": storage_query_proper
                    }
                    target_file.save()

                return JsonResponse({"Status": "Success", "Message": "Target Definitions Created Successfully!!!"})
            return JsonResponse({"Status": "Error"})
        return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Create Target Definitions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_create_target_mapping(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "target_id":
                    target_id = value
                if key == "target_def_id":
                    target_def_id = value
                if key == "target_def_list":
                    target_def_list = value

            if TargetFileDefinitions.objects.filter(target_files_id = target_id, id = target_def_id, is_active = 1).exists():
                target_files_definitions = TargetFileDefinitions.objects.filter(id = target_def_id, target_files_id = target_id, is_active = 1)
                # print(target_files_definitions)
                for setting in target_files_definitions:
                    setting.files_config = target_def_list
                    setting.save()

                source_ids_list = []
                for target_def in target_def_list:
                    source_ids_list.append(target_def["sourceId"])

                source_ids_list_unique = list(set(source_ids_list))

                for source_id in source_ids_list_unique:

                    sources = Sources.objects.filter(id = source_id, is_active = True)
                    for source in sources:
                        source_config = source.source_config
                        source_config_target_ids_list = source_config["target_ids"]
                        source_config_target_ids_list.append(target_id)
                        source_config_target_ids_list_unique = list(set(source_config_target_ids_list))
                        source_config["target_ids"] = source_config_target_ids_list_unique
                        source.source_config = source_config
                        source.save()

                return JsonResponse({"Status": "Success", "Message": "Mapping Created Successfully!!!"})
            return JsonResponse({"Status": "Error", "Message": "Target Does Not Exists!!!"})
        return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Create Target Definitions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_validate_error_to_batch(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "validateFileIdsList":
                    file_id_list = value
            FileUploads.objects.filter(id__in=file_id_list, is_active=1, extraction_type='FOLDER').update(status="BATCH", comments="File Queued in Batch!!!")

            return JsonResponse({"Status": "Success", "Message": "File in Batch Status!!!"})

        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_file_gst_month(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "fileId":
                    file_id = value
                if key == "gstRemittanceMonth":
                    gst_month = value
                if key == "fileRequired":
                    file_required = value

            remove_file = True
            if str(file_required) == "1":
                remove_file = False

            if FileUploads.objects.filter(id=file_id, is_active=1).exists():
                files_upload = FileUploads.objects.filter(id=file_id, is_active=1)
                #print(files_upload)
                for setting in files_upload:
                    setting.gst_month = gst_month
                    setting.is_active = remove_file
                    setting.save()
                return JsonResponse({"Status": "Success", "Message": "Gst Month Updated!!!"})
            return JsonResponse({"Status": "Error", "Message": "File Does Not Exists!!!"})
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_file_gst_month_all(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "fileUploadsIdList":
                    file_id_list = value
                if key == "gstRemittanceMonth":
                    gst_month = value

            FileUploads.objects.filter(id__in=file_id_list, is_active=1, status="VALIDATED").update(gst_month=gst_month)

            return JsonResponse({"Status": "Success", "Message": "Gst Month Updated!!!"})
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_proper_file_name(file_name):
    try:
        file_name_extension = "." + file_name.split(".")[-1]
        file_name_without_extension = file_name.replace(file_name_extension, "")
        file_name_date = file_name_without_extension.replace(".", "") + "_" + str(datetime.now()).replace("-", "_").replace(" ", "_").replace(":", "_").replace(".","_") + file_name_extension
        file_name_proper = file_name_date.replace(" ", "_").replace("-", "_").replace("'", "").replace("#", "_No_").replace("&", "_").replace("(", "_").replace(")", "_")
        return file_name_proper
    except Exception:
        logger.error("Error in Getting Proper File Name!!!", exc_info=True)
        return "Error"

def get_create_file_upload_record(**kwargs):
    try:
        FileUploads.objects.create(
            tenants_id = kwargs["tenant_id"],
            groups_id = kwargs["groups_id"],
            entities_id = kwargs["entity_id"],
            m_sources_id = kwargs["m_source_id"],
            source_name = kwargs["source_name"],
            m_processing_layer_id = kwargs["m_processing_layer_id"],
            m_processing_sub_layer_id = kwargs["m_processing_sub_layer_id"],
            processing_layer_id = kwargs["processing_layer_id"],
            source_type = 'FILE',
            extraction_type = 'UPLOAD',
            file_name = kwargs["file_name"],
            file_size_bytes = kwargs["file_size"],
            file_path = kwargs["file_path"],
            status = kwargs["status"],
            comments = kwargs["comment"],
            file_row_count = kwargs["row_count"],
            is_processed = 0,
            is_active = True,
            created_by = kwargs["user_id"],
            created_date = str(datetime.today()),
            modified_by = kwargs["user_id"],
            modified_date = str(datetime.today()),
            gst_month = kwargs["gst_month"]
        )
        return "Success"
    except Exception:
        logger.error("Error in Getting Proper File Name!!!", exc_info=True)
        return "Error"


@csrf_exempt
def get_upload_file_sequential(request, *args, **kwargs):
    try:
        if request.method == 'POST':

            file_name = request.FILES["fileName"].name
            tenant_id = request.POST.get("tenantsId")
            groups_id = request.POST.get("groupsId")
            entity_id = request.POST.get("entityId")
            m_processing_layer_id = request.POST.get("mProcessingLayerId")
            m_processing_sub_layer_id = request.POST.get("mProcessingSubLayerId")
            processing_layer_id = request.POST.get("processingLayerId")
            user_id = request.POST.get("userId")
            gst_month = request.POST.get("gstRemittanceMonth")
            m_source_id = request.POST.get("sourceId")

            sources = Sources.objects.filter(id = m_source_id, is_active = True)

            for source in sources:
                source_name = source.source_name
                file_path = source.source_import_location

            file_name_with_date = file_path + "/" +  get_proper_file_name(file_name)

            with open(file_name_with_date, 'wb+') as destination:
                for chunk in request.FILES["fileName"]:
                    destination.write(chunk)


            source_dict = Sources.objects.filter(id = m_source_id, is_active = 1).values()[0]
            source_def_dict = list(SourceDefinitions.objects.filter(sources_id = m_source_id, is_active = 1).order_by('attribute_position').values())

            validate_file = vf.FileValidation(file = file_name_with_date, source_dict = source_dict, source_def_dict = source_def_dict)

            file_size = Path(file_name_with_date).stat().st_size

            status = "VALIDATION ERROR"
            comments = ''

            keyword_check = validate_file.get_keyword_check()
            # print("keyword_check", keyword_check)

            if not keyword_check:
                comments = "File Name Does not Match with Source!!!"
                file_uploads_create = get_create_file_upload_record(
                    tenant_id = tenant_id,
                    groups_id = groups_id,
                    entity_id = entity_id,
                    m_source_id = m_source_id,
                    source_name = source_name,
                    m_processing_layer_id = m_processing_layer_id,
                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id,
                    file_name = file_name_with_date.split("/")[-1],
                    file_size = file_size,
                    file_path = file_name_with_date,
                    status = status,
                    comment = comments,
                    row_count = 0,
                    user_id = user_id,
                    gst_month = None
                )

                if file_uploads_create == "Success":
                    return JsonResponse({"Status": "Success", "Message": "File Uploaded Successfully!!!"})
                elif file_uploads_create == "Error":
                    return JsonResponse({"Status": "Error"})

            else:
                position_check = validate_file.get_check_column_position()
                # print("position_check", position_check)

                if not position_check:
                    mismatch_data_list = validate_file.get_mismatch_data_list()

                    for mismatch_data in mismatch_data_list:
                        comments = comments + "Position " + str(mismatch_data["position"]) + " is mismatched. Original defined definition is " + "'" + mismatch_data["source_def_attr_name"] + "'" + ". Uploaded Data Column is " + "'" + mismatch_data["data_col_name"] + "'" + "; "

                    comments = comments[:-1]
                    column_count = validate_file.get_check_column_count()

                    if not column_count:
                        unmatched_data_list_source_def = validate_file.get_unmatched_column_list_source_def()
                        unmatched_data_list_data = validate_file.get_unmatched_column_list_data()
                        comments = ''

                        if len(unmatched_data_list_source_def) > 0:
                            comments = "Column not defined in source def and contained in uploaded file - " + str(unmatched_data_list_source_def)
                        else:
                            comments = "Column not defined in Upload file and contained in Source def - " + str(unmatched_data_list_data)

                        file_uploads_create = get_create_file_upload_record(
                            tenant_id=tenant_id,
                            groups_id=groups_id,
                            entity_id=entity_id,
                            m_source_id=m_source_id,
                            source_name=source_name,
                            m_processing_layer_id=m_processing_layer_id,
                            m_processing_sub_layer_id=m_processing_sub_layer_id,
                            processing_layer_id=processing_layer_id,
                            file_name=file_name_with_date.split("/")[-1],
                            file_size=file_size,
                            file_path=file_name_with_date,
                            status=status,
                            comment=comments,
                            row_count=0,
                            user_id=user_id,
                            gst_month=None
                        )

                        if file_uploads_create == "Success":
                            return JsonResponse({"Status": "Success", "Message": "File Uploaded Successfully!!!"})
                        elif file_uploads_create == "Error":
                            return JsonResponse({"Status": "Error"})

                    file_uploads_create = get_create_file_upload_record(
                        tenant_id=tenant_id,
                        groups_id=groups_id,
                        entity_id=entity_id,
                        m_source_id=m_source_id,
                        source_name=source_name,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        file_name=file_name_with_date.split("/")[-1],
                        file_size=file_size,
                        file_path=file_name_with_date,
                        status=status,
                        comment=comments,
                        row_count=0,
                        user_id=user_id,
                        gst_month=None
                    )

                    if file_uploads_create == "Success":
                        return JsonResponse({"Status": "Success", "Message": "File Uploaded Successfully!!!"})
                    elif file_uploads_create == "Error":
                        return JsonResponse({"Status": "Error"})

                else:
                    data_type_check = validate_file.get_data_type_check()
                    # print("data_type_check", data_type_check)

                    if not data_type_check:
                        incorrect_data_type_list_data = validate_file.get_incorrect_data_type_list_data()

                        for incorrect_data in incorrect_data_type_list_data:
                            comments = comments + "Incorrect Data in (row,column) - " + str(incorrect_data["column_position"]) + ". value should be '" + str(incorrect_data["data_type"]) + "'; "

                        comments = comments[:-1]

                        file_uploads_create = get_create_file_upload_record(
                            tenant_id=tenant_id,
                            groups_id=groups_id,
                            entity_id=entity_id,
                            m_source_id=m_source_id,
                            source_name=source_name,
                            m_processing_layer_id=m_processing_layer_id,
                            m_processing_sub_layer_id=m_processing_sub_layer_id,
                            processing_layer_id=processing_layer_id,
                            file_name=file_name_with_date.split("/")[-1],
                            file_size=file_size,
                            file_path=file_name_with_date,
                            status=status,
                            comment=comments,
                            row_count=0,
                            user_id=user_id,
                            gst_month=None
                        )

                        if file_uploads_create == "Success":
                            return JsonResponse({"Status": "Success", "Message": "File Uploaded Successfully!!!"})
                        elif file_uploads_create == "Error":
                            return JsonResponse({"Status": "Error"})

            file_uploads_create = get_create_file_upload_record(
                tenant_id=tenant_id,
                groups_id=groups_id,
                entity_id=entity_id,
                m_source_id=m_source_id,
                source_name=source_name,
                m_processing_layer_id=m_processing_layer_id,
                m_processing_sub_layer_id=m_processing_sub_layer_id,
                processing_layer_id=processing_layer_id,
                file_name=file_name_with_date.split("/")[-1],
                file_size=file_size,
                file_path=file_name_with_date.replace("import", "input"),
                status='VALIDATED',
                comment = "File Validated Successfully!!!",
                row_count=validate_file.get_excel_data_row_count(),
                user_id=user_id,
                gst_month=gst_month
            )

            # Move File to Input Folder Location

            file_name_with_date_input = file_name_with_date.replace("import", "input")
            with open(file_name_with_date_input, 'wb+') as destination:
                for chunk in request.FILES["fileName"]:
                    destination.write(chunk)

            if file_uploads_create == "Success":
                return JsonResponse({"Status": "Success", "Message": "File Uploaded Successfully!!!"})
            elif file_uploads_create == "Error":
                return JsonResponse({"Status": "Error"})

        return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_target_mapping_details(request, *args, **kwargs):
    try:
        if request.method == 'POST':

            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "tenants_id":
                    tenants_id = value
                if key == "groups_id":
                    groups_id = value
                if key == "entities_id":
                    entities_id = value
                if key == "m_processing_layer_id":
                    m_processing_layer_id = value
                if key == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = value
                if key == "processing_layer_id":
                    processing_layer_id = value
                if key == "target_files_id":
                    target_files_id = value

            target_file_definitions = TargetFileDefinitions.objects.filter(tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, target_files_id = target_files_id, is_active = True)

            target_mapping_list = []

            for target_file in target_file_definitions:
                # print("target_file", target_file)
                target_def_name = target_file.field_name

                source_details_list = []
                files_config_list = target_file.files_config

                if files_config_list is not None:
                    for file_config in files_config_list:
                        # print("file_config", file_config)
                        source_id = file_config["sourceId"]
                        source_definition_id = file_config["sourceDefinitionId"]

                        sources = Sources.objects.filter(id = source_id)

                        for source in sources:
                            source_name = source.source_name

                        source_definitions = SourceDefinitions.objects.filter(id = source_definition_id)

                        for source_def in source_definitions:
                            source_definition_name = source_def.attribute_name

                        source_details_list.append({
                            "Source Name": source_name,
                            "Source Def Name": source_definition_name
                        })
                else:
                    source_details_list.append([])

                target_mapping_list.append({
                    "target": target_def_name,
                    "mapping": source_details_list
                })

            return JsonResponse({"Status": "Success", "data": target_mapping_list})

        return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Target Mapping Details Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})


def get_sql_query_string(**kwargs):
    try:

        data = kwargs["data"]

        if len(data) > 0:

            storage_query_proper = kwargs["storage_query_proper"]

            data_rows_list = []
            for index, rows in data.iterrows():
                # create a list for the current row
                data_list = [rows[column] for column in data.columns]
                data_rows_list.append(data_list)

            for row in data_rows_list:
                row.append(kwargs["tenants_id"])
                row.append(kwargs["groups_id"])
                row.append(kwargs["entities_id"])
                row.append(kwargs["m_processing_layer_id"])
                row.append(kwargs["m_processing_sub_layer_id"])
                row.append(kwargs["processing_layer_id"])
                row.append(kwargs["file_id"])
                row.append(kwargs["m_sources_id"])
                row.append(kwargs["m_source_name"])
                row.append(kwargs["target_id"])
                row.append(kwargs["target_name"])
                row.append(kwargs["gst_remittance_month"])
                row.append(kwargs["processing_status"])
                row.append(kwargs["is_active"])
                row.append(kwargs["created_by"])
                row.append(kwargs["created_date"])
                row.append(kwargs["modified_by"])
                row.append(kwargs["modified_date"])


            # Create a insert string from the list
            records = []
            for record_lists in data_rows_list:
                record_string = ''
                for record_list in record_lists:
                    record_string = record_string + "'" + str(record_list) + "', "
                record_proper = "(" + record_string[:-2] + "),"
                records.append(record_proper)

            insert_value_string = ''
            for record in records:
                insert_value_string = insert_value_string + record

            final_query = storage_query_proper.replace("{SOURCE_VALUES}", insert_value_string[:-1])

            return final_query
        else:
            return ""

    except Exception:
        logger.error("Error in Get SQL Query String!!!", exc_info=True)
        return ""

def get_update_file_status(**kwargs):
    try:
        file_uploads = FileUploads.objects.filter(id=kwargs["file_id"], is_active = 1)

        for file in file_uploads:
            file.status = kwargs["status"]
            file.comments = kwargs["comments"]
            file.is_processed = kwargs["is_processed"]
            file.save()

    except Exception:
        logger.error("Error in Get Update File Status Function!!!", exc_info=True)

@csrf_exempt
def get_process_validated_files(request, *args, **kwargs):
    try:
        if request.method == 'POST':
            body = request.body.decode('utf-8')
            data = json.loads(body)

            validated_file_list = []

            for key,value in data.items():
                if key == "validatedFileList":
                    validated_file_list = value

            file_run_count = 0

            for validated_file in validated_file_list:

                file_run_count = file_run_count + 1

                file_path = validated_file["file_path"]
                gst_month = validated_file["gst_month"]
                m_sources_id = validated_file["m_sources"]
                m_source_name = validated_file["source_name"]
                tenants_id = validated_file["tenants_id"]
                groups_id = validated_file["groups_id"]
                entities_id = validated_file["entities_id"]
                m_processing_layer_id = validated_file["m_processing_layer_id"]
                m_processing_sub_layer_id = validated_file["m_processing_sub_layer_id"]
                processing_layer_id = validated_file["processing_layer_id"]
                file_id = validated_file["id"]
                user_id = validated_file["created_by"]

                sources = Sources.objects.filter(id = m_sources_id, is_active = 1)

                for source in sources:
                    target_file_ids_list = source.source_config["target_ids"]
                    column_start_row = source.source_config["column_start_row"]

                read_file_class_out = rf.ReadFile(m_sources_id = m_sources_id, file_path=file_path, column_start_row = column_start_row, m_sources_name = m_source_name)
                # print("read_file_class_out")
                # print(read_file_class_out)
                source_data = read_file_class_out.get_read_file_output()

                for target_file_id in target_file_ids_list:

                    target_file = TargetFiles.objects.filter(id = target_file_id, is_active = 1)

                    for target in target_file:
                        target_name = target.name

                    target_definitions = TargetFileDefinitions.objects.filter(target_files = target_file_id, is_active = 1)

                    if target_definitions:

                        target_definitions_list = []

                        for target_def in target_definitions:

                            target_definitions_list.append({
                                "files_config": target_def.files_config,
                                "target_definition_id": target_def.id,
                                "target_definition_field_name": target_def.field_name,
                                "target_files_id": target_file_id,
                                "storage_reference_column": target_def.storage_reference_column
                            })


                        #target_definitions_list=
                        #[[{"sourceId": "10", "sourceDefinitionId": "61"},{"sourceId": "11", "sourceDefinitionId": "71"},{"sourceId": "12", "sourceDefinitionId": "91"}],
                        # [{"sourceId": "10", "sourceDefinitionId": "61"},{"sourceId": "11", "sourceDefinitionId": "62"}]]

                        required_info_list = []

                        for target_definition in target_definitions_list:
                            # print("target_definition", target_definition)
                            if target_definition["files_config"] is not None:
                                for i in range(0, len(target_definition["files_config"])):
                                    if int(target_definition["files_config"][i]["sourceId"]) == int(m_sources_id):

                                        source_def_id = target_definition["files_config"][i]["sourceDefinitionId"]

                                        source_definitions = SourceDefinitions.objects.filter(id = source_def_id, is_active = 1)

                                        for source_def in source_definitions:
                                            source_def_attribute_name = source_def.attribute_name

                                        required_info_list.append({
                                            "source_definition_id": source_def_id,
                                            "source_definition_field_name": source_def_attribute_name,
                                            "target_files_id": target_definition["target_files_id"],
                                            "target_definition_id": target_definition["target_definition_id"],
                                            "target_definition_field_name": target_definition["target_definition_field_name"],
                                            "storage_reference_column": target_definition["storage_reference_column"]
                                        })
                            else:
                                continue

                        """
                        [
                            {'source_definition_id': '61', 'source_definition_field_name': 'CS', 'target_files_id': '1', 'target_definition_id': 1, 'target_definition_field_name': 'CS', 'storage_reference_column': 'reference_text_1'}, 
                            {'source_definition_id': '62', 'source_definition_field_name': 'CLIENT NAME', 'target_files_id': '1', 'target_definition_id': 2, 'target_definition_field_name': 'CLIENT NAME', 'storage_reference_column': 'reference_text_2'}, 
                            {'source_definition_id': '63', 'source_definition_field_name': 'GM ID/GST CODE', 'target_files_id': '1', 'target_definition_id': 3, 'target_definition_field_name': 'GM ID/GST CODE', 'storage_reference_column': 'reference_int_1'}, 
                            {'source_definition_id': '64', 'source_definition_field_name': 'INVOICE DATE', 'target_files_id': '1', 'target_definition_id': 4, 'target_definition_field_name': 'INVOICE DATE', 'storage_reference_column': 'reference_date_1'}, 
                            {'source_definition_id': '65', 'source_definition_field_name': 'INVOICE#', 'target_files_id': '1', 'target_definition_id': 5, 'target_definition_field_name': 'INVOICE NUMBER', 'storage_reference_column': 'reference_text_3'}, 
                            {'source_definition_id': '66', 'source_definition_field_name': 'CTC', 'target_files_id': '1', 'target_definition_id': 6, 'target_definition_field_name': 'CTC', 'storage_reference_column': 'reference_dec_1'},
                        ]
                        
                        Read the source (fetch the info from excel with source config and source_def)
                        
                        """
                        # print("required_info_list")
                        # print(required_info_list)

                        required_source_def_field_name_list = []
                        required_storage_field_list = []

                        for required_info_list in required_info_list:
                            required_source_def_field_name_list.append(required_info_list["source_definition_field_name"])
                            required_storage_field_list.append(required_info_list["storage_reference_column"])


                        # print("required_source_def_field_name_list", required_source_def_field_name_list)
                        #
                        # print("source_data")
                        # print(source_data)
                        # print("required_source_def_field_name_list", required_source_def_field_name_list)
                        required_data = source_data[required_source_def_field_name_list]
                        # print("required_data")
                        # print(required_data)

                        required_storage_field_list_string = str(required_storage_field_list).replace("[", "").replace("]", "")

                        module_settings = ModuleSettings.objects.filter(setting_key = 'storage_query', is_active = 1)

                        for setting in module_settings:
                            storage_query = setting.setting_value["storage_query"]

                        storage_query_proper = storage_query.replace("{STORAGE_REF_FIELDS}", required_storage_field_list_string.replace("'", ""))

                        # print("storage_query_proper")
                        # print(storage_query_proper)

                        today_date = str(datetime.today())

                        storage_sql_query = get_sql_query_string(
                            data = required_data,
                            storage_query_proper = storage_query_proper,
                            tenants_id = tenants_id,
                            groups_id = groups_id,
                            entities_id = entities_id,
                            m_processing_layer_id = m_processing_layer_id,
                            m_processing_sub_layer_id = m_processing_sub_layer_id,
                            processing_layer_id = processing_layer_id,
                            file_id = file_id,
                            m_sources_id = m_sources_id,
                            m_source_name = m_source_name,
                            target_id = target_file_id,
                            target_name = target_name,
                            gst_remittance_month = gst_month,
                            processing_status = 'New',
                            is_active = 1,
                            created_by = user_id,
                            created_date = today_date,
                            modified_by = user_id,
                            modified_date = today_date
                        )

                        # print("storage_sql_query")
                        # print(storage_sql_query)

                        if storage_sql_query != "":

                            storage_sql_query_output = execute_sql_query(storage_sql_query, object_type="Normal")

                            # print("storage_sql_query_output", storage_sql_query_output)

                            if storage_sql_query_output == "Success":

                                get_update_file_status(file_id = file_id, status = "COMPLETED", comments = "FILE Processed Successfully!!!", is_processed = 1)

                                #return JsonResponse({"Status": "Success", "Message": "File Processed Successfully!!!"})
                            else:
                                get_update_file_status(file_id = file_id, status = "ERROR", comments = "Error in Processing the File!!!", is_processed = 1)
                                #return JsonResponse({"Status": "Error"})

                        else:
                            #return JsonResponse({"Status": "Error"})
                            get_update_file_status(file_id=file_id, status="ERROR", comments="Error in Processing the File!!!", is_processed=1)


            if file_run_count == len(validated_file_list):
                return JsonResponse({"Status": "Success", "Message": "File Processed Successfully!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "File Processing Error!!!"})
        return JsonResponse({"Status": "Error"})


    except Exception:
        logger.error("Error in Get Process Validated Files Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_report_query_string(**kwargs):
    try:

        """
        
            SELECT IFNULL(TRIM(reference_text_1), '') AS reference_text_1, reference_text_2,
            IFNULL(CONVERT(reference_int_1, CHAR), '0') AS reference_int_1, 
            IFNULL(DATE_FORMAT(STR_TO_DATE(reference_date_1, '%Y-%d-%m %H:%i:%s'), '%d-%b-%Y'), '') AS reference_date_1, reference_text_3, 
            IFNULL(CONVERT(reference_dec_1, CHAR), '0.00') AS reference_dec_1, reference_text_4, IFNULL(TRIM(reference_text_5), '') AS reference_text_5
            from consolidation_files.stg_report_storage where target_id = 1 and gst_remittance_month = '2022-06' and is_active = 1;
            
            static_query = "SELECT {fields} FROM consolidation_files.stg_report_storage WHERE target_id = {target_id} AND gst_remittance_month = {gst_month} AND is_active = 1 AND is_invoice = {is_invoice};"
        
        """

        module_settings = ModuleSettings.objects.filter(tenants_id = kwargs["tenants_id"], groups_id = kwargs["groups_id"], entities_id = kwargs["entities_id"],
                                                        m_processing_layer_id = kwargs["m_processing_layer_id"], m_processing_sub_layer_id = kwargs["m_processing_sub_layer_id"],
                                                        processing_layer_id = kwargs["processing_layer_id"], setting_key = 'report_query', is_active = 1)

        for setting in module_settings:
            report_query = setting.setting_value["report_query"]

        storage_reference_column_list = kwargs["storage_reference_column_list"]

        storage_field_changes_list = []

        for storage_reference_column in storage_reference_column_list:
            if re.search("text", storage_reference_column.lower()):
                text_string = "IFNULL(TRIM(" + storage_reference_column + "), '') AS " + storage_reference_column
                storage_field_changes_list.append(text_string)
            elif re.search("int", storage_reference_column.lower()):
                int_string = "IFNULL(CONVERT(" + storage_reference_column +", CHAR), '0') AS " + storage_reference_column
                storage_field_changes_list.append(int_string)
            elif re.search("dec", storage_reference_column.lower()):
                dec_string = "IFNULL(CONVERT(" + storage_reference_column +", CHAR), '0.00') AS " + storage_reference_column
                storage_field_changes_list.append(dec_string)
            elif re.search("date", storage_reference_column.lower()):
                date_string = "IFNULL(DATE_FORMAT(" + storage_reference_column + ", '%d-%m-%Y'), '') AS " + storage_reference_column
                storage_field_changes_list.append(date_string)

        query_field_changed = ''

        for storage_filed in storage_field_changes_list:
            query_field_changed = query_field_changed + storage_filed + ", "

        report_query_proper = report_query.replace("{fields}", query_field_changed[:-2])

        return report_query_proper
    except Exception:
        logger.error("Error in Get Report Query Function!!!", exc_info=True)
        return ""

@csrf_exempt
def get_report(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for key, value in data.items():
                if key == "tenants_id":
                    tenants_id = value
                if key == "groups_id":
                    groups_id = value
                if key == "entities_id":
                    entities_id = value
                if key == "m_processing_layer_id":
                    m_processing_layer_id = value
                if key == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = value
                if key == "processing_layer_id":
                    processing_layer_id = value
                if key == "target_files_id":
                    target_files_id = value
                if key == "report_type":
                    report_type = value
                if key == "gst_month":
                    gst_month = value

            reports = Reports.objects.filter(
                tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id,
                m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, target_id = target_files_id,
                gst_month = gst_month, is_active = 1
            )

            if reports:
                print("Exists")

                return JsonResponse({"Status": "Success"})
            else:

                target_file_definitions = TargetFileDefinitions.objects.filter(
                    tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id, is_active = 1, target_files_id = target_files_id
                ).order_by('field_sequence')

                field_name_list = []
                storage_reference_column_list = []

                for target_file in target_file_definitions:
                    field_name_list.append(target_file.field_name)
                    storage_reference_column_list.append(target_file.storage_reference_column)

                report_query_string = get_report_query_string(
                    tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id, storage_reference_column_list = storage_reference_column_list
                )

                if report_query_string != "":

                    report_query_string_proper = report_query_string.replace("{target_id}", str(target_files_id)).replace("{gst_month}", gst_month)

                    report_query_string_proper_output = json.loads(execute_sql_query(report_query_string_proper, object_type="table"))

                    report_query_string_headers = {"headers": field_name_list}

                    report_query_string_proper_output["headers"] = get_grid_transform(report_query_string_proper_output, report_query_string_headers)

                    return JsonResponse({"Status": "Success", "data": report_query_string_proper_output})
                else:
                    return JsonResponse({"Status": "Error"})

        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Report Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})
