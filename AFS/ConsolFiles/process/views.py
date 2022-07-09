import json
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import uuid
from rest_framework import viewsets
from rest_framework.generics import ListAPIView
from .serializers import *
from datetime import datetime
from pathlib import Path
from . import keyword_check as kc
from . packages import validate_file as vf

# from AFS.Scripts.read_file import get_data_from_file
# import numpy as np
# import shutil
# import mysql.connector

# Create your views here.

logger = logging.getLogger("consolidation_files")


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
        # print("target def list", target_def_list)
        for i in range(0, len(target_def_list)):
            # print("field Type", target_def_list[i]["fieldType"])
            if target_def_list[i]["fieldType"] == 'char':
                count_for_text = count_for_text + 1
                name = 'reference_text_' + str(count_for_text)
                # print("count_for_text", count_for_text)
                # print("name in char", name)
                # storage_column_name_list.append(name)
            elif target_def_list[i]["fieldType"] == 'integer':
                count_for_int = count_for_int + 1
                name = 'reference_int_' + str(count_for_int)
                # storage_column_name_list.append(name)
            elif target_def_list[i]["fieldType"] == 'decimal':
                count_for_dec = count_for_dec + 1
                name = 'reference_dec_' + str(count_for_dec)
                # storage_column_name_list.append(name)
            elif target_def_list[i]["fieldType"] == 'date':
                count_for_date = count_for_date + 1
                name = 'reference_date_' + str(count_for_date)
                # storage_column_name_list.append(name)

            storage_column_name_list.append(name)
            # print("storage_column_name_list", storage_column_name_list)

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

            if len(storage_column_fields) > 0:
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

            # print("source_dict", source_dict)
            # print("source_def_dict", source_def_dict)

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

                    # print("column_count", column_count)

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
                print("target_file", target_file)
                target_def_name = target_file.field_name

                source_details_list = []
                files_config_list = target_file.files_config

                if files_config_list is not None:
                    for file_config in files_config_list:
                        print("file_config", file_config)
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

# def get_out(request):
#     try:
#
#         target_id=1
#         target_def = TargetFileDefinitions.objects.filter(target_files=target_id)
#         target_col_list = []
#         len_target_field = len(target_def)
#         print(len_target_field)
#         for target in target_def:
#             target_col_list.append(
#                 {
#                     "target_col": target.field_name,
#                     "target_config": target.files_config,
#                     "target_storage_colm": target.storage_colm
#                 }
#             )
#         contain_id = []
#         contain_target_def_col = []
#         contain_source_name = []
#         contain_storage_colm=[]
#         for i in range(0, len_target_field):
#             contain_storage_colm.append(target_col_list[i]['target_storage_colm'])
#             for tar_con in target_col_list[i]['target_config']['source_data']:
#                 contain_id.append(tar_con['source_id'])
#                 contain_target_def_col.append(tar_con['source_def_name'])
#         contain_id = list(set(contain_id))
#         res = []
#         for i in contain_target_def_col:
#             if i not in res:
#                 res.append(i)
#         list_col = list(set(contain_target_def_col))
#         data_final_df_list = []
#         for j in contain_id:
#             for i in range(0, len(set(contain_target_def_col))):
#
#                 for tar_con in target_col_list[i]['target_config']['source_data']:
#                     if tar_con['source_id'] == j:
#                         contain_source_name.append(tar_con['source_def_name'])
#             get_output = get_data_from_file(get_excel_loc(j), 'sheet1', 'xlsx', contain_source_name, 1, '', '',
#                                             '', '', '')
#             get_data = final_data(get_output, contain_source_name, list_col)
#             print("/n#################")
#             print(get_data)
#             data_dict = dict()
#             for z in range(0, len(get_data)):
#                 data_dict[list_col[z]] = get_data[z]
#             proper_data_df = pd.DataFrame(data_dict)
#             data_final_df_list.append(proper_data_df)
#             contain_source_name.clear()
#         print("Final Data")
#         data_final_df = pd.concat(data_final_df_list, axis=0, ignore_index=False)
#         data_final_df_proper = data_final_df.replace(np.nan, "")
#         print(data_final_df_proper)
#         data_final_df_proper=data_final_df_proper.reindex(columns=res)
#         print(data_final_df_proper)
#         print(data_final_df_proper.columns)
#         print("Storage coloum name")
#         storage_save(data_final_df_proper,contain_storage_colm,target_id)
#         return JsonResponse({"Status": "Success"})
#     except Exception as e:
#         print(e)
#         return JsonResponse({"Status": "Error"})
# # #
#
# def get_excel_loc(source_id):
#     source = Sources.objects.get(id=source_id)
#     source_na = source.source_name
#     source_loc = source.source_input_location
#     final_loc = source_loc + '/' + source_na + '.xlsx'
#     return final_loc
#
#
# def final_data(getoutput, contain_source_name, list_col):
#     arr_value = []
#     len1 = len(getoutput['data']['data'])
#     for k in list_col:
#         flag = 0
#         for j in contain_source_name:
#             stu_roll = getoutput['data']['data'][j].to_list()
#             if k == j:
#                 flag = 1
#                 arr_value.append(stu_roll)
#         if flag == 0:
#             arr_value.append(nan_value(len1))
#     return arr_value
#
#
# def nan_value(len):
#     val = []
#     for i in range(0, len):
#         val.append(np.nan)
#     return val
#
#
# def get_all_files_from():
#     try:
#         input_path = "D:\\Advents\\Cosnolidation Files\\Data\\Consolidated Sales Register_Workings Mar-21\\"
#         output_path = "D:\\AdventsProduct\\V1.1.0\\AFS\\ConsolFiles\\Data\\"
#         # fetch all files
#
#         for file_name in os.listdir(input_path):
#             # construct full file path
#             print("files:", file_name)
#             source = input_path + file_name
#             # create_file_upload(source)
#             destination = output_path + file_name
#             if not os.path.exists(destination):
#                 os.mkdir(destination)
#             source_path_proper_input = destination + "\\" + "input\\"
#             if not os.path.exists(source_path_proper_input):
#                 os.mkdir(source_path_proper_input)
#             # move files
#             print("moving file")
#             shutil.move(source, source_path_proper_input)
#             print('Moved:', file_name)
#         return {"Status": "Success"}
#     except Exception as e:
#         print(e)
#         return {"Status": "Error"}
#
#
# def storage_save(data_final_df_proper,contain_storage_colm,target_id):
#     global cursor1
#     try:
#         print(data_final_df_proper)
#         print("Save data in storage table")
#         conn = mysql.connector.connect(host ="localhost", user = "root", password = "rootuser12", db = "consolidation_files")
#         #if conn.connection is None:
#         cursor1 = conn.cursor()
#         print("cursor created")
#         len_data=len(data_final_df_proper)
#         listToStr = '`,`'.join([str(elem) for elem in contain_storage_colm])
#         print(listToStr)
#         sql = "INSERT INTO `report_storage` (`" + listToStr + "`) VALUES (" + "%s," * (len_data - 1) + "%s)"
#         print(sql)
#         print("execute")
#         #cursor1.execute(sql, val)
#         #conn.commit()
#         print("Data inserted")
#         query_update(sql, target_id)
#     except Exception as e:
#         print(e)
#         return {"Status": "Error"}
#
#

#
#
# def get_create_sql_file(data, insert_query, file_type):
#     try:
#         if len(data) > 0:
#             data_rows_list = []
#             for index, rows in data.iterrows():
#                 # create a list for the current row
#                 data_list = [rows[column] for column in data.columns]
#                 data_rows_list.append(data_list)
#
#             # Create a insert string from the list
#             records = []
#             for record_lists in data_rows_list:
#                 record_string = ''
#                 for record_list in record_lists:
#                     record_string = record_string + "'" + str(record_list) + "', "
#                 record_proper = "(" + record_string[:-2] + "),"
#                 records.append(record_proper)
#
#             insert_value_string = ''
#             for record in records:
#                 insert_value_string = insert_value_string + record
#             final_query = insert_query.replace("{data_values}", insert_value_string[:-1])
#             #execute_sql_query(final_query, object_type="Normal")
#
#             return 'Success'
#         else:
#             print("Length of Loading Data Frame is equal to Zero!!!")
#             return "Error"
#     except Exception as e:
#         print(e)
#         logging.error("Error in Get Load Dataframe Function!!!", exc_info=True)
#
#
#
# def query_update(query,target_id):
#     try:
#         print("inside query")
#         TargetFiles_1 = TargetFiles.objects.filter(id=target_id)
#         for setting in TargetFiles_1:
#             setting.files_config = query
#             setting.save()
#         print("target update")
#     except Exception as e:
#         print(e)
#         return {"Status": "Error"}

