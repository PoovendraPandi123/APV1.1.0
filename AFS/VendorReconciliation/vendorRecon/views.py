import logging
import json
import re
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
import requests
from django.db import connection
import pandas as pd
import os
from pathlib import Path
import shutil
from rest_framework import viewsets
from .serializers import *

# Create your views here.
logger = logging.getLogger("vendor_reconciliation")

def execute_sql_query(query, object_type):
    try:
        with connection.cursor() as cursor:
            #logger.info("Executing SQL Query..")
            # logger.info(query)
            # print(query)
            cursor.execute(query)
            if object_type == "table":
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers":column_names, "data":rows}
                output = json.dumps(table_output)
                return output
            elif object_type in ["data"]:
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers": column_names, "data": rows}
                return table_output
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
        logger.info("Error Executing SQL Query!!", exc_info=True)
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

class VendorMasterViewSet(viewsets.ModelViewSet):
    queryset = VendorMaster.objects.all()
    serializer_class = VendorMasterSerializer

class ReconFileUploadsViewSet(viewsets.ModelViewSet):
    queryset = ReconFileUploads.objects.all()
    serializer_class = ReconFileUploadsSerializer

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

def get_proper_paths(input_path):
    try:
        file_location_to_data = input_path.split("Data/")[0] + "Data/"
        file_location_to_processing_layer_name = file_location_to_data + input_path.split("Data/")[1].split("/")[0]
        file_location_with_input = file_location_to_processing_layer_name + "/" + "input"
        return [file_location_to_processing_layer_name, file_location_with_input]
    except Exception:
        logger.error("Error in Getting Proper Paths!!!", exc_info=True)
        return "Error"

@csrf_exempt
def get_file_upload(request, *args, **kwargs):
    try:
        if request.method == "POST":

            tenant_id = request.POST.get("tenantId")
            group_id = request.POST.get("groupId")
            entity_id = request.POST.get("entityId")
            processing_layer_id = request.POST.get("processingLayerId")
            m_processing_layer_id = request.POST.get("mProcessingLayerId")
            m_processing_sub_layer_id = request.POST.get("mProcessingSubLayerId")
            user_id = request.POST.get("userId")
            file_uploaded = request.POST.get("fileUploaded")

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if len(file_uploaded) > 0:
                                            post_url = "http://localhost:50003/source/get_processing_layer_def_list/"
                                            payload = json.dumps(
                                                {"tenant_id": tenant_id, "group_id": group_id,
                                                 "entity_id": entity_id, "processing_layer_id": processing_layer_id})
                                            headers = {
                                                "Content-Type": "application/json"
                                            }
                                            response = requests.get(post_url, data=payload, headers=headers)
                                            # print(response)
                                            if response.content:
                                                content_data = json.loads(response.content)
                                                if content_data["Status"] == "Success":
                                                    if file_uploaded == "BOTH":

                                                        file_locations = content_data["file_locations"]

                                                        internal_file_name = request.FILES["internalFileName"].name
                                                        external_file_name = request.FILES["externalFileName"].name

                                                        internal_file_name_proper = get_proper_file_name(internal_file_name)
                                                        external_file_name_proper = get_proper_file_name(external_file_name)

                                                        internal_file_location = ''
                                                        external_file_location = ''

                                                        for file_location in file_locations :
                                                            if file_location['side'] == "Internal" :
                                                                int_source_id = file_location['source_id']
                                                                internal_file_location = file_location['input_location']
                                                                int_processing_layer_name = file_location['processing_layer_name']
                                                            elif file_location['side'] == "External" :
                                                                external_file_location = file_location['input_location']
                                                                ext_source_id = file_location['source_id']
                                                                ext_processing_layer_name = file_location['processing_layer_name']

                                                        file_uploads_internal = ReconFileUploads.objects.filter(m_source_id = int_source_id, is_processed = 0)
                                                        internal_file_upload_ids = []
                                                        for internal_file in file_uploads_internal:
                                                            internal_file_upload_ids.append(internal_file.m_source_id)

                                                        file_uploads_external = ReconFileUploads.objects.filter(m_source_id = ext_source_id, is_processed = 0)
                                                        external_file_upload_ids = []
                                                        for external_file in file_uploads_external:
                                                            external_file_upload_ids.append(external_file.m_source_id)

                                                        if len(internal_file_upload_ids) == 0 and len(external_file_upload_ids) == 0:
                                                            if len(internal_file_location) > 0 and len(external_file_location) > 0:

                                                                internal_file_upload_path_name_date = internal_file_location + internal_file_name_proper
                                                                external_file_upload_path_name_date = external_file_location + external_file_name_proper

                                                                internal_file_paths = get_proper_paths(internal_file_location)
                                                                external_file_paths = get_proper_paths(external_file_location)

                                                                if not os.path.exists(internal_file_paths[0]):
                                                                    os.mkdir(internal_file_paths[0])
                                                                if not os.path.exists(internal_file_paths[1]):
                                                                    os.mkdir(internal_file_paths[1])

                                                                if not os.path.exists(external_file_paths[0]):
                                                                    os.mkdir(external_file_paths[0])
                                                                if not os.path.exists(external_file_paths[1]):
                                                                    os.mkdir(external_file_paths[1])

                                                                with open(internal_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["internalFileName"]:
                                                                        destination.write(chunk)
                                                                internal_file_size = Path(internal_file_upload_path_name_date).stat().st_size

                                                                with open(external_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["externalFileName"]:
                                                                        destination.write(chunk)
                                                                external_file_size = Path(external_file_upload_path_name_date).stat().st_size

                                                                # TODO : Add row_count also in the below Table

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = int_processing_layer_name,
                                                                    m_source_id = int_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE' ,
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = internal_file_name_proper,
                                                                    file_size_bytes = internal_file_size,
                                                                    file_path = internal_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = timezone.now(),
                                                                    modified_by = user_id,
                                                                    modified_date = timezone.now()
                                                                )

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = ext_processing_layer_name,
                                                                    m_source_id = ext_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE',
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = external_file_name_proper,
                                                                    file_size_bytes = external_file_size,
                                                                    file_path = external_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = timezone.now(),
                                                                    modified_by = user_id,
                                                                    modified_date = timezone.now()
                                                                )
                                                                return JsonResponse({"Status": "Success","Message": "File Uploaded Sucessfully!!!"})
                                                            else:
                                                                logger.error("Error in Getting the input and external file locations!!!")
                                                                return JsonResponse({"Status": "Error"})
                                                        else:
                                                            return JsonResponse({"Status": "File Exists", "Message": "Already File Exists with Choosen relationship!!!"})

                                                    elif file_uploaded == "INTERNAL":
                                                        file_locations = content_data["file_locations"]
                                                        internal_file_name = request.FILES["internalFileName"].name
                                                        internal_file_name_proper = get_proper_file_name(internal_file_name)

                                                        internal_file_location = ''

                                                        for file_location in file_locations :
                                                            if file_location['side'] == "Internal" :
                                                                int_source_id = file_location['source_id']
                                                                internal_file_location = file_location['input_location']
                                                                int_processing_layer_name = file_location['processing_layer_name']

                                                        file_uploads_internal = ReconFileUploads.objects.filter(m_source_id = int_source_id, is_processed = 0)
                                                        internal_file_upload_ids = []
                                                        for internal_file in file_uploads_internal:
                                                            internal_file_upload_ids.append(internal_file.m_source_id)

                                                        if len(internal_file_upload_ids) == 0:
                                                            if len(internal_file_location) > 0:
                                                                internal_file_upload_path_name_date = internal_file_location + internal_file_name_proper
                                                                internal_file_paths = get_proper_paths(internal_file_location)

                                                                if not os.path.exists(internal_file_paths[0]):
                                                                    os.mkdir(internal_file_paths[0])
                                                                if not os.path.exists(internal_file_paths[1]):
                                                                    os.mkdir(internal_file_paths[1])

                                                                with open(internal_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["internalFileName"]:
                                                                        destination.write(chunk)
                                                                internal_file_size = Path(internal_file_upload_path_name_date).stat().st_size

                                                                # TODO : Add row_count also in the below Table

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = int_processing_layer_name,
                                                                    m_source_id = int_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE' ,
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = internal_file_name_proper,
                                                                    file_size_bytes = internal_file_size,
                                                                    file_path = internal_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = timezone.now(),
                                                                    modified_by = user_id,
                                                                    modified_date = timezone.now()
                                                                )
                                                                return JsonResponse({"Status": "Success", "Message": "File Uploaded Sucessfully!!!"})
                                                            else:
                                                                logger.error("Error in Getting the Internal file locations!!!")
                                                                return JsonResponse({"Status": "Error"})
                                                        else:
                                                            return JsonResponse({"Status": "File Exists", "Message": "Already File Exists with Choosen relationship!!!"})

                                                    elif file_uploaded == "EXTERNAL":
                                                        file_locations = content_data["file_locations"]
                                                        external_file_name = request.FILES["externalFileName"].name

                                                        external_file_name_proper = get_proper_file_name(external_file_name)
                                                        external_file_location = ''

                                                        for file_location in file_locations :
                                                            if file_location['side'] == "External" :
                                                                external_file_location = file_location['input_location']
                                                                ext_source_id = file_location['source_id']
                                                                ext_processing_layer_name = file_location['processing_layer_name']

                                                        file_uploads_external = ReconFileUploads.objects.filter(m_source_id = ext_source_id, is_processed = 0)
                                                        external_file_upload_ids = []
                                                        for external_file in file_uploads_external:
                                                            external_file_upload_ids.append(external_file.m_source_id)

                                                        if len(external_file_upload_ids) == 0:
                                                            if len(external_file_location) > 0:
                                                                external_file_upload_path_name_date = external_file_location + external_file_name_proper
                                                                external_file_paths = get_proper_paths(external_file_location)

                                                                if not os.path.exists(external_file_paths[0]):
                                                                    os.mkdir(external_file_paths[0])
                                                                if not os.path.exists(external_file_paths[1]):
                                                                    os.mkdir(external_file_paths[1])

                                                                with open(external_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["externalFileName"]:
                                                                        destination.write(chunk)
                                                                external_file_size = Path(external_file_upload_path_name_date).stat().st_size

                                                                # TODO : Add row_count also in the below Table

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = ext_processing_layer_name,
                                                                    m_source_id = ext_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE',
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = external_file_name_proper,
                                                                    file_size_bytes = external_file_size,
                                                                    file_path = external_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = timezone.now(),
                                                                    modified_by = user_id,
                                                                    modified_date = timezone.now()
                                                                )
                                                                return JsonResponse({"Status": "Success", "Message": "File Uploaded Sucessfully!!!"})
                                                            else:
                                                                logger.error("Error in Getting the External file locations!!!")
                                                                return JsonResponse({"Status": "Error"})
                                                        else:
                                                            return JsonResponse({"Status": "File Exists", "Message": "Already File Exists with Choosen relationship!!!"})
                                                    else:
                                                        return JsonResponse({"Status": "Error", "Message": "Unknown File Upload Tye Found!!!"})
                                                elif content_data["Status"] == "Error":
                                                    logger.error("Error in Getting Processing Layer Definition List from Recon ETL Service!!!")
                                                    return JsonResponse({"Status": "Error"})
                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "File Uploaded Not Found!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "Unmatched Status Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in File Upload !!!", exc_info=True)
        return JsonResponse({"Status": "Error"})