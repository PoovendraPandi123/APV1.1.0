from django.shortcuts import render
from .models import  *
from django.http import JsonResponse
from django.db import connection
import logging
import pandas as pd
import json
from rest_framework import generics
from rest_framework import mixins
from rest_framework.response import Response
from rest_framework import status
from .serializers import *
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated

# Create your views here.

logger = logging.getLogger("alcs_recon")

def execute_sql_query(query, object_type):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            if object_type == "table":
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers":column_names, "data":rows}
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

class FileUploadsViewGeneric(generics.ListAPIView):
    serializer_class = FileUploadSerializer
    # queryset = FileUploads.objects.all()

    def get_queryset(self):
        queryset = FileUploads.objects.all()
        upload_status = self.request.query_params.get('status', '')

        if upload_status:
            if upload_status.lower() == "batch":
                return queryset.filter(status = 'BATCH')
            else:
                return queryset.filter(status = '')
        return queryset

class MasterClientsDetailsViewGeneric(generics.ListAPIView):
    serializer_class = MasterClientDetailsSerializer

    def get_queryset(self):
        queryset = MasterClientDetails.objects.all()
        client_name = self.request.query_params.get('client_name', '')

        if client_name:
            return queryset.filter(client_name = client_name, is_active = 1)
        return queryset

class RecoSettingsViewGeneric(generics.ListAPIView):
    serializer_class = RecoSettingsSerializer

    def get_queryset(self):
        queryset = RecoSettings.objects.all()
        processing_layer_id = self.request.query_params.get('processing_layer_id', '')
        setting_key = self.request.query_params.get('setting_key', '')

        if processing_layer_id and setting_key:
            return queryset.filter(processing_layer_id =  processing_layer_id, setting_key = setting_key, is_active = 1)

def get_store_files(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if  k == "file_type":
                    file_type = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if  k == "transfer_type":
                    transfer_type = v

            if file_type == 'internal':
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/ALCSRecon/static/internal_file.sql'
            elif file_type == 'external':
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/ALCSRecon/static/external_file.sql'
            elif file_type == 'utr':
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/ALCSRecon/static/utr_file.sql'

            file = open(file_path, 'r+')
            sql_query_file = file.read()
            file.close()

            load_output = execute_sql_query(sql_query_file, object_type="Normal")
            if load_output == "Success":

                reco_settings = RecoSettings.objects.filter(processing_layer_id = processing_layer_id, setting_key = transfer_type)

                for setting in reco_settings:
                    transfer_query = setting.setting_value

                transfer_query_output = execute_sql_query(transfer_query, object_type="Normal")

                if transfer_query_output == "Success":
                    return JsonResponse({"Status": "Success"})
                else:
                    return JsonResponse({"Status": "Error"})
            else:
                return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Store Files!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

