from django.shortcuts import render
from django.utils import timezone
from pathlib import Path
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
from django.db import connection
import logging
import pandas as pd
import json
from datetime import datetime
import requests
from rest_framework import generics
from rest_framework import mixins
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Sum
from .serializers import *
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework import viewsets
from .script import send_mail_client as sm
from .script import utr_file_functions as utr


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

class SendRequest:

    def __init__(self):
        pass

    def get_response(self, post_url, headers, data):
        try:
            response = requests.get(post_url, headers=headers, data=data)
            if response.content:
                content_data = json.loads(response.content)
                # print("content_data")
                # print(content_data)
                return {"Status": "Success"}
            else:
                logging.error("Error in Getting Response in Send Request Class!!!")
                return {"Status": "Error"}
        except Exception as e:
            logging.error("Error in Get Batch Files!!!", exc_info=True)
            logging.error(str(e))
            return {"Status": "Error"}

class FileUploadsViewGeneric(generics.ListAPIView):
    serializer_class = FileUploadSerializer
    # queryset = FileUploads.objects.all()

    def get_queryset(self):
        queryset = FileUploads.objects.all()
        upload_status = self.request.query_params.get('status', '')
        file_uploaded = self.request.query_params.get('file_uploaded', '')

        if upload_status:
            if upload_status.lower() == "batch":
                return queryset.filter(status = 'BATCH', is_processed = 0, is_active = 1)
            if upload_status.lower() == "batch_all":
                return queryset.filter(status = 'BATCH_ALL', is_processed = 0, is_active = 1)

        if file_uploaded:
            queryset_reversed = queryset[::-1]
            return queryset_reversed[0:int(file_uploaded)]
        return queryset.filter(status = '')


class FileUploadsViewSet(viewsets.ModelViewSet):
    queryset = FileUploads.objects.all()
    serializer_class = FileUploadSerializer

class InternalRecordsViewSet(viewsets.ModelViewSet):
    queryset = InternalRecords.objects.all()
    serializer_class = InternalRecordsSerializer

class InternalRecordsViewGeneric(generics.ListAPIView):
    serializer_class = InternalRecordsSerializer

    def get_queryset(self):
        queryset = InternalRecords.objects.all()
        payment_date = self.request.query_params.get('payment_date', '')
        client_id = self.request.query_params.get('client_id', '')
        tenants_id = self.request.query_params.get('tenants_id', '')
        groups_id = self.request.query_params.get('groups_id', '')
        entities_id = self.request.query_params.get('entity_id', '')
        m_processing_layer_id = self.request.query_params.get('m_processing_layer_id', '')
        m_processing_sub_layer_id = self.request.query_params.get('m_processing_sub_layer_id', '')
        processing_layer_id = self.request.query_params.get('processing_layer_id', '')
        report_type = self.request.query_params.get('report_type', '')
        if payment_date and client_id:
            return queryset.filter(int_extracted_text_50 = payment_date, int_reference_text_8 = client_id, is_active = 1, int_processing_status_1__isnull=True)
        elif payment_date and tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id:
            return queryset.filter(int_extracted_text_50 = payment_date, tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id,
                                   m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id,
                                   is_active = 1, int_processing_status_1__isnull=True)

# class SendMailClientViewGeneric(generics.ListAPIView):
#     serializer_class = InternalRecordsSerializer
#
#     def get_queryset(self):
#         queryset = InternalRecords.objects.all()
#         payment_date = self.request.query_params.get('payment_date', '')
#         client_id = self.request.query_params.get('client_id', '')
#         if payment_date and client_id:
#             data_list = list(queryset.filter(int_extracted_text_50 = payment_date, int_reference_text_8 = client_id, is_active = 1).values(
#                 'int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_4', 'int_amount_1', 'int_reference_text_5', 'int_reference_text_6',
#                 'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'
#             ))
#             if len(data_list) > 0:
#                 m_client_details = MasterClientDetails.objects.filter(client_id = client_id)
#
#                 for client in m_client_details:
#                     email_address = client.email_address
#
#                 send_mail_output = sm.send_mail_client(data_list = data_list, email_address = email_address, payment_date = payment_date, client_id = client_id)
#                 if send_mail_output == True:
#                     for client in m_client_details:
#                         client.last_send_on = timezone.now()
#                         client.save()
#                     return queryset.filter(int_extracted_text_9 = payment_date, int_reference_text_8 = client_id, is_active = 1)
#             else:
#                 return queryset.filter(id = 0)

class SendMailClientViewGeneric(generics.ListAPIView):
    serializer_class = InternalRecordsSerializer

    def get_queryset(self):
        queryset = InternalRecords.objects.all()
        payment_from_date = self.request.query_params.get('paymentFromDate', '')
        payment_to_date = self.request.query_params.get('paymentToDate', '')
        client_id = self.request.query_params.get('clientId', '')
        tenants_id = self.request.query_params.get('tenantsId', '')
        groups_id = self.request.query_params.get('groupsId', '')
        entities_id = self.request.query_params.get('entitiesId', '')
        m_processing_layer_id = self.request.query_params.get('mProcessingLayerId', '')
        m_processing_sub_layer_id = self.request.query_params.get('mProcessingSubLayerId', '')

        # print("payment_from_date", payment_from_date)

        if payment_from_date and payment_to_date and client_id and tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id:
            common_settings = CommonSettings.objects.filter(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entities_id,
                m_processing_layer_id = m_processing_layer_id,
                m_processing_sub_layer_id = m_processing_sub_layer_id,
                setting_key = 'send_email_client'
            )

            for setting in common_settings:
                send_email_client_query = setting.setting_value

            common_settings_rejections = CommonSettings.objects.filter(
                tenants_id=tenants_id,
                groups_id=groups_id,
                entities_id=entities_id,
                m_processing_layer_id=m_processing_layer_id,
                m_processing_sub_layer_id=m_processing_sub_layer_id,
                setting_key='send_email_client_rejections'
            )

            for setting in common_settings_rejections:
                send_email_client_rejections_query = setting.setting_value

            send_email_client_query_proper = send_email_client_query.replace("{from_date}", payment_from_date).replace("{to_date}", payment_to_date).replace("{client_id}", client_id)
            send_email_client_query_output = json.loads(execute_sql_query(send_email_client_query_proper, object_type="table"))["data"]

            send_email_client_rejections_query_proper = send_email_client_rejections_query.replace("{from_date}", payment_from_date).replace("{to_date}", payment_to_date).replace("{client_id}", client_id)
            send_email_client_rejections_query_output = json.loads(execute_sql_query(send_email_client_rejections_query_proper, object_type="table"))["data"]
            # print("send_email_client_rejections_query_proper", send_email_client_rejections_query_proper)
            if len(send_email_client_query_output) > 0 or len(send_email_client_rejections_query_output) > 0:
                m_client_details = MasterClientDetails.objects.filter(client_id=client_id)
                for client in m_client_details:
                    email_address = client.email_address
                # print("email_address", email_address)
                send_mail_output = sm.send_mail_client(
                    data_list = send_email_client_query_output,
                    email_address = email_address,
                    payment_from_date = payment_from_date,
                    payment_to_date = payment_to_date,
                    client_id=client_id,
                    rejections_data_list = send_email_client_rejections_query_output
                )

                if send_mail_output:

                    for client in m_client_details:
                        client.last_send_on = timezone.now()
                        client.save()

                    return queryset.filter(m_processing_sub_layer_id=m_processing_sub_layer_id, int_reference_text_8=client_id, is_active=1)

            else:
                return queryset.filter(id = 0)

class MasterClientsDetailsViewGeneric(generics.ListAPIView):
    serializer_class = MasterClientDetailsSerializer

    def get_queryset(self):
        queryset = MasterClientDetails.objects.all()
        client_name = self.request.query_params.get('client_name', '')
        client_id = self.request.query_params.get('client_id', '')

        if client_name:
            return queryset.filter(client_name = client_name, is_active = 1)
        elif client_id:
            return queryset.filter(client_id = client_id, is_active = 1)
        return queryset

class MasterClientDetailsViewSet(viewsets.ModelViewSet):
    queryset = MasterClientDetails.objects.all()
    serializer_class = MasterClientDetailsSerializer

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
                if k == "input_date":
                    input_date = v

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

                transfer_query_proper = transfer_query.replace("{params}", "'" + input_date + "'")
                print("transfer_query_proper", transfer_query_proper)

                transfer_query_output = execute_sql_query(transfer_query_proper, object_type="Normal")

                if transfer_query_output == "Success":
                    return JsonResponse({"Status": "Success"})
                else:
                    return JsonResponse({"Status": "Error"})
            else:
                return JsonResponse({"Status": "Error"})

    except Exception:
        logger.error("Error in Get Store Files!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

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

@csrf_exempt
def get_upload_files(request, *args, **kwargs):
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
            file_upload_type = request.POST.get("fileUploadType")
            input_date = request.POST.get("inputDate")
            file_path = ''
            m_source_id = ''
            processing_layer_name = ''
            # print("file_upload_type", file_upload_type)
            status = ''

            if file_upload_type == "alcs":
                status = 'BATCH_ALL'
                m_source_id = 100
                processing_layer_name = 'ALCS-RECON'
                file_path = "G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ALCS_ALL/ALCS/input/"
                file_uploads = FileUploads.objects.filter(m_source_id__in = [1, 5, 3, 7, 100], status__in = ['BATCH', 'BATCH_ALL'])
                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "bank":
                status = 'BATCH_ALL'
                m_source_id = 101
                processing_layer_name = 'ALCS-RECON'
                file_path = "G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ALCS_ALL/BANK/input/"
                file_uploads = FileUploads.objects.filter(m_source_id__in = [2, 4, 6, 8, 10, 101], status__in = ['BATCH', 'BATCH_ALL'])
                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "hdfc-utr":
                status = 'BATCH1'
                m_source_id = 11
                processing_layer_name = 'HDFC NEFT LETTERS RECON'
                file_path = "G:/AdventsProduct/V1.1.0/AFS/Sources/Data/HDFC_NEFT_UTR/input/"
                file_uploads = FileUploads.objects.filter(m_source_id__in = [11], status = 'BATCH')
                # print("file_uploads", file_uploads)
                if file_uploads:
                    # print("file_uploads_inside", file_uploads)
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "alcs-manual":
                status = 'BATCH_ALL'
                m_source_id = 102
                processing_layer_name = 'ALCS-RECON'
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ALCS_MANUAL_ALL/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in = [102], status = 'BATCH_ALL')

                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "alcs-icici-neft":
                status = 'BATCH1'
                m_source_id = 12
                processing_layer_name = 'ICICI NEFT LETTERS RECON'
                processing_layer_id = 405
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ALCS_ICICI240_NEFT/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in=[12], status='BATCH')

                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "icici-reversal":
                status = 'BATCH1'
                m_source_id = 13
                processing_layer_name = 'ICICI NEFT LETTERS RECON'
                processing_layer_id = 405
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ICICI_NEFT_UTR/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in=[13], status='BATCH_ALL')

                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "icici-nurture":
                status = 'BATCH1'
                m_source_id = 14
                processing_layer_name = 'ICICI NEFT LETTERS RECON'
                processing_layer_id = 405
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ICICI_NURTURE/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in=[14], status='BATCH_ALL')

                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

                elif file_upload_type == "alcs-icici-neft-1":
                    status = 'BATCH1'
                    m_source_id = 101
                    processing_layer_id = 500
                    processing_layer_name = 'ICICI NEFT LETTERS RECON'
                    file_path = "G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ALCS_ICICI240_NEFT_1/input/"
                    file_uploads = FileUploads.objects.filter(m_source_id__in=[101], status='BATCH')

                    if file_uploads:
                        return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

                elif file_upload_type == "icici-reversal-1":
                    status = 'BATCH1'
                    m_source_id = 102
                    processing_layer_name = 'ICICI NEFT LETTERS RECON'
                    processing_layer_id = 500
                    file_path = 'G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ICICI_NEFT_UTR_1/input/'
                    file_uploads = FileUploads.objects.filter(m_source_id__in=[103], status='BATCH')

                    if file_uploads:
                        return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            if len(file_path) > 0:
                file_name_with_date = file_path + get_proper_file_name(file_name)
                # print("File Name with Date", file_name_with_date)

                with open(file_name_with_date, 'wb+') as destination:
                    for chunk in request.FILES["fileName"]:
                        destination.write(chunk)
                file_size = Path(file_name_with_date).stat().st_size

                FileUploads.objects.create(
                    tenants_id = tenant_id,
                    groups_id = groups_id,
                    entities_id = entity_id,
                    m_source_id = m_source_id,
                    m_processing_layer_id = m_processing_layer_id,
                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id,
                    processing_layer_name = processing_layer_name,
                    source_type = 'FILE',
                    extraction_type = 'UPLOAD',
                    file_name = file_name_with_date.split("/")[-1],
                    file_size_bytes = file_size,
                    file_path = file_name_with_date,
                    input_date = input_date,
                    status = status,
                    comments = 'File in Batch!!!',
                    file_row_count = None,
                    is_processed = 0,
                    is_processing = 0,
                    is_active = 1,
                    created_by = user_id,
                    created_date = timezone.now(),
                    modified_by = user_id,
                    modified_date = timezone.now(),
                    file_upload_type = file_upload_type
                )

                return JsonResponse({"Status": "Success", "Message": "File Uploaded Successfully!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "File Upload Type Wrong!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Upload Files!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_daily_letters_report(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if  k == "tenantsId":
                    tenants_id = v
                if k == "groupsId":
                    groups_id = v
                if k == "entitiesId":
                    entities_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if  k == "paymentDate":
                    payment_date = v

            reco_settings_hdfc_neft = RecoSettings.objects.filter(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entities_id,
                m_processing_layer_id = m_processing_layer_id,
                m_processing_sub_layer_id = m_processing_sub_layer_id,
                processing_layer_id = 404,
                setting_key = 'daily_letters_report'
            )

            common_settings = CommonSettings.objects.filter(
                tenants_id=tenants_id,
                groups_id=groups_id,
                entities_id=entities_id,
                m_processing_layer_id=m_processing_layer_id,
                m_processing_sub_layer_id=m_processing_sub_layer_id,
                setting_key='daily_letters_report'
            )

            for setting in reco_settings_hdfc_neft:
                daily_letters_hdfc_neft_query = setting.setting_value

            for setting in common_settings:
                daily_letters_query_common = setting.setting_value

            daily_letters_hdfc_neft_query_proper = daily_letters_hdfc_neft_query.replace('{payment_date}', payment_date)
            daily_letters_query_common_proper = daily_letters_query_common.replace('{payment_date}', payment_date)

            daily_letters_axis = daily_letters_query_common_proper.replace("{processing_layer_id}", str(400))
            daily_letters_icici = daily_letters_query_common_proper.replace("{processing_layer_id}", str(401))
            daily_letters_sbi = daily_letters_query_common_proper.replace("{processing_layer_id}", str(402))
            daily_letters_hdfc = daily_letters_query_common_proper.replace("{processing_layer_id}", str(403))
            daily_letters_hdfc_neft = daily_letters_hdfc_neft_query_proper.replace("{processing_layer_id}", str(404))
            daily_letters_icici_neft = daily_letters_query_common_proper.replace("{processing_layer_id}", str(405))

            daily_letters_axis_query_output = json.loads(execute_sql_query(daily_letters_axis, object_type="table"))
            daily_letters_icici_query_output = json.loads(execute_sql_query(daily_letters_icici, object_type="table"))
            daily_letters_sbi_query_output = json.loads(execute_sql_query(daily_letters_sbi, object_type="table"))
            daily_letters_hdfc_query_otput = json.loads(execute_sql_query(daily_letters_hdfc, object_type="table"))
            daily_letters_hdfc_neft_query_output = json.loads(execute_sql_query(daily_letters_hdfc_neft, object_type="table"))
            daily_letters_icici_neft_query_output = json.loads(execute_sql_query(daily_letters_icici_neft, object_type="table"))

            daily_letters_data_list = list()
            daily_letters_output = dict()

            headers = daily_letters_axis_query_output["headers"]
            daily_letters_axis_query_output_data = daily_letters_axis_query_output['data']
            daily_letters_icici_query_output_data = daily_letters_icici_query_output['data']
            daily_letters_sbi_query_output_data = daily_letters_sbi_query_output['data']
            daily_letters_hdfc_query_output_data = daily_letters_hdfc_query_otput['data']
            daily_letters_hdfc_neft_query_output_data = daily_letters_hdfc_neft_query_output['data']
            daily_letters_icici_neft_query_output_data = daily_letters_icici_neft_query_output['data']

            daily_letters_output["headers"] = headers
            daily_letters_output["data"] = daily_letters_data_list

            for data in daily_letters_axis_query_output_data:
                daily_letters_data_list.append(data)

            for data in daily_letters_icici_query_output_data:
                daily_letters_data_list.append(data)

            for data in daily_letters_sbi_query_output_data:
                daily_letters_data_list.append(data)

            for data in daily_letters_hdfc_query_output_data:
                daily_letters_data_list.append(data)

            for data in daily_letters_hdfc_neft_query_output_data:
                daily_letters_data_list.append(data)

            for data in daily_letters_icici_neft_query_output_data:
                daily_letters_data_list.append(data)

            # print("axis", daily_letters_axis_query_output)
            # print("icici", daily_letters_icici_query_output)
            # print("sbi", daily_letters_sbi_query_output)
            # print("hdfc", daily_letters_hdfc_query_otput)
            # print("hdfc_neft", daily_letters_hdfc_neft_query_output)
            # print("icici_neft", daily_letters_icici_neft_query_output)

            # print(daily_letters_data_list)

            # queryset = InternalRecords.objects.all()

            # value = list(InternalRecords.objects.filter(int_extracted_text_50=payment_date, tenants_id=tenants_id, groups_id=groups_id,
            #                              entities_id=entities_id,
            #                              m_processing_layer_id=m_processing_layer_id,
            #                              m_processing_sub_layer_id=m_processing_sub_layer_id,
            #                              processing_layer_id__in = [400, 401, 402, 403, 404],
            #                              is_active=1).values(
            #     'int_reference_text_1', 'int_reference_text_14', 'int_extracted_text_6', 'int_extracted_text_7', 'int_reference_date_time_2',
            #     'int_generated_num_2', 'int_extracted_text_50', 'int_amount_2', 'int_reference_date_time_3', 'int_reference_date_time_4'
            # ).order_by('processing_layer_id', 'int_generated_num_2').annotate(total=Sum('int_amount_1')))

            return JsonResponse({"Status": "Success", "data": daily_letters_output})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Daily Letters Report Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_utr_file_update(request, *args, **kwargs):
    try:
        if request.method == 'POST':
            tenants_id = request.POST.get("tenantsId")
            groups_id = request.POST.get("groupsId")
            entities_id = request.POST.get("entityId")
            m_processing_layer_id = request.POST.get("mProcessingLayerId")
            m_processing_sub_layer_id = request.POST.get("mProcessingSubLayerId")
            payment_date = request.POST.get("paymentDate")

            file_name = request.FILES["fileName"].name
            file_path = 'G:/AdventsProduct/V1.1.0/AFS/ALCSRecon/static/UTR/Input/'
            file_name_with_date = file_path + get_proper_file_name(file_name)

            with open(file_name_with_date, 'wb+') as destination:
                for chunk in request.FILES["fileName"]:
                    destination.write(chunk)

            utr_file_functions = utr.ValidateUTRFile(utr_file_path = file_name_with_date)
            validate_file = utr_file_functions.check_utr_columns()
            if validate_file == "Success":
                utr_file_data = utr_file_functions.read_file_proper()
                if utr_file_data == "NoData":
                    return JsonResponse({"Status": "NoData"})
                elif utr_file_data == "Success":
                    utr_file_functions.get_utr_proper_data()
                    common_settings = CommonSettings.objects.filter(
                        tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, setting_key = 'alcs_upload_temp_query', is_active = 1
                    )

                    for setting in common_settings:
                        alcs_upload_temp_query = setting.setting_value

                    alcs_upload_temp_query_proper = alcs_upload_temp_query.replace("{payment_date}", payment_date)

                    alcs_upload_temp_query_output = json.loads(execute_sql_query(alcs_upload_temp_query_proper, object_type="table"))

                    # print("alcs_upload_temp_query_output")
                    # print(alcs_upload_temp_query_output)

                    internal_data_list = alcs_upload_temp_query_output['data']

                    # internal_data_list = list(InternalRecords.objects.filter(
                    #     tenants_id = tenants_id,
                    #     groups_id = groups_id,
                    #     entities_id = entities_id,
                    #     m_processing_layer_id = m_processing_layer_id,
                    #     m_processing_sub_layer_id = m_processing_sub_layer_id,
                    #     int_processing_status_1__isnull=True,
                    #     is_active=1
                    # ).values('int_reference_text_1', 'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_10', 'int_reference_text_14', 'int_reference_text_15', 'int_reference_text_16', 'int_amount_1', 'int_amount_2', 'int_reference_date_time_3', 'int_reference_date_time_5'))

                    update_utr_file = utr_file_functions.update_utr_values(internal_records_list = internal_data_list)
                    if update_utr_file["Status"] == "Success":
                        return JsonResponse({"Status": "Success", "report_url": update_utr_file["report_url"]})
                    elif update_utr_file["Status"] == "Error":
                        return JsonResponse({"Status": "Error"})
            elif validate_file == "ColumnMismatch":
                return JsonResponse({"Status": "columnMismatch"})
            elif validate_file == "ColumnCount":
                return JsonResponse({"Status": "columnCount"})
            elif validate_file == "Error":
                return JsonResponse({"Status": "Error"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get UTR File Update Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_transaction_count(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "letterDate":
                    letter_date = v

            reco_settings = RecoSettings.objects.filter(
                setting_key = 'transaction_count_query',
                processing_layer_id = processing_layer_id,
                is_active = 1
            )

            for setting in reco_settings:
                transaction_count_query = setting.setting_value

            transaction_count_query_proper = transaction_count_query.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date)
            transaction_count_query_output = json.loads(execute_sql_query(transaction_count_query_proper, object_type="table"))

            return JsonResponse({"Status": "Success", "data": transaction_count_query_output})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Transaction Count Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_transaction_records(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "procesingLayerId":
                    processing_layer_id =  v
                if k == "status":
                    status = v
                if k == "letterDate":
                    letter_date = v

            reco_settings_alcs = RecoSettings.objects.filter(
                setting_key = 'transaction_list_query_alcs', is_active = 1, processing_layer_id = processing_layer_id
            )

            reco_settings_bank = RecoSettings.objects.filter(
                setting_key = 'transaction_list_query_bank', is_active = 1, processing_layer_id = processing_layer_id
            )

            for setting in reco_settings_alcs:
                transaction_list_query_alcs = setting.setting_value

            for setting in reco_settings_bank:
                transaction_list_query_bank = setting.setting_value

            if status == 'UnMatched':
                transaction_list_query_alcs_proper = transaction_list_query_alcs.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace('{null}', 'NULL').replace("{CONDITIONS}", "")
                transaction_list_query_alcs_output = json.loads(execute_sql_query(transaction_list_query_alcs_proper, object_type="table"))

                transaction_list_query_bank_proper = transaction_list_query_bank.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace("{CONDITIONS}", "")
                transaction_list_query_bank_output = json.loads(execute_sql_query(transaction_list_query_bank_proper, object_type="table"))

                data = {
                    "alcs": transaction_list_query_alcs_output,
                    "bank": transaction_list_query_bank_output
                }

                return JsonResponse({"Status": "Success", "data": data})

            elif status == 'Matched':
                transaction_list_query_alcs_proper = transaction_list_query_alcs.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace('{null}', 'NOT NULL')
                transaction_list_query_alcs_output = json.loads(execute_sql_query(transaction_list_query_alcs_proper, object_type="table"))
                return JsonResponse({"Status": "Success", "data": transaction_list_query_alcs_output})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Transaction Records Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_internal_transaction_records(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "paymentDateList":
                    payment_date_list = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "letterDate":
                    letter_date = v

            reco_settings_alcs = RecoSettings.objects.filter(
                setting_key = 'transaction_list_query_alcs', is_active = 1, processing_layer_id = processing_layer_id
            )

            for setting in reco_settings_alcs:
                transaction_list_query_alcs = setting.setting_value

            payment_date_tuple_string = str(payment_date_list).replace("[", "(").replace("]", ")")

            transaction_list_query_alcs_proper = transaction_list_query_alcs.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace('{null}', 'NULL').replace("{CONDITIONS}", " AND int_reference_text_13 IN " + payment_date_tuple_string)
            transaction_list_query_alcs_output = json.loads(execute_sql_query(transaction_list_query_alcs_proper, object_type="table"))

            return JsonResponse({"Status": "Success", "data": transaction_list_query_alcs_output})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Internal Transaction Records Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_unmatched_transactions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "externalRecordsId":
                    external_records_id = v
                if k == "internalRecordsIdList":
                    internal_records_ids_list = v

            t_external_records = ExternalRecords.objects.filter(
                id = external_records_id
            )

            for record in t_external_records:
                utr_reference = record.ext_extracted_text_1
                debit_date = record.ext_reference_date_time_1

            # print("external_records_id", external_records_id)
            # print("internal_records_ids_list", internal_records_ids_list)
            # print("utr_reference", utr_reference)
            # print("debit_date", debit_date)

            InternalRecords.objects.filter(
                id__in = internal_records_ids_list
            ).update(int_reference_text_14 = utr_reference, int_reference_date_time_2 = debit_date, int_generated_num_2 = None)

            return JsonResponse({"Status": "Success", "Message": "Records Updated Successfully!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Update UnMatched Transactions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_letter_numbers(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "letterDate":
                    letter_date = v

            common_settings = CommonSettings.objects.filter(
                setting_key = 'max_gen_num_query',
                is_active = 1
            )

            for setting in common_settings:
                max_gen_num_query = setting.setting_value

            common_settings = CommonSettings.objects.filter(
                setting_key = 'gen_num_not_updated_list_query',
                is_active=1
            )

            for setting in common_settings:
                gen_num_not_updated_list_query = setting.setting_value

            common_settings = CommonSettings.objects.filter(
                setting_key='gen_num_not_updated_id_list_query',
                is_active=1
            )

            for setting in common_settings:
                gen_num_not_updated_id_list_query = setting.setting_value

            max_gen_num_query_proper = max_gen_num_query.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace("{null}", "NULL")
            max_gen_num_query_output = json.loads(execute_sql_query(max_gen_num_query_proper, object_type="table"))

            gen_num_not_updated_list_query_proper = gen_num_not_updated_list_query.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace("{null}", "NULL")
            gen_num_not_updated_list_query_output = json.loads(execute_sql_query(gen_num_not_updated_list_query_proper, object_type="table"))

            max_generated_number = int(max_gen_num_query_output['data'][0]['max_num'])
            utr_number_output_data_list = gen_num_not_updated_list_query_output['data']

            utr_list = list()

            for utr_number in utr_number_output_data_list:
                utr_list.append(utr_number['int_reference_text_14'])

            for utr in utr_list:
                gen_num_not_updated_id_list_query_proper = gen_num_not_updated_id_list_query.replace("{processing_layer_id}", str(processing_layer_id)).replace("{letter_date}", letter_date).replace("{utr_number}", utr)
                gen_num_not_updated_id_list_query_output = json.loads(execute_sql_query(gen_num_not_updated_id_list_query_proper, object_type="table"))

                gen_num_not_updated_id_data_output_list = gen_num_not_updated_id_list_query_output["data"]

                gen_num_to_be_updated_ids_list = []

                for id in gen_num_not_updated_id_data_output_list:
                    gen_num_to_be_updated_ids_list.append(id["id"])

                InternalRecords.objects.filter(
                    id__in = gen_num_to_be_updated_ids_list
                ).update(int_generated_num_2 = max_generated_number + 1)

                max_generated_number = max_generated_number + 1

            return JsonResponse({"Status": "Success", "Message": "Letter Number Updated Successfully!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Update UnMatched Transactions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_reject_all_transactions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "internalRecordsIdList":
                    internal_records_id_list = v
                if k == "userId":
                    user_id = v

            InternalRecords.objects.filter(
                id__in = internal_records_id_list
            ).update(int_processing_status_1 = 'Rejected', modified_by = user_id, modified_date = timezone.now())

            return JsonResponse({"Status": "Success", "Message": "Reject All Records Updated Successfully!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Update Reject All Transactions Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_auto_send_mail_to_clients(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "tenantsId":
                    tenants_id = v
                if k == "groupsId":
                    groups_id = v
                if k == "entitiesId":
                    entities_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "paymentFromDate":
                    payment_from_date = v
                if k == "paymentToDate":
                    payment_to_date = v

            common_settings = CommonSettings.objects.filter(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entities_id,
                m_processing_layer_id = m_processing_layer_id,
                m_processing_sub_layer_id = m_processing_sub_layer_id,
                setting_key = 'send_auto_email_client_query'
            )

            for setting in common_settings:
                send_auto_email_client_query = setting.setting_value

            send_auto_email_client_query_proper = send_auto_email_client_query.replace("{payment_from_date}", payment_from_date)

            send_auto_email_client_query_output = json.loads(execute_sql_query(send_auto_email_client_query_proper, object_type="table"))

            client_id_data = send_auto_email_client_query_output["data"]

            client_id_list = []

            for data in client_id_data:
                client_id_list.append(data["client_id"])

            print(client_id_list)

            etl = SendRequest()

            headers = {
                "Content-Type": "application/json"
            }

            for client_id in client_id_list:

                post_url = "http://localhost:50010/api/v1/alcs/generic/send_mail_client/?paymentFromDate={payment_from_date}&clientId={client_id}&paymentToDate={payment_to_date}&tenantsId={tenants_id}&groupsId={groups_id}&entitiesId={entities_id}&mProcessingLayerId={m_processing_layer_id}&mProcessingSubLayerId={m_processing_sub_layer_id}"
                post_url_proper = post_url.replace("{payment_from_date}", payment_from_date).replace("{payment_to_date}", payment_to_date).replace("{tenants_id}", str(tenants_id)).replace("{groups_id}", str(groups_id)).replace("{entities_id}", str(entities_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{client_id}", client_id)

                # print("post_url_proper", post_url_proper)
                mail_send_response = etl.get_response(post_url = post_url_proper, headers=headers, data='')

                print("The mail sent status of {} is {}".format(client_id, mail_send_response))

            return JsonResponse({"Status": "Success", "Message": "Email Sent to Clients Successfully!!!"})
        return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Auto Send Mail to Clients Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})