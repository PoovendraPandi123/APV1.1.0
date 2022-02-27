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
            return queryset.filter(int_extracted_text_50 = payment_date, int_reference_text_8 = client_id, is_active = 1)
        elif payment_date and tenants_id and groups_id and entities_id and m_processing_layer_id and m_processing_sub_layer_id and processing_layer_id:
            return queryset.filter(int_extracted_text_50 = payment_date, tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id,
                                   m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)

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

        print("payment_from_date", payment_from_date)

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

            send_email_client_query_proper = send_email_client_query.replace("{from_date}", payment_from_date).replace("{to_date}", payment_to_date).replace("{client_id}", client_id)
            send_email_client_query_output = json.loads(execute_sql_query(send_email_client_query_proper, object_type="table"))["data"]
            if len(send_email_client_query_output) > 0:
                m_client_details = MasterClientDetails.objects.filter(client_id=client_id)
                for client in m_client_details:
                    email_address = client.email_address

                send_mail_output = sm.send_mail_client(
                    data_list = send_email_client_query_output,
                    email_address = email_address,
                    payment_from_date = payment_from_date,
                    payment_to_date = payment_to_date,
                    client_id=client_id
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
                status = 'BATCH'
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
                file_path = 'G:/AdventsProduct/V1.0.0/AFS/Sources/Data/ALCS_MANUAL_ALL/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in = [102], status = 'BATCH_ALL')

                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "alcs-icici-neft":
                status = 'BATCH'
                m_source_id = 103
                processing_layer_name = 'ALCS-RECON'
                file_path = 'G:/AdventsProduct/V1.0.0/AFS/Sources/Data/ALCS_ICICI240_NEFT/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in=[103], status='BATCH')

                if file_uploads:
                    return JsonResponse({"Status": "Exists", "Message": "File Already Exists in BATCH!!!"})

            elif file_upload_type == "icici-reversal":
                status = 'BATCH'
                m_source_id = 12
                processing_layer_name = 'ICICI NEFT LETTERS RECON'
                file_path = 'G:/AdventsProduct/V1.1.0/AFS/Sources/Data/ICICI_NEFT_UTR/input/'
                file_uploads = FileUploads.objects.filter(m_source_id__in=[12], status='BATCH_ALL')

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

            common_settings = CommonSettings.objects.filter(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entities_id,
                m_processing_layer_id = m_processing_layer_id,
                m_processing_sub_layer_id = m_processing_sub_layer_id,
                setting_key = 'daily_letters_report'
            )

            for setting in common_settings:
                daily_letters_query = setting.setting_value

            daily_letters_query_proper = daily_letters_query.replace('{payment_date}', payment_date)
            # print(daily_letters_query_proper)

            daily_letters_query_output = json.loads(execute_sql_query(daily_letters_query_proper, object_type="table"))

            # print(daily_letters_query_output)

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

            return JsonResponse({"Status": "Success", "data": daily_letters_query_output})
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

                    internal_data_list = list(InternalRecords.objects.filter(
                        tenants_id = tenants_id,
                        groups_id = groups_id,
                        entities_id = entities_id,
                        m_processing_layer_id = m_processing_layer_id,
                        m_processing_sub_layer_id = m_processing_sub_layer_id,
                        int_extracted_text_50 = payment_date
                    ).values('int_reference_text_7', 'int_reference_text_8', 'int_reference_text_10', 'int_reference_text_14', 'int_amount_1', 'int_amount_2', 'int_reference_date_time_3'))

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
