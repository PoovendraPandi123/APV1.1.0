import logging
import json
from django.views.decorators.csrf import csrf_exempt
from .models import *
from django.http import JsonResponse
import uuid
from django.db import connection
import pandas as pd
from rest_framework import viewsets
from rest_framework.generics import ListAPIView
from .serializers import *
from datetime import datetime
from .keyword_check import KeywordsUniqueCheck
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
                                       processing_layer_id=processing_layer_id, is_active=1, status=status)

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

            keywords_unique_check = KeywordsUniqueCheck(keyword = keywords.lower())
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
                    source_def_list = value

            if SourceDefinitions.objects.filter(sources_id = sources_id, is_active = True).exists():
                source_def_v = SourceDefinitions.objects.filter(sources_id = sources_id, is_active = True)
                # print("source_definition_list", source_def_v)
                for setting in source_def_v:
                    setting.is_active = False
                    print("inside loop source_list", setting)
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
                elif key == "entities_id":
                    entities_id = value
                elif key == "m_processing_layer_id":
                    m_processing_layer_id = value
                elif key == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = value
                elif key == "processing_layer_id":
                    processing_layer_id = value
                elif key == "target_files_id":
                    target_files_id = value
                elif key == "target_def_list":
                    target_def_list = value
                elif key == "user_id":
                    user_id = value


            if TargetFileDefinitions.objects.filter(target_files_id = target_files_id, is_active = True).exists():
                target_def_v = TargetFileDefinitions.objects.filter(target_files_id = target_files_id, is_active = True)
                for setting in target_def_v:
                    setting.is_active = False
                    setting.save()

            for target_def in target_def_list:
                TargetFileDefinitions.objects.create(
                    tenants_id = tenants_id,
                    groups_id = groups_id,
                    entities_id = entities_id,
                    m_processing_layer_id = m_processing_layer_id,
                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id,
                    field_name = target_def["fieldName"],
                    field_sequence = str(target_def["fieldPosition"]),
                    files_config = None,
                    is_active = True,
                    created_by = user_id,
                    created_date = str(datetime.today()),
                    modified_by = user_id,
                    modified_date = str(datetime.today()),
                    target_files_id = target_files_id
                )

            return JsonResponse({"Status": "Success", "Message": "Target Definitions Created Successfully!!!"})
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

            if TargetFileDefinitions.objects.filter(target_files_id = target_id, id = target_def_id, is_active = True).exists():
                target_files_definitions = TargetFileDefinitions.objects.filter(id = target_def_id, target_files_id = target_id, is_active = True)
                print(target_files_definitions)
                for setting in target_files_definitions:
                    setting.files_config = target_def_list
                    setting.save()

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
            pass
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_file_gst_month(request, *args, **kwargs):
    try:
        if request.method == "POST":
            pass
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_file_gst_month_all(request, *args, **kwargs):
    try:
        if request.method == "POST":
            pass
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Validate Error to Batch Function!!!", exc_info=True)
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
#
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
# def storage_config(types):
#     try:
#         count_for_text = 0
#         count_for_int = 0
#         # count_for_Dec = 0
#         # count_for_Date = 0
#         coloumn_name_list = []
#         for i in range(0, len(types)):
#             if types[i] == 'Str':
#                 count_for_text = count_for_text + 1
#                 name = 'Reference_Text' + str(count_for_text)
#                 coloumn_name_list.append(name)
#             elif types[i] == 'Integer':
#                 count_for_int = count_for_int + 1
#                 name = 'Reference_Int' + str(count_for_int)
#                 coloumn_name_list.append(name)
#         print(coloumn_name_list)
#         print(count_for_text)
#         print(count_for_int)
#     except Exception as e:
#         print(e)
#         return {"Status": "Error"}
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

