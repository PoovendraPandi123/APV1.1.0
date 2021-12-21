from django.http import JsonResponse
import logging
import json
from django.db import connection
from .packages import read_files, validate_files
from django.utils import timezone

# Create your views here.

logger = logging.getLogger("etl_service_one")

def execute_sql_query(query, object_type):
    try:
        with connection.cursor() as cursor:
            if object_type == "Normal":
                cursor.execute(query)
                return "Success"
            else:
                return None
    except Exception as e:
        logger.info(query)
        logger.error(str(e))
        logger.error("Error in Executing SQL Query", exc_info=True)
        return None

def reading_file_view(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "source_input_location":
                    source_input_location = v
                if k == "source_name":
                    source_name = v
                if k == "source_config":
                    source_config = v

            for k,v in source_config.items():
                if k == "column_start_row":
                    column_start_row = v
                if k == "source_extension":
                    source_extension = v
            file_path = source_input_location + source_name + source_extension

        file_read_output = read_files.read_file_for_source(file_path=file_path, column_start_row = column_start_row, source_extension = source_extension)

        if file_read_output["Status"] == "Error":
            logger.info("***** Error in File Read Output!!! *****")
            logger.info(file_read_output["Message"])
            return JsonResponse({"Status": "Error", "Message": "Error in File Read Output!!!"})

        elif file_read_output["Status"] == "Success":
            logger.info("File Read Successfully!!!")

        data_header = file_read_output["data_header"]
        return JsonResponse({"Status" : "Success", "data_header" : data_header})

    except Exception:
        logger.error("***** Error in Reading File!!! *****", exc_info=True)
        return JsonResponse({"Status": "Error"})

def read_file_view(request, *args, **kwargs):
    try:
        if request.method == "GET":
            if request.headers['source-config-value']:
                source_config_value = request.headers['source-config-value']
                if request.headers['source-name']:
                    source_name = request.headers['source-name']
                    if request.headers['file-uploads-id']:
                        file_uploads_id = request.headers['file-uploads-id']
                        if request.headers['users-id']:
                            users_id = request.headers['users-id']
                            if request.headers['tenants-id']:
                                tenants_id = request.headers['tenants-id']
                                if request.headers['groups-id']:
                                    groups_id = request.headers['groups-id']
                                    if request.headers['entities-id']:
                                        entities_id = request.headers['entities-id']
                                        if request.headers['m-sources-id']:
                                            m_sources_id = request.headers['m-sources-id']

                                            logger.info(str(file_uploads_id))

                                            body = request.body.decode('utf-8')
                                            data = json.loads(body)

                                            for k,v in data.items():
                                                if k == "m_source_definition_list":
                                                    source_definitions = v
                                                if k == "file_path":
                                                    file_path = v

                                            file_read_output = read_files.get_read_file(file_path = file_path, source_definitions = source_definitions)

                                            if file_read_output["Status"] == "Error":
                                                logger.info("***** Error in File Read Output!!! *****")
                                                logger.info(file_read_output["message"])
                                                return JsonResponse({"Status": "Error", "Message" : "Error in File Read Output!!!"})

                                            elif file_read_output["Status"] == "Success":
                                                logger.info("File Read Successfully!!!")

                                                # Data from file
                                                data = file_read_output["data"]
                                                data_columns = file_read_output["data_columns"]
                                                attribute_reference_fields = file_read_output["attribute_reference_fields"]
                                                attribute_table = file_read_output["attribute_table"]

                                                # Creating Common Fields or query
                                                common_fields = " is_active, created_by, created_date, modified_by, modified_date, m_sources_id, tenants_id, groups_id, entities_id, file_uploads_id, m_source_name, processingStatus, processing_date_time) VALUES insert_records"
                                                query = "INSERT INTO " + attribute_table + " ("

                                                for field in attribute_reference_fields:
                                                    query = query + field + ", "

                                                query_proper = query + common_fields

                                                # convert data into a list
                                                # Iterate Over Each Row
                                                data_rows_list = []
                                                for index, rows in data.iterrows():
                                                    # create a list for the current row
                                                    data_list = [rows[column] for column in data_columns]
                                                    data_rows_list.append(data_list)

                                                # Adding Common Necessary Fields to the rows
                                                for row in data_rows_list:
                                                    row.append("1") # For is_active
                                                    row.append(users_id) # For created_by
                                                    row.append(timezone.now()) # For created_date
                                                    row.append(users_id) # For modified_by
                                                    row.append(timezone.now()) # For modified_date
                                                    row.append(m_sources_id) # For m_sources_id
                                                    row.append(tenants_id) # For tenants_id
                                                    row.append(groups_id) # For groups_id
                                                    row.append(entities_id) # For entities_id
                                                    row.append(file_uploads_id) # For file_uploads_id
                                                    row.append(source_name) # For Source Name
                                                    row.append('New') # For processingStatus
                                                    row.append(timezone.now()) # processing_date_time

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

                                                # print(insert_value_string)
                                                final_query = query_proper.replace("insert_records", insert_value_string[:-1])

                                                # Loading to the Proper Table
                                                load_output = execute_sql_query(final_query, object_type = "Normal")
                                                if load_output == "Success":
                                                    final_sp = source_config_value.replace("vFileId", str(file_uploads_id))
                                                    sp_out = execute_sql_query(final_sp, object_type="Normal") # Executing SP
                                                    if sp_out == "Success":
                                                        logger.info("File Uploaded Successfully")
                                                        return JsonResponse({"Status": "Success", "Message": "Data Loaded Successfully"})
                                                    elif sp_out is None:
                                                        return JsonResponse({"Status": "Error", "Message": "Error in Executing SP for Read File"})
                                                elif load_output is None:
                                                    return JsonResponse({"Status": "Error", "Message" : "Error in Executing Query for Read File!!!"})
                                        else:
                                            return JsonResponse({"Status" : "Error", "Message" : "Module Id not Found!!!"})
                                    else:
                                        return JsonResponse({"Status" : "Error", "Message" : "Entities Id not Found!!!"})
                                else:
                                    return JsonResponse({"Status" : "Error", "Message" : "Groups Id not Found!!!"})
                            else:
                                return JsonResponse({"status" : "Error", "Message" : "Tenants Id not Found!!!"})
                        else:
                            return JsonResponse({"Status" : "Error", "Message" : "Users Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status" : "Error", "Message" : "File Upload Id Not Found!!!"})
                else:
                    return JsonResponse({"Status" : "Error", "Message" : "Source Name Not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error", "Message" : "Source Config Value Not Found!!!"})
    except Exception:
        logger.error("***** Error in Reading File!!! *****", exc_info=True)
        return JsonResponse({"Status" : "Error"})

def validate_file_view(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "file_path":
                    file_path = v
                if k == "source_config":
                    source_config = v

            validate_file_output = validate_files.validate_file(file_path = file_path, source_config = source_config)

            if validate_file_output["Status"] == "User Error":
                return JsonResponse({"Status" : "Error", "Message" : validate_file_output["Message"]})
            elif validate_file_output["Status"] == "Success":
                return JsonResponse({"Status" : "Success", "data" : validate_file_output["data"]})
        else:
            logger.error("GET Method not Received for Validating File!!!")
            return JsonResponse({"Status" : "Error", "Message" : "GET Method not Received for Validating File!!!"})
    except Exception:
        logger.error("***** Error in Validating File!!! *****", exc_info=True)
        return JsonResponse({"Status" : "Error"})

def get_store_files_view(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "attribute_name_list":
                    attribute_name_list = v
                if k == "attribute_data_types_list":
                    attribute_data_types_list = v
                if k == "source_config":
                    source_config = v
                if k == "details":
                    details = v

            for k,v in source_config.items():
                if k == "sheet_name":
                    sheet_name = v
                if k == "source_password":
                    source_password = v
                if k == "column_start_row":
                    column_start_row = v
                if k == "source_extension":
                    source_extension = v
                if k == "password_protected":
                    password_protected = v

            for k,v in details.items():
                if k == "source_name":
                    source_name = v
                if k == "insert_query":
                    insert_query = v
                if k == "sp":
                    sp = v
                if k == "file_path":
                    file_path = v
                if k == "m_sources_id":
                    m_sources_id = v
                if k == "tenants_id":
                    tenants_id = v
                if k == "groups_id":
                    groups_id = v
                if k == "entities_id":
                    entities_id = v
                if k == "module_id":
                    module_id = v
                if k == "users_id":
                    users_id = v
                if k == "file_uploads_id":
                    file_uploads_id = v

            read_file_output = read_files.get_data_from_file(
                file_path = file_path,
                sheet_name = sheet_name,
                source_extension = source_extension,
                attribute_list = attribute_name_list,
                column_start_row = column_start_row,
                password_protected = password_protected,
                source_password = source_password,
                attribute_data_types_list = attribute_data_types_list
            )

            if read_file_output["Status"] == "Success":

                data = read_file_output["data"]["data"]
                # print(data["INVOICE DATE"])
                # convert data into a list
                # Iterate Over Each Row
                data_rows_list = []
                for index, rows in data.iterrows():
                    # create a list for the current row
                    data_list = [rows[column] for column in data.columns]
                    data_rows_list.append(data_list)

                # Adding Common Necessary Fields to the rows
                for row in data_rows_list:
                    row.append(tenants_id) # Tenants Id
                    row.append(groups_id) # Groups Id
                    row.append(entities_id) # Entities Id
                    row.append(module_id) # Module Id
                    row.append(1) # is active
                    row.append(users_id) # created_by
                    row.append(timezone.now()) # created_date
                    row.append(users_id) # modified_by
                    row.append(timezone.now()) # modified_date
                    row.append(file_uploads_id) # File Uploads Id
                    row.append(m_sources_id) # Source Id
                    row.append(source_name) # Source Name
                    row.append("New") # processing_status
                    row.append(timezone.now()) # processing_date_time

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

                final_query = insert_query.replace("{source_values}", insert_value_string[:-1])

                # print(final_query)

                # Loading to the Proper Table
                load_output = execute_sql_query(final_query, object_type="Normal")
                if load_output == "Success":
                    final_sp = sp.replace("vFileId", str(file_uploads_id)).replace("vSourceId", str(m_sources_id))
                    sp_out = execute_sql_query(final_sp, object_type="Normal")  # Executing SP
                    if sp_out == "Success":
                        logger.info("File Uploaded Successfully")
                        return JsonResponse({"Status": "Success", "Message": "Data Loaded Successfully"})
                    elif sp_out is None:
                        return JsonResponse({"Status": "Error", "Message": "Error in Executing SP for Read File"})
                    return JsonResponse({"Status" : "Success"})
                elif load_output is None:
                    return JsonResponse({"Status": "Error", "Message": "Error in Executing Query for Read File!!!"})

            elif read_file_output["Status"] == "Error":
                logger.error("Error in Read File Output!!!")
                logger.error(read_file_output["Message"])
        else:
            logger.error("GET Method not Received for Validating File!!!")
            return JsonResponse({"Status" : "Error", "Message" : "GET Method not Received for Validating File!!!"})
    except Exception:
        logger.error("**** Error in Get Store Files!!! ****", exc_info=True)
        return JsonResponse({"Status" : "Error"})