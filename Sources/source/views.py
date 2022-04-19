import os
import logging
import json
from django.db.models import Exists, OuterRef
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
import uuid
from django.db import connection
import pandas as pd
import requests
from .packages import read_mysql, read_postgres

# Create your views here.

logger = logging.getLogger("sources_one")

@csrf_exempt
def get_database_values(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            database = ''
            host = ''
            port = ''
            username = ''
            password = ''
            type = ''
            schema = ''

            for k,v in data.items():
                if k == "database":
                    database = v
                if k == "host":
                    host = v
                if k == "port":
                    port = v
                if k == "username":
                    username = v
                if k == "password":
                    password = v
                if k == "type":
                    type = v
                if  k == "schema":
                    schema = v
                if  k == "psqlDatabase":
                    psql_database = v

            if database == "mysql":
                if type == "schema":
                    query = "SHOW DATABASES;"
                if type == "table":
                    query = "SHOW TABLES FROM " + schema + ";"

                mysql_db = read_mysql.MySqlDatabase(
                    host=host,
                    username=username,
                    password=password,
                    port=port,
                    query=query
                )
                query_output = mysql_db.get_query_result_proper()
                if len(query_output) > 0:
                    return JsonResponse({"Status": "Success", "data": query_output})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Could not connect to MySQL Database!!!"})

            elif database == "postgres":
                if type == "database":
                    query = "SELECT datname FROM pg_database WHERE datistemplate = false;"

                    psql_db = read_postgres.PostgresDatabase(
                        host=host,
                        username=username,
                        password=password,
                        port=port,
                        query=query,
                        database=''
                    )

                    query_output = psql_db.get_query_result_proper_db_list()
                    if len(query_output) > 0:
                        return JsonResponse({"Status": "Success", "data": query_output})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Could not connect to Postgres Database!!!"})

                elif type == "schema":
                    query = "SELECT nspname FROM pg_catalog.pg_namespace;"

                    psql_db = read_postgres.PostgresDatabase(
                        host=host,
                        username=username,
                        password=password,
                        port=port,
                        query=query,
                        database=psql_database
                    )

                    query_output = psql_db.get_query_result_proper()
                    if len(query_output) > 0:
                        return JsonResponse({"Status": "Success", "data": query_output})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Could not connect to Postgres Database!!!"})

                elif type == "table":
                    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}';".replace("{schema_name}", schema)
                    psql_db = read_postgres.PostgresDatabase(
                        host=host,
                        username=username,
                        password=password,
                        port=port,
                        query=query,
                        database=psql_database
                    )

                    query_output = psql_db.get_query_result_proper()
                    if len(query_output) > 0:
                        return JsonResponse({"Status": "Success", "data": query_output})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Could not connect to Postgres Database!!!"})

            elif database == "oracle":
                return JsonResponse({"Status": "Error", "Message": "Could not connect to Oracle Database!!!"})

            elif database == "sqlServer":
                return JsonResponse({"Status": "Error", "Message": "Could not connect to SQL Server Database!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Unknown Database!!!"})

        else:
            return JsonResponse({"Status" : "Success", "Message" : "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Database Values!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

@csrf_exempt
def get_insert_source(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            source_config = dict()

            for k,v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if  k == "entityId":
                    entity_id = v
                if  k == "userId":
                    user_id = v
                if k == "sourceName":
                    source_name = v
                if k == "sourceType":
                    source_type = v
                if  k == "fileType":
                    file_type = v
                if k == "textSeparator":
                    text_separator = v
                if k == "otherSeparator":
                    other_separator = v
                if k == "filePasswordRequired":
                    file_password_required = v
                if k == "filePassword":
                    file_password = v
                if  k == "sheetNameRequired":
                    sheet_name_required = v
                if  k == "sheetName":
                    sheet_name = v
                if  k == "sheetPasswordRequired":
                    sheet_password_required = v
                if  k == "sheetPassword":
                    sheet_password = v
                if k == "columnStartRow":
                    column_start_row = v
                if  k == "database":
                    database = v
                if  k == "dbHost":
                    db_host = v
                if k == "dbPort":
                    db_port = v
                if k == "dbUserName":
                    db_user_name = v
                if  k == "dbPassword":
                    db_password = v
                if k == "schema":
                    schema = v
                if k == "table":
                    table = v
                if k == "psqlDatabase":
                    psql_database = v
                if k == "importSequence":
                    import_sequence = v

            source_config["source_type"] = source_type
            source_config["file_type"] = file_type
            source_config["text_separator"] = text_separator
            source_config["other_separator"] = other_separator
            source_config["file_password_required"] = file_password_required
            source_config["file_password"] = file_password
            source_config["sheet_name_required"] = sheet_name_required
            source_config["sheet_name"] = sheet_name
            source_config["sheet_password_required"] = sheet_password_required
            source_config["sheet_password"] = sheet_password
            source_config["column_start_row"] = column_start_row
            source_config["database"] = database
            source_config["db_host"] = db_host
            source_config["db_port"] = db_port
            source_config["db_user_name"] = db_user_name
            source_config["db_password"] = db_password
            source_config["schema"] = schema
            source_config["table"] = table
            source_config["psql_database"] = psql_database

            source_settings = SourceSettings.objects.filter(
                setting_key = 'source_path'
            )

            for setting in source_settings:
                source_path = setting.setting_value

            source_input_location = source_path["sourceInputPath"].replace("{source_name}", source_name.upper().replace(" ", "_"))

            MasterSources.objects.create(
                tenants_id = tenant_id,
                groups_id = group_id,
                entities_id = entity_id,
                source_code = str(uuid.uuid4()),
                source_name = source_name,
                source_config = source_config,
                source_input_location = source_input_location,
                source_import_seq = import_sequence,
                source_field_number = 1,
                is_active = 1,
                created_by = user_id,
                created_date = timezone.now(),
                modified_by = user_id,
                modified_date = timezone.now()
            )

            return JsonResponse({"Status": "Success", "Message": "Records Inserted Successfully"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method not received!!!"})
    except Exception:
        logger.error("Error in Updating Source!!!", exc_info=True)

# @csrf_exempt
# def get_update_source(request, *args, **kwargs):
#
#     try:
#         if request.method == "POST":
#             body = request.body.decode('utf-8')
#             data = json.loads(body)
#
#             source_id = ''
#             tenant_id = ''
#             group_id = ''
#             entity_id = ''
#             user_id = ''
#             source_name = ''
#             source_extension = ''
#             sheet_name = ''
#             column_start_row = ''
#             password_protected = ''
#             source_password = ''
#             source_type = ''
#             audit_flag = 'ON'
#             delimiter = ''
#             db_name = ''
#             schema_name = ''
#             username = ''
#             password = ''
#             import_sequence = ''
#             source_config = ''
#
#             for k,v in data.items():
#                 if k == "tenant_id":
#                     tenant_id = v
#                 if k == "group_id":
#                     group_id = v
#                 if k == "entity_id":
#                     entity_id = v
#                 if k == "user_id":
#                     user_id = v
#                 if k == "source_id":
#                     source_id = v
#                 if k == "source_name":
#                     source_name = v
#                 if k == "source_extension":
#                     source_extension = v
#                 if k == "sheet_name":
#                     sheet_name = v
#                 if k == "column_start_row":
#                     column_start_row = v
#                 if k == "password_protected":
#                     password_protected = v
#                 if k == "source_password":
#                     source_password = v
#                 if k == "source_type":
#                     source_type = v
#                 if k == "import_sequence":
#                     import_sequence = v
#                 if source_extension == ".txt" :
#                     if k == "delimiter":
#                         delimiter = v
#                 if source_type == "DB" :
#                     if k == "db_name":
#                         db_name = v
#                     if k == "schema_name":
#                         schema_name = v
#                     if k == "username":
#                         username = v
#                     if k == "password":
#                         password = v
#
#             if len(str(tenant_id)) > 0:
#                 if len(str(group_id)) > 0:
#                     if len(str(entity_id)) > 0:
#                         if len(str(user_id)) > 0:
#                             if int(source_id) > 0:
#                                 m_sources = MasterSources.objects.filter(id = source_id)
#                                 source_name = source_name.replace(" ", "_")
#                                 for source in m_sources:
#                                     source.source_name = source_name
#                                     source_config = source.source_config
#                                     source_config["sheet_name"] = sheet_name
#                                     source_config["column_start_row"] = column_start_row
#                                     source_config["password_protected"] = password_protected
#                                     source_config["source_password"] = source_password
#                                     source.source_config = source_config
#                                     source.save()
#                                     if(audit_flag=="ON") :
#                                         MasterSourcesAudit.objects.create(
#                                             operation_flag="UPDATE",
#                                             tenants_id=tenant_id,
#                                             groups_id=group_id,
#                                             entities_id=entity_id,
#                                             m_sources_id=source_id,
#                                             source_code=source.source_code,
#                                             source_name=source.source_name,
#                                             source_config=source.source_config,
#                                             source_input_location=source.source_input_location,
#                                             source_field_number=1,
#                                             is_active=1,
#                                             created_by=user_id,
#                                             created_date=timezone.now()
#                                         )
#                                     return JsonResponse({"Status" : "Success", "Message" : "Source Updated Successfully!!!"})
#
#                             elif int(source_id) == 0:
#                                 # print("inside 0 source id")
#                                 if(source_type == 'File') :
#                                     source_config = {
#                                         "source_type": "File",
#                                         "source_extension": source_extension,
#                                         "sheet_name": sheet_name,
#                                         "column_start_row": column_start_row,
#                                         "password_protected": password_protected,
#                                         "source_password": source_password,
#                                         "delimiter": delimiter,
#                                         "password": "No"
#                                     }
#                                 elif (source_type == 'DB'):
#                                     source_config = {
#                                         "source_type": "DB",
#                                         "source_extension": source_extension,
#                                         "sheet_name": sheet_name,
#                                         "column_start_row": column_start_row,
#                                         "password_protected": password_protected,
#                                         "source_password": source_password,
#                                         "delimiter": delimiter,
#                                         "db_name": db_name,
#                                         "schema_name": schema_name,
#                                         "username": username,
#                                         "password": password
#                                     }
#                                 source_settings = SourceSettings.objects.filter(tenants_id=tenant_id,
#                                                                                     groups_id=group_id,
#                                                                                     entities_id=entity_id,
#                                                                                     setting_key='source_path')
#                                 sourcePath = ''
#                                 source_path=''
#                                 for setting in source_settings:
#                                     sourcePath = setting.setting_value
#
#                                 for k, v in sourcePath.items():
#                                     if k == "sourcePath":
#                                         source_path = v
#
#                                 source_name = source_name.replace(" ", "_")
#
#                                 # Creating Source Path with Source Name
#                                 source_path_module = source_path + source_name.replace(" ", "_")
#
#                                 if not os.path.exists(source_path_module):
#                                     os.mkdir(source_path_module)
#
#                                 # Creating Input Path
#                                 source_input_path = source_path_module + "/" + "input"
#
#                                 if not os.path.exists(source_input_path):
#                                     os.mkdir(source_input_path)
#
#                                 source_input_location = source_input_path + "/"
#                                 source_code = str(uuid.uuid4())
#                                 # print(source_config)
#                                 MasterSources.objects.create(
#                                     tenants_id=tenant_id,
#                                     groups_id=group_id,
#                                     entities_id=entity_id,
#                                     source_code=source_code,
#                                     source_name=source_name,
#                                     source_config=source_config,
#                                     source_input_location=source_input_location,
#                                     source_import_seq=import_sequence,
#                                     source_field_number=1,
#                                     is_active=1,
#                                     created_by=user_id,
#                                     created_date=timezone.now(),
#                                     modified_by=user_id,
#                                     modified_date=timezone.now()
#                                 )
#                                 #print("success  " +source_code)
#                                 master_sources = MasterSources.objects.filter(source_code=source_code)
#                                 for sources in master_sources:
#                                     m_sources_id = sources.id
#                                 if (audit_flag == "ON"):
#                                     # {
#                                     MasterSourcesAudit.objects.create(
#                                         operation_flag = "INSERT",
#                                         tenants_id=tenant_id,
#                                         groups_id=group_id,
#                                         entities_id=entity_id,
#                                         m_sources_id=m_sources_id,
#                                         source_code=source_code,
#                                         source_name=source_name,
#                                         source_config=source_config,
#                                         source_input_location=source_input_location,
#                                         source_field_number=1,
#                                         is_active=1,
#                                         created_by=user_id,
#                                         created_date=timezone.now()
#                                     )
#                                     # }
#
#                                 return JsonResponse({"Status": "Success", "Message": "Source Created Successfully!!!"})
#                         else:
#                             return JsonResponse({"Status" : "Error", "Message" : "User Id not Found!!!"})
#                     else:
#                         return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
#                 else:
#                     return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
#             else:
#                 return JsonResponse({"Status" : "Error", "Message" : "Tenant Id not Found!!!"})
#         else:
#             return JsonResponse({"Status" : "Error", "Message" : "POST Method Not Received!!!"})
#     except Exception:
#         logger.error("Error in Update Source!!!", exc_info=True)
#         return JsonResponse({"Status" : "Error"})

@csrf_exempt
def upload_files(request, *args, **kwargs):
    # print("upload file")
    try:
        if request.method == "POST":
            #body = request.body.decode('utf-8')
            #data = json.loads(body)
            body=""
            data=''

            #if requests is not None:
            #    body=request.POST.get("requests")
            #    #print("reee ",body)
            #    print(type(body))
            #    data = json.loads(body)
            #    print(type(data))

            source_id = ''
            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            source_input_location=''
            source_name=''

            source_id = request.POST.get("source_id")
            tenant_id = request.POST.get("tenant_id")
            group_id = request.POST.get("group_id")
            entity_id = request.POST.get("entity_id")
            user_id = request.POST.get("user_id")

            # print(request.FILES["m_source_file"].name)

            if len(str(source_id)) > 0:
                if len(str(tenant_id)) > 0:
                    if len(str(group_id)) > 0:
                        if len(str(entity_id)) > 0:
                            if len(str(user_id))> 0:
                                file_name = request.FILES["m_source_file"].name
                                file_name_extension = "." + file_name.split(".")[-1]

                                m_sources = MasterSources.objects.filter(id = source_id)
                                for source in m_sources:
                                    source_input_location = source.source_input_location
                                    source_name = source.source_name
                                    source_config = source.source_config
                                    # print(source_id)
                                    #file_upload_path_name_date = source_input_location + file_name_proper
                                file_upload_path_name_date=source_input_location + source_name+file_name_extension

                                with open(file_upload_path_name_date, 'wb+') as destination:
                                    for chunk in request.FILES["m_source_file"]:
                                        destination.write(chunk)

                                return JsonResponse({"Status" : "Success", "m_source_fileMessage" : "File Uploaded Successfully!!!"})
                            else:
                                return JsonResponse({"Status" : "Error", "Message" : "Users Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status" : "Error", "Message" : "Entities Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status" : "Error", "Message" : "Groups Id Not Found!!!"})
                else:
                    return JsonResponse({"Status" : "Error", "Message" : "Tenants Id Not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error", "Message" : "Source Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Storing Files!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

@csrf_exempt
def get_source_field_list(request, *args, **kwargs) :
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            source_id = ''
            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "source_id":
                    source_id = v
            # print(source_id)
            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            if int(source_id) > 0:
                                m_sources = MasterSources.objects.filter(id=source_id)
                                source_input_location=''
                                source_name=''
                                source_config=''
                                for source in m_sources:
                                    source_input_location = source.source_input_location
                                    source_name = source.source_name
                                    source_config = source.source_config

                                file_headers = get_field_list(
                                    source_input_location = source_input_location,
                                    source_name = source_name,
                                    source_config = source_config
                                )

                                if file_headers is not None:
                                    status = file_headers["Status"]

                                    if status == "Error":
                                        return JsonResponse({"Status": "Error", "Message": "Error in Getting Header Details!!!"})
                                    elif status == "Success":
                                        data_header = file_headers["data_header"]
                                        length=len(data_header)
                                        attribute_list=[]
                                        attribute_lists=[]
                                        for i in range(0,length):
                                            attribute_list = {
                                                             "attribute_name":data_header[i],
                                                             "attribute_position":0,
                                                             "attribute_date_format":"",
                                                             "validate_field":"",
                                                             "required_field":"",
                                                             "unique_field":"",
                                                             "editable_field":""}
                                            attribute_lists.append(attribute_list)
                                        return JsonResponse({"Status" : "Success", "data_header" : attribute_lists})
                                elif file_headers is None:
                                    return JsonResponse({"Status": "Error", "Message": "Header Details Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "Source Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenants Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Update Source!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

def get_field_list(source_input_location, source_name, source_config):
    try:
        post_url = "http://localhost:50002/etl/reading_file/"
        payload = json.dumps({"source_input_location" : source_input_location, "source_name" : source_name,  "source_config" : source_config})
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.get(post_url, data=payload, headers=headers)
        if response.content:
            # print(response.content)
            content_data = json.loads(response.content)
            status = content_data["Status"]

            if status == "Error":
                return {"Status" : "Error", "Message" : "Error in Reading File!!!"}
            elif status == "Success":
                return {"Status" : "Success", "data_header" : content_data["data_header"]}
        else:
            return None
    except Exception:
        logger.error("Error in Validating File!!!", exc_info=True)
        return {"Status" : "Def Error"}



@csrf_exempt
def get_field_list_details(request, *args, **kwargs) :
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            source_id = ''
            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            source_list = ''
            validate_field=''
            required_field=''
            unique_field=''
            editable_field=''
            attribute_position=''
            attribute_data_type = ''
            attribute_date_format = ''
            is_validate = ''
            is_required = ''
            is_unique = ''
            is_editable = ''


            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "source_id":
                    source_id = v
                if k == "source_attributes":
                    source_list = v

            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            if int(source_id) > 0:
                                if len(source_list) > 0:
                                    # print(source_list)
                                    for source in source_list:
                                        for k, v in source.items():
                                            if k == "source_definition_id":
                                                source_definition_id = v
                                            if k == "attribute_name":
                                                attribute_name = v
                                            if k == "attribute_position":
                                                attribute_position = v
                                            if k == "attribute_data_type":
                                                attribute_data_type = v
                                            if k == "attribute_date_format":
                                                attribute_date_format = v
                                            if k == "validate_field":
                                                validate_field = v
                                            if k == "required_field":
                                                required_field = v
                                            if k == "unique_field":
                                                unique_field = v
                                            if k == "editable_field":
                                                editable_field = v

                                        # print(attribute_position)

                                        if validate_field == "Yes":
                                            is_validate = 1
                                        elif validate_field == "No":
                                            is_validate = 0

                                        if required_field == "Yes":
                                            is_required = 1
                                        elif required_field == "No":
                                            is_required = 0

                                        if unique_field == "Yes":
                                            is_unique = 1
                                        elif unique_field == "No":
                                            is_unique = 0

                                        if editable_field == "Yes":
                                            is_editable = 1
                                        elif editable_field == "No":
                                            is_editable = 0

                                        # print(is_validate)
                                        # print(is_required)
                                        # print(is_unique)
                                        # print(is_editable)
                                        m_sources = MasterSources.objects.filter(id=source_id)

                                        for source in m_sources:
                                            source_code=source.source_code
                                            source_field_number = source.source_field_number

                                        attribute_reference_field = "reference_field_" + str(source_field_number)

                                        for source in m_sources:
                                            source.source_field_number = int(source_field_number) + 1
                                            source.save()

                                        master_source_definitions = MasterSourceDefinitions.objects.filter(
                                            tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id,
                                            attribute_name=attribute_name, m_sources_id=source_id)
                                        # print(attribute_name)
                                        if not master_source_definitions:
                                            # print(attribute_reference_field)
                                            MasterSourceDefinitions.objects.create(
                                                tenants_id=tenant_id,
                                                groups_id=group_id,
                                                entities_id=entity_id,
                                                attribute_name=attribute_name,
                                                attribute_position=attribute_position,
                                                attribute_data_type=attribute_data_type,
                                                attribute_date_format=attribute_date_format,
                                                attribute_reference_field=attribute_reference_field,
                                                is_validate=is_validate,
                                                is_required=is_required,
                                                is_unique=is_unique,
                                                is_editable=is_editable,
                                                is_active=1,
                                                created_by=user_id,
                                                created_date=timezone.now(),
                                                modified_by=user_id,
                                                modified_date=timezone.now(),
                                                m_sources_id=source_id
                                            )

                                        # Create Table from Source Configurations
                                    source_configurations = MasterSourceDefinitions.objects.filter(tenants_id=tenant_id,
                                                                                                   groups_id=group_id,
                                                                                                   entities_id=entity_id,
                                                                                                   m_sources=source_id,
                                                                                                   is_active=1).order_by(
                                        'attribute_position')

                                    # Get the attribute names in the sequence
                                    attributes_list = []
                                    #file_reference_field_list = []
                                    for config in source_configurations:
                                        #attribute_list = {
                                        #   "attribute_reference_field": config.attribute_reference_field
                                        #}
                                        #file_reference_field_list.append(config.file_reference_field)
                                        attributes_list.append(config.attribute_reference_field)

                                    # print(attributes_list)

                                    # Get the defined table query from Module Settings
                                    source_settings = SourceSettings.objects.filter(tenants_id=tenant_id,
                                                                                    groups_id=group_id,
                                                                                    entities_id=entity_id,
                                                                                    is_active=1,
                                                                                    setting_key='source_table_query')
                                    table_create_query=''
                                    for setting in source_settings:
                                        table_create_query = setting.setting_value
                                    #     print(table_create_query)
                                    # print(type(table_create_query))
                                    # table_create_query=json.loads(table_create_query)

                                    for k, v in table_create_query.items():
                                        if(k == "create_table") :
                                            table_create_query= v
                                            # print(table_create_query)

                                    create_table = get_create_table(
                                        attributes_list=attributes_list,
                                        table_query=table_create_query,
                                        table_name=source_code
                                    )

                                    if create_table:
                                        source_settings = SourceSettings.objects.filter(tenants_id=tenant_id,
                                                                                            groups_id=group_id,
                                                                                            entities_id=entity_id,
                                                                                            is_active=1,
                                                                                            setting_key='source_table_insert_query')
                                        for setting in source_settings:
                                            insertQuery = setting.setting_value

                                        # print(type(insertQuery))

                                        #insertQuery = json.loads(insertQuery)

                                        for k, v in insertQuery.items():
                                            if (k == "insert_table") :
                                                insertQuery = v

                                        master_source_definitions = MasterSourceDefinitions.objects.filter(
                                            tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id,
                                            m_sources_id=source_id).order_by(
                                            'attribute_position')

                                        attribute_reference_field_list = []
                                        for source in master_source_definitions:
                                            attribute_reference_field_list.append(source.attribute_reference_field)

                                        insert_query_output = get_insert_query(
                                            attribute_field_list=attribute_reference_field_list,
                                            insert_query=insertQuery,
                                            table_name=source_code
                                            )

                                        if insert_query_output:

                                            source_queries = SourceQueries.objects.filter(tenants_id=tenant_id,
                                                                                              groups_id=group_id,
                                                                                              entities_id=entity_id,
                                                                                              m_sources_id=source_id,
                                                                                              source_key='insertQuerySrc')
                                            if not source_queries:
                                                SourceQueries.objects.create(
                                                    tenants_id=tenant_id,
                                                    groups_id=group_id,
                                                    entities_id=entity_id,
                                                    source_key='insertQuerySrc',
                                                    source_value=insert_query_output,
                                                    description="Insert Query For Source - {source_name}",
                                                    is_active=1,
                                                    created_by=user_id,
                                                    created_date=timezone.now(),
                                                    modified_by=1,
                                                    modified_date=timezone.now(),
                                                    m_sources_id=source_id
                                                    )
                                                return JsonResponse({"Status": "Success",
                                                                     "Message": "Source Definitions Updated Successfully!!!"})
                                            else:
                                                return JsonResponse({"Status": "Error",
                                                                     "Message": "Error in Creating Insert Query From Source Definitions!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error",
                                                                 "Message": "Error in Creating Insert Query From Source Definitions!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error",
                                                             "Message": "Error in Creating Tables From Source Definitions!!!"})

                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Source List Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "Source Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenants Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Validating File!!!", exc_info=True)
        return {"Status" : "Def Error"}

def get_create_table(attributes_list, table_query, table_name):
    """
    Create a Table from the attribute names
    """
    try:
        field_string = ''
        for attributes in attributes_list:
            #for k,v in attributes.items():
            #    if k == "attribute_reference_field":
            #       attribute_reference_field = v

            field_string = field_string + "`" + attributes+ "`"+ " VARCHAR(64) DEFAULT NULL, "
            final_query = table_query.replace('{required_fields}', field_string).replace('{table_name}', table_name)
        # print("final_query : "+final_query)
        # Executing Query to Create Table
        query_output = execute_sql_query(final_query, object_type="create")
        if query_output is None:
            return True
        else:
            return False
    except Exception:
        logger.error("Error in Creating Table from attribute list!!!", exc_info=True)
        return False


def get_insert_query(attribute_field_list, insert_query, table_name):
    """
    Creates a Insert Query from the Table name and the table column names
    """
    try:
        field_query = ''
        for i in range(0, len(attribute_field_list)):
            if i != len(attribute_field_list) - 1:
                field_query = field_query + attribute_field_list[i] + ", "
            elif i == len(attribute_field_list) - 1:
                field_query = field_query + attribute_field_list[i]

        # Creating Query Output
        final_query = insert_query.replace("{source_attributes}", field_query).replace("{reference_table}", table_name)
        return final_query
    except Exception:
        logger.error("Error in Creating Inert Query!!!", exc_info=True)
        return False

def execute_sql_query(query, object_type):
    try:
        with connection.cursor() as cursor:
            logger.info("Executing SQL Query..")
            logger.info(query)

            cursor.execute(query)
            if object_type == "table":
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers":column_names, "data":rows}
                output = json.dumps(table_output)
                return output
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



# Aggregators

@csrf_exempt
def get_aggregator_details(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)
            # print(data)
            source_id = ''
            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            aggregator_name= ''
            aggregator_list=''
            m_source_id=''
            m_aggregator_id=''
            is_base=''

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "source_id":
                    source_id = v
                if k == "aggregator_name":
                    aggregator_name = v
                if k == "aggregator_list":
                    aggregator_list = v
            # print(type(aggregator_list))
            #aggregator_list = json.du(aggregator_list)
            #print(aggregator_list)

            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            #if int(source_id) > 0:
                                if len(aggregator_list) > 0:
                                    aggregator_code = str(uuid.uuid4())
                                    # print(aggregator_name)
                                    MasterAggregators.objects.create(
                                        tenants_id=tenant_id,
                                        groups_id=group_id,
                                        entities_id=entity_id,
                                        aggregator_code=aggregator_code,
                                        aggregator_name=aggregator_name,
                                        is_active=1,
                                        created_by=user_id,
                                        created_date=timezone.now(),
                                        modified_by=user_id,
                                        modified_date=timezone.now()
                                        )
                                    m_aggregator=MasterAggregators.objects.filter(aggregator_code=aggregator_code)
                                    for aggregator in m_aggregator:
                                        m_aggregator_id = aggregator.id
                                    # print(aggregator_list)
                                    for aggregator_source in aggregator_list:
                                        aggregator_source=json.dumps(aggregator_source)
                                        # print(type(aggregator_source))
                                        agg_source = json.loads(aggregator_source)
                                        # print(type(agg_source))
                                        # print(agg_source)
                                        for k, v in agg_source.items():
                                            if k == "source_id":
                                                m_source_id = v
                                            if k == "base_file":
                                                is_base = v
                                                # print(is_base)
                                                if is_base == True:
                                                    is_base = 1
                                                else :
                                                    is_base = 0
                                        if(is_base == 1) :
                                            source_id = m_source_id
                                        MasterAggregatorsDetails.objects.create(
                                            tenants_id=tenant_id,
                                            groups_id=group_id,
                                            entities_id=entity_id,
                                            m_sources_id=m_source_id,
                                            m_aggregator_id=m_aggregator_id,
                                            is_base_source=is_base,
                                            is_active=1,
                                            created_by=user_id,
                                            created_date=timezone.now(),
                                            modified_by=1,
                                            modified_date=timezone.now()
                                        )

                                    source_configurations = MasterSourceDefinitions.objects.filter(tenants_id=tenant_id,
                                                                                                   groups_id=group_id,
                                                                                                   entities_id=entity_id,
                                                                                                   m_sources=source_id,
                                                                                                   is_active=1).order_by(
                                        'attribute_position')

                                    # Get the attribute names in the sequence
                                    attributes_list = []
                                    for config in source_configurations:
                                        attributes_list.append(config.attribute_reference_field+"-"+config.attribute_data_type)

                                    # print(attributes_list)

                                    # Get the defined table query from Module Settings
                                    source_settings = SourceSettings.objects.filter(tenants_id=tenant_id,
                                                                                    groups_id=group_id,
                                                                                    entities_id=entity_id,
                                                                                    is_active=1,
                                                                                    setting_key='aggregate_table_query')
                                    table_create_query=''
                                    for setting in source_settings:
                                        table_create_query = setting.setting_value
                                    #     print(table_create_query)
                                    # print(type(table_create_query))
                                    # table_create_query=json.loads(table_create_query)

                                    for k, v in table_create_query.items():
                                        if(k == "create_table") :
                                            table_create_query= v
                                            # print(table_create_query)

                                    create_table = get_create_table_agg(
                                        attributes_list=attributes_list,
                                        table_query=table_create_query,
                                        table_name=aggregator_code
                                    )

                                    if create_table:
                                        source_settings = SourceSettings.objects.filter(tenants_id=tenant_id,
                                                                                            groups_id=group_id,
                                                                                            entities_id=entity_id,
                                                                                            is_active=1,
                                                                                            setting_key='aggregate_table_insert_query')
                                        for setting in source_settings:
                                            insertQuery = setting.setting_value

                                        # print(type(insertQuery))

                                        #insertQuery = json.loads(insertQuery)

                                        for k, v in insertQuery.items():
                                            if (k == "insert_table") :
                                                insertQuery = v

                                        master_source_definitions = MasterSourceDefinitions.objects.filter(
                                            tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id,
                                            m_sources_id=source_id).order_by(
                                            'attribute_position')

                                        attribute_reference_field_list = []
                                        for source in master_source_definitions:
                                            attribute_reference_field_list.append(source.attribute_reference_field)

                                        insert_query_output = get_insert_query_agg(
                                            attribute_field_list=attribute_reference_field_list,
                                            insert_query=insertQuery,
                                            table_name=aggregator_code
                                            )
                                        # print(insert_query_output)
                                        if insert_query_output:

                                            aggregator_queries = AggregatorsQueries.objects.filter(tenants_id=tenant_id,
                                                                                              groups_id=group_id,
                                                                                              entities_id=entity_id,
                                                                                              m_aggregator_id=m_aggregator_id,
                                                                                              key='insertQueryAgg')
                                            if not aggregator_queries:
                                                AggregatorsQueries.objects.create(
                                                    tenants_id=tenant_id,
                                                    groups_id=group_id,
                                                    entities_id=entity_id,
                                                    m_aggregator_id=m_aggregator_id,
                                                    key="insertQueryAgg",
                                                    value=insert_query_output,
                                                    description="Insert Query For Aggregator - {aggregator_name}",
                                                    is_active=1,
                                                    created_by=user_id,
                                                    created_date=timezone.now(),
                                                    modified_by=1,
                                                    modified_date=timezone.now()
                                                    )


                                    return JsonResponse({"Status": "Success", "Message": "Aggregator Created Successfully!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Aggregator Source List not Found!!!"})
                            #else:
                            #    return JsonResponse({"Status": "Error", "Message": "Source Id not Found!!!"})
                        else:
                            return JsonResponse({"Status" : "Error", "Message" : "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error", "Message" : "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Update Source!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})


def get_create_table_agg(attributes_list, table_query, table_name):
    """
    Create a Table from the attribute names
    """
    try:
        field_string = ''
        for attribute in attributes_list:
            attributes=attribute.split("-")
            if(attributes[1]=="Date"):
                attributes[1] = "DATE"
            elif (attributes[1] == "Datetime"):
                attributes[1] = "DATETIME"
            elif (attributes[1] == "Number"):
                attributes[1] = "INT(11)"
            elif (attributes[1] == "Char"):
                attributes[1] = "VARCHAR(64)"
            elif (attributes[1] == "Text"):
                attributes[1] = "TEXT"
            elif (attributes[1] == "Decimal"):
                attributes[1] = "DOUBLE"
            else :
                attributes[1] = "VARCHAR(64)"
            field_string = field_string + "`" + attributes[0]+ "`  "+attributes[1] +" , "
            final_query = table_query.replace('{required_fields}', field_string).replace('{table_name}', table_name)
        # print("final_query : "+final_query)
        # Executing Query to Create Table
        query_output = execute_sql_query(final_query, object_type="create")
        if query_output is None:
            return True
        else:
            return False
    except Exception:
        logger.error("Error in Creating Table from attribute list!!!", exc_info=True)
        return False


def get_insert_query_agg(attribute_field_list, insert_query, table_name):
    """
    Creates a Insert Query from the Table name and the table column names
    """
    try:
        field_query = ''
        for i in range(0, len(attribute_field_list)):
            if i != len(attribute_field_list) - 1:
                field_query = field_query + attribute_field_list[i] + ", "
            elif i == len(attribute_field_list) - 1:
                field_query = field_query + attribute_field_list[i]

        # Creating Query Output
        final_query = insert_query.replace("{aggregator_attributes}", field_query).replace("{reference_table}", table_name)
        return final_query
    except Exception:
        logger.error("Error in Creating Inert Query!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})



@csrf_exempt
def get_source_list_details(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id = 0
            aggregator_id=0
            operation_flag=''

            delimiter=''
            sheet_name=''
            source_type=''
            source_password=''
            column_start_row=''
            source_extension=''
            password_protected=''

            # print(type(data))

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "operation_flag":
                    operation_flag = v
                if k == "aggregator_id" :
                    aggregator_id= v

            if(tenant_id > 0):
                if(group_id > 0):
                    #print(group_id)
                    if(entity_id > 0):
                        m_sources = []
                        m_aggregators = []
                        if(operation_flag == "SELECT") :
                            m_sources_list=MasterSources.objects.filter(tenants_id = tenant_id,
                                                                        groups_id = group_id,
                                                                        entities_id = entity_id
                                                                        )
                            for sources in m_sources_list:
                                for k, v in sources.source_config.items():
                                    # print("type of config",type(sources.source_config))
                                    if k == "delimiter":
                                        delimiter = v
                                    if (k == "sheet_name"):
                                        sheet_name = v
                                    if (k == "source_type"):
                                        source_type = v
                                    if (k == "source_password"):
                                        source_password = v
                                    if (k == "column_start_row"):
                                        column_start_row = v
                                    if (k == "source_extension"):
                                        source_extension = v
                                    if (k == "password_protected"):
                                        password_protected = v

                                m_sources.append({
                                    "source_id": sources.id,
                                    "tenant_id": sources.tenants_id,
                                    "group_id": sources.groups_id,
                                    "entity_id": sources.entities_id,
                                    "source_code": sources.source_code,
                                    "source_name": sources.source_name,
                                    "delimiter": delimiter,
                                    "sheet_name": sheet_name,
                                    "source_type": source_type,
                                    "source_password": source_password,
                                    "column_start_row": column_start_row,
                                    "source_extension": source_extension,
                                    "password_protected": password_protected,
                                    "source_input_location": sources.source_input_location,
                                    "source_import_seq": sources.source_import_seq
                                })
                        elif (operation_flag == "SD"):
                            # print("IN SND")
                            m_sources_list = MasterSources.objects.filter(Exists(
                                MasterSourceDefinitions.objects.filter(m_sources=OuterRef('pk'))),
                                                                          tenants_id=tenant_id,
                                                                          groups_id=group_id,
                                                                          entities_id=entity_id
                                                                          )

                            for sources in m_sources_list:
                                m_sources.append({
                                    "source_id": sources.id,
                                    "source_name": sources.source_name
                                })
                        elif(operation_flag == "SND") :
                            #print("IN SND")
                            m_sources_list = MasterSources.objects.filter(~Exists(
                                                                          MasterSourceDefinitions.objects.filter(m_sources=OuterRef('pk'))),
                                                                          tenants_id=tenant_id,
                                                                          groups_id=group_id,
                                                                          entities_id=entity_id
                                                                          )

                            for sources in m_sources_list:
                                m_sources.append({
                                    "source_id": sources.id,
                                    "source_name": sources.source_name
                                })

                        elif (operation_flag == "ANDSD"):
                            m_sources_list = MasterSourceDefinitions.objects.filter(~Exists(
                                MasterAggregatorsDetails.objects.filter(m_sources=OuterRef('pk'))),
                                                                          tenants_id=tenant_id,
                                                                          groups_id=group_id,
                                                                          entities_id=entity_id
                                                                          )

                            for sources in m_sources_list:
                                m_sources.append({
                                    "source_id": sources.id,
                                    "source_name": sources.m_sources.source_name
                                })

                        elif (operation_flag == "ADSD"):
                            m_sources_list = MasterSourceDefinitions.objects.filter(Exists(
                                MasterAggregatorsDetails.objects.filter(m_sources=OuterRef('pk')),m_aggregator=aggregator_id),
                                                                          tenants_id=tenant_id,
                                                                          groups_id=group_id,
                                                                          entities_id=entity_id,                                                                          )

                            for sources in m_sources_list:
                                m_sources.append({
                                    "source_id": sources.id,
                                    "source_name": sources.m_sources.source_name
                                })

                        return JsonResponse({"Status": "Success", "sources_list": m_sources})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error","Message" : "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})


@csrf_exempt
def get_aggregators_list_details(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id = 0
            operation_flag=''

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "operation_flag":
                    operation_flag = v

            if(tenant_id > 0):
                if(group_id > 0):
                    if(entity_id > 0):
                        m_aggregators = []
                        if (operation_flag == "AD"):
                            m_aggregator_list = MasterAggregators.objects.filter(
                                                                          tenants_id=tenant_id,
                                                                          groups_id=group_id,
                                                                          entities_id=entity_id
                                                                          )
                            for aggregators in m_aggregator_list:
                                m_aggregators.append({
                                    "aggregator_id": aggregators.id,
                                    "aggregator_name": aggregators.aggregator_name
                                })

                        return JsonResponse({"Status": "Success", "Data": m_aggregators})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error","Message" : "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})


@csrf_exempt
def get_source_def_list_details(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            source_id = 0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "source_id":
                    source_id = v

            validate_field=''
            required_field=''
            unique_field=''
            editable_field=''

            if(int(tenant_id) > 0):
                if(int(group_id) > 0):
                    if(int(entity_id) > 0):
                        if (int(source_id) > 0):
                            m_sources = []
                            m_sources_definition_list=MasterSourceDefinitions.objects.filter(tenants_id = tenant_id,
                                                                            groups_id = group_id,
                                                                            entities_id = entity_id,
                                                                            m_sources=source_id)
                            for sources in m_sources_definition_list:
                                if sources.is_validate == 1:
                                    validate_field = "Yes"
                                elif sources.is_validate == 0:
                                    validate_field = "No"

                                if sources.is_required == 1:
                                    required_field = "Yes"
                                elif sources.is_required == 0:
                                    required_field= "No"

                                if sources.is_unique  == 1:
                                    unique_field = "Yes"
                                elif sources.is_unique == 0:
                                    unique_field = "No"

                                if sources.is_editable == 1:
                                    editable_field = "Yes"
                                elif sources.is_editable == 0:
                                    editable_field = "No"
                                m_sources.append({
                                    "tenants_id":sources.tenants_id,
                                    "groups_id":sources.groups_id,
                                    "entities_id":sources.entities_id,
                                    "attribute_name":sources.attribute_name,
                                    "attribute_position":sources.attribute_position,
                                    "attribute_data_type":sources.attribute_data_type,
                                    "attribute_date_format":sources.attribute_date_format,
                                    "attribute_reference_field":sources.attribute_reference_field,
                                    "validate_field":validate_field,
                                    "required_field":required_field,
                                    "unique_field":unique_field,
                                    "editable_field":editable_field,
                                    "sources_id":source_id
                                    })

                            return JsonResponse({"Status": "Success", "sources_list": m_sources})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "Source Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error","Message" : "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_aggregator_list_details(request, *args, **kwargs):
    try:
        if request.method == "POST":

            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0
            operation_flag = 0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "operation_flag":
                    operation_flag = v



            if(int(tenant_id) > 0):
                if(int(group_id) > 0):
                    if(int(entity_id) > 0):
                        if (int(user_id) > 0):
                            if (operation_flag == "SELECT"):
                                m_aggregators = []
                                aggregators_list = []
                                m_aggregators_list=MasterAggregatorsDetails.objects.all()
                                base_file=''
                                for aggregator in m_aggregators_list:
                                    if aggregator.is_base_source == 1:
                                        base_file = True
                                    m_aggregators.append({
                                        "source_id":aggregator.m_sources.id,
                                        "source_name": aggregator.m_sources.source_name,
                                        "base_file":base_file
                                        })
                                    aggregators_list.append({
                                        "tenants_id": aggregator.tenants_id,
                                        "groups_id": aggregator.groups_id,
                                        "entities_id": aggregator.entities_id,
                                        "aggregator_name": aggregator.m_aggregator.aggregator_name,
                                        "aggregator_list":m_aggregators
                                    })

                                return JsonResponse({"Status": "Success", "aggregator_list": aggregators_list})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "Operation Flag not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error","Message" : "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})


@csrf_exempt
def get_transformation_source(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v

            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_transformations=[]
                            m_transformations_list=MasterTransformations.objects.all()
                            for transformations in m_transformations_list :
                                m_transformations.append({
                                    "transformation_id" : transformations.id,
                                    "transformation_name" : transformations.transformation_name
                                })
                            return JsonResponse({"Status":"Success","Data":m_transformations})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_transformation_page(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0
            transformation_id=0
            source_id=0
            aggregator_id=0
            trans_name=''

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "transformation_id":
                    transformation_id = v
                if k == "source_id":
                    source_id = v
                if k == "aggregator_id":
                    aggregator_id = v


            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            if (int(aggregator_id)) > 0:
                                if (int(source_id)) > 0:
                                    if (int(transformation_id)) > 0:
                                        transformation=MasterTransformations.objects.filter(id=transformation_id)
                                        for transformations in transformation :
                                            trans_name=transformations.transformation_name
                                        if trans_name=="Field Extraction" :
                                            m_source_definition_list=MasterSourceDefinitions.objects.filter(m_sources=source_id)
                                            m_transformation_operators=MasterTransformationsOperators.objects.all()

                                            field_extraction_page_list=[]
                                            m_source_def_list =[]
                                            m_trans_operators = []
                                            operators_list=[]
                                            for m_source_def in m_source_definition_list:
                                                m_source_def_list.append(
                                                    {"source_definition_id": m_source_def.id,
							                         "attribute_name": m_source_def.attribute_name}
                                                )
                                            field_extraction_page_list.append(
                                                {"attributes_names" : m_source_def_list}
                                            )
                                            opt_kv_pair=[]
                                            trans_opt=[]
                                            for m_trans_opt in m_transformation_operators:
                                                for trans_opt in m_trans_opt.operator_value:
                                                    trans_opt=json.dumps(trans_opt)
                                                    trans_opt = json.loads(trans_opt)
                                                    for k,v in trans_opt.items():
                                                        operators_list.append(k)
                                                field_extraction_page_list.append(
                                                    {
                                                        m_trans_opt.operator_key:operators_list
                                                    })
                                                operators_list=[]
                                            return JsonResponse({"Status": "Success", "Data": field_extraction_page_list})
                                        elif trans_name=="Lookup Extraction" :
                                            sub_aggregator_list=MasterAggregatorsDetails.objects.filter(is_base_source=0)
                                            m_source_definition_list = MasterSourceDefinitions.objects.all(
                                                m_sources=source_id)

                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "Transformation Id not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Source Id not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "Aggregator Id not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})


#Transformations

@csrf_exempt
def get_transformations_fields(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            transformation_name = ''
            m_transformations_id = ''


            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "m_transformations_id":
                    m_transformations_id = v
                if k == "transformation_name":
                    transformation_name = v


            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:

                            TransformationsFields.objects.create(
                                tenants_id=tenant_id,
                                groups_id=group_id,
                                entities_id=entity_id,
                                m_transformations_id=m_transformations_id,
                                transformation_name=transformation_name,
                                is_active=1,
                                created_by=user_id,
                                created_date=timezone.now(),
                                modified_by=user_id,
                                modified_date=timezone.now()
                            )
                            return JsonResponse({"Status": "Success", "Message": "Transformation Successful!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})


    except Exception:
        logger.error("Error in Update Transformations!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})


@csrf_exempt
def get_transformations_field_extraction(request, *args, **kwargs):
    # print("inpost")
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            m_transformation_id = ''
            m_source_definition_id=''
            m_source_id=''
            m_aggregator_id=''
            trans_field_extract_list=''
            transformation_name=''
            attribute_name=''
            transaction_reference=''
            transaction_placed=''
            pattern_input=''
            pattern_type=''

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "m_transformation_id":
                    m_transformation_id = v
                if k == "transformation_name":
                    transformation_name = v
                if k == "m_aggregator_id":
                    m_aggregator_id = v
                if k == "m_source_definition_id":
                    m_source_definition_id = v
                if k == "trans_field_extract_list":
                    trans_field_extract_list = v


            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            TransformationsFields.objects.create(
                                tenants_id=tenant_id,
                                groups_id=group_id,
                                entities_id=entity_id,
                                m_transformations_id=m_transformation_id,
                                transformation_name=transformation_name,
                                is_active=1,
                                created_by=user_id,
                                created_date=timezone.now(),
                                modified_by=user_id,
                                modified_date=timezone.now()
                            )

                            m_sources_list = MasterSourceDefinitions.objects.filter(id=m_source_definition_id)
                            for sources in m_sources_list:
                                    m_source_id=sources.id
                            # print(m_source_id)
                            for translist in trans_field_extract_list:
                                for k, v in translist.items():
                                    if k == "attribute_name":
                                        attribute_name = v
                                    if k == "pattern_type":
                                        pattern_type = v
                                    if k == "pattern_input":
                                        pattern_input = v
                                    if k == "transaction_placed":
                                        transaction_placed = v
                                    if k == "transaction_reference":
                                        transaction_reference = v

                                FieldExtraction.objects.create(
                                    tenants_id=tenant_id,
                                    groups_id=group_id,
                                    entities_id=entity_id,
                                    transformation_fields_id=m_transformation_id,
                                    m_aggregators_id=m_aggregator_id,
                                    m_source_definition_id=m_source_definition_id,
                                    m_sources_id=m_source_id,
                                    attribute_name=attribute_name,
                                    pattern_type=pattern_type,
                                    pattern_input=pattern_input,
                                    transaction_placed=transaction_placed,
                                    transaction_reference=transaction_reference,
                                    is_active=1,
                                    created_by=user_id,
                                    created_date=timezone.now(),
                                    modified_by=user_id,
                                    modified_date=timezone.now()
                                    )

                            return JsonResponse({"Status": "Success", "Message": "Transformation Successful!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})


    except Exception:
        logger.error("Error in Update Transformations!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

@csrf_exempt
def get_processing_layers(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v

            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_processing_layers=[]

                            m_processing_list=MasterProcessingLayer.objects.all()
                            for processlayers in m_processing_list :
                                m_processing_layers.append({
                                    "processing_id" : processlayers.id,
                                    "processing_name" : processlayers.processing_layer_name
                                })

                            return JsonResponse({"Status":"Success","Data":m_processing_layers})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_processing_sub_layers(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v

            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_processing_sub_layers=[]
                            m_processing_sub_list = MasterProcessingSubLayer.objects.filter(m_processing_layer=processing_layer_id)
                            for processsublayers in m_processing_sub_list:
                                m_processing_sub_layers.append({
                                    "sub_processing_id": processsublayers.id,
                                    "sub_processing_name": processsublayers.sub_layer_name
                                })
                            return JsonResponse({"Status":"Success","Data":m_processing_sub_layers})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_processing_layer(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            processing_layer_id = ''
            name=''
            m_processing_sub_layer_id=''
            side=''
            side_name=''
            m_aggregator_id=''
            processing_layer_list=''

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "name":
                    name = v
                if k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_list":
                    processing_layer_list = v



            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            ProcessingLayer.objects.create(
                                tenants_id=tenant_id,
                                groups_id=group_id,
                                entities_id=entity_id,
                                m_processing_layer_id=processing_layer_id,
                                m_processing_sub_layer_id=m_processing_sub_layer_id,
                                name=name,
                                is_active=1,
                                created_by=user_id,
                                created_date=timezone.now(),
                                modified_by=user_id,
                                modified_date=timezone.now()
                            )
                            # print(type(processing_layer_list))
                            # print(processing_layer_list)
                            processing_layer_list = json.dumps(processing_layer_list)
                            processing_layer_list = json.loads(processing_layer_list)
                            # print(type(processing_layer_list))
                            # print(processing_layer_list)
                            for processing_list in processing_layer_list:
                                # print(type(processinglayerlist))
                                # print(processinglayerlist)
                                # processing_layerlist= json.dumps(processinglayerlist)
                                # processing_layerlist = json.loads(processing_layerlist)
                                # print(type(processing_list))
                                # print(processing_list)
                                for k, v in processing_list.items():
                                    if k == 'side':
                                        side = v
                                    if k == "aggregator_id":
                                        m_aggregator_id = v
                                    if k == "side_name":
                                        side_name = v

                                ProcessingLayerDefinition.objects.create(
                                    tenants_id=tenant_id,
                                    groups_id=group_id,
                                    entities_id=entity_id,
                                    processing_layer_id=processing_layer_id,
                                    m_aggregators_id=m_aggregator_id,
                                    side=side,
                                    side_name=side_name,
                                    is_active=1,
                                    created_by=user_id,
                                    created_date=timezone.now(),
                                    modified_by=user_id,
                                    modified_date=timezone.now()
                                )

                            return JsonResponse({"Status": "Success", "Message": "Processing Layer Updated Successfully!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})


    except Exception:
        logger.error("Error in Update Source!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})



@csrf_exempt
def get_business_layer_source(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v

            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_processing_layers_list=[]
                            m_rule_categories_list=[]
                            m_rule_types_list=[]
                            m_business_layer_source_list=[]

                            m_processing_list=ProcessingLayer.objects.all()
                            for processlayers in m_processing_list :
                                m_processing_layers_list.append({
                                    "processing_id" : processlayers.id,
                                    "processing_name" : processlayers.name
                                })
                            m_rule_category_list = MasterRuleCategories.objects.all()
                            for rulescategory in m_rule_category_list:
                                m_rule_categories_list.append({
                                    "rule_category_id": rulescategory.id,
                                    "rule_category_name": rulescategory.rule_category
                                })

                            m_rule_type_list = MasterRuleType.objects.all()
                            for ruletypes in m_rule_type_list:
                                m_rule_types_list.append({
                                    "rule_type_id": ruletypes.id,
                                    "rule_type_name": ruletypes.rule_type
                                })

                            m_business_layer_source_list.append({
                                "process_lay_def_list": m_processing_layers_list,
                                "rule_categories_list": m_rule_categories_list,
                                "rule_type_list": m_rule_types_list
                            })


                            return JsonResponse({"Status":"Success","Data":m_business_layer_source_list})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})


@csrf_exempt
def get_update_business_layer(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            rule_name = ''
            rule_description=''
            rule_type_id=''
            match_type=''
            execution_sub_sequence=''
            processing_layer_id=''
            processing_layer_name=''
            rule_set_name=''
            m_rule_categories_id=''
            rule_category_name=''
            execution_sequence=''
            business_layer_list=''
            # print(data)
            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "rule_set_name":
                    rule_set_name = v
                if k == "m_rule_categories_id":
                    m_rule_categories_id = v
                if k == "execution_sequence":
                    execution_sequence = v
                if k == "rules_list":
                    business_layer_list = v

            # print("busines layer list ",business_layer_list)

            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            for businesslayerlist in business_layer_list:
                                for k, v in businesslayerlist.items():
                                    if k == "rule_name":
                                        rule_name = v
                                    if k == "rule_description":
                                        rule_description = v
                                    if k == "rule_type_id":
                                        rule_type_id = v
                                    if k == "match_type":
                                        match_type = v
                                    if k == "sub_sequence":
                                        execution_sub_sequence = v

                                processing_layers=ProcessingLayer.objects.filter(id=processing_layer_id)
                                for processing_layer in processing_layers:
                                    processing_layer_name=processing_layer.name
                                m_rule_category= MasterRuleCategories.objects.filter(id=m_rule_categories_id)
                                for rulescategory in m_rule_category:
                                        rule_category_name =  rulescategory.rule_category
                                # print(rule_category_name)
                                BusinessLayer.objects.create(
                                    tenants_id=tenant_id,
                                    groups_id=group_id,
                                    entities_id=entity_id,
                                    processing_layer_id=processing_layer_id,
                                    processing_layer_name=processing_layer_name,
                                    execution_sequence=execution_sequence,
                                    rule_set_name=rule_set_name,
                                    m_rule_categories_id=m_rule_categories_id,
                                    rule_name=rule_name,
                                    rule_description=rule_description,
                                    rule_type_id=rule_type_id,
                                    match_type=match_type,
                                    execution_sub_sequence=execution_sub_sequence,
                                    is_active=1,
                                    created_by=user_id,
                                    created_date=timezone.now(),
                                    modified_by=user_id,
                                    modified_date=timezone.now()
                                    )
                            return JsonResponse({"Status": "Success", "Message": "Business Layer Updated Successfully!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})


    except Exception:
        logger.error("Error in Update Source!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})


@csrf_exempt
def get_bus_lay_process_def(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v

            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_processing_layers_list=[]

                            m_processing_list=ProcessingLayer.objects.all()
                            for processlayers in m_processing_list :
                                m_processing_layers_list.append({
                                    "processing_id" : processlayers.id,
                                    "processing_name" : processlayers.name
                                })


                            return JsonResponse({"Status":"Success","Data":m_processing_layers_list})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_bus_lay_rule_set(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0
            processing_id=0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "processing_id":
                    processing_id = v

            # print(processing_id)
            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_rule_set_list=[]

                            m_rules_set_list = BusinessLayer.objects.filter(processing_layer_id=processing_id).distinct()
                            # print("rules ",m_rules_set_list)
                            for rulesset in m_rules_set_list:
                                m_rule_set_list.append({
                                    "rule_set_id": rulesset.id,
                                    "rule_set_name": rulesset.rule_set_name
                                })
                            return JsonResponse({"Status":"Success","Data":m_rule_set_list})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_bus_lay_rule_name(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body=request.body.decode('utf-8')
            data=json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            user_id=0
            rule_set_id=0
            rule_set_name=''
            processing_id=''
            aggregator_id=0
            int_aggregator_id=0
            ext_aggregator_id=0
            int_m_source_id=0
            ext_m_source_id = 0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "rule_set_id":
                    rule_set_id = v
                if k == "processing_id":
                    processing_id = v


            if (int(tenant_id) > 0):
                if (int(group_id) > 0):
                    if (int(entity_id) > 0):
                        if(int(user_id)) > 0:
                            m_rule_name_list=[]
                            process_layer_define_list=[]
                            int_aggregator_list=[]
                            ext_aggregator_list=[]
                            int_attributename_list=[]
                            ext_attributename_list=[]
                            int_attribute_name_list = []
                            ext_attribute_name_list = []

                            business_layer_definition_list=[]

                            rule_set = BusinessLayer.objects.filter(id=rule_set_id)
                            for ruleset in rule_set:
                                rule_set_name=ruleset.rule_set_name

                            m_rules_set_list = BusinessLayer.objects.filter(rule_set_name=rule_set_name)
                            for rulesset in m_rules_set_list:
                                m_rule_name_list.append({
                                    "rule_set_id": rulesset.id,
                                    "rule_name": rulesset.rule_name
                                })
                            process_layer_definition_list = ProcessingLayerDefinition.objects.filter(processing_layer_id=processing_id)
                            for proslaydef in process_layer_definition_list:
                               if(proslaydef.side == 'Internal') :
                                   int_aggregator_id = proslaydef.m_aggregators_id
                               elif(proslaydef.side == 'External') :
                                   ext_aggregator_id = proslaydef.m_aggregators_id

                            int_aggregator_list=MasterAggregatorsDetails.objects.filter(m_aggregator=int_aggregator_id)
                            ext_aggregator_list = MasterAggregatorsDetails.objects.filter(m_aggregator=ext_aggregator_id)

                            for agglist in int_aggregator_list:
                                int_m_source_id=agglist.m_sources
                            for agglist in ext_aggregator_list:
                                ext_m_source_id=agglist.m_sources

                            int_attributename_list=MasterSourceDefinitions.objects.filter(m_sources=int_m_source_id)
                            ext_attributename_list = MasterSourceDefinitions.objects.filter(m_sources=ext_m_source_id)

                            for attrnames in int_attributename_list:
                                int_attribute_name_list.append({
                                    "attribute_id": attrnames.id,
                                    "attribute_name" : attrnames.attribute_name
                                })

                            for attrnames in ext_attributename_list:
                                ext_attribute_name_list.append({
                                    "attribute_id": attrnames.id,
                                    "attribute_name": attrnames.attribute_name
                                })

                            business_layer_definition_list.append({
                                "rule_name_list" :m_rule_name_list,
                                "int_attributename_list" :   int_attribute_name_list,
                                "ext_attributename_list": ext_attribute_name_list,
                            })
                            return JsonResponse({"Status":"Success","Data":business_layer_definition_list})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting Transformation Loading Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_processing_layer_list(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            type_id = ''

            for k,v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if  k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "typeId":
                    type_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if len(str(type_id)) > 0:

                                    if type_id == 0:
                                        processing_layer = ProcessingLayer.objects.filter(
                                            tenants_id=tenant_id,
                                            groups_id=group_id,
                                            entities_id=entity_id,
                                            m_processing_layer_id=m_processing_layer_id,
                                            m_processing_sub_layer_id=m_processing_sub_layer_id
                                        )
                                        processing_layer_list = []
                                        for layer in processing_layer:
                                            processing_layer_list.append(
                                                {
                                                    "processing_layer_id": layer.id,
                                                    "processing_layer_name": layer.name
                                                }
                                            )

                                    elif type_id != 0:
                                        type_connections = TypeConnections.objects.filter(types_id = type_id, is_active = 1)
                                        processing_layer_ids = []
                                        for connection in type_connections:
                                            processing_layer_ids.append(connection.processing_layer_id)

                                        processing_layer_list = []
                                        processing_layer = ProcessingLayer.objects.filter(
                                            tenants_id=tenant_id,
                                            groups_id=group_id,
                                            entities_id=entity_id,
                                            m_processing_layer_id=m_processing_layer_id,
                                            m_processing_sub_layer_id=m_processing_sub_layer_id
                                        )

                                        for layer in processing_layer:
                                            if layer.id in processing_layer_ids:
                                                processing_layer_list.append(
                                                    {
                                                        "processing_layer_id": layer.id,
                                                        "processing_layer_name": layer.name
                                                    }
                                                )
                                    return JsonResponse({"Status": "Success", "processing_layer_list": processing_layer_list})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Type Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status":"Error", "Message": "POST Method not Received!!!"})

    except Exception:
        logger.error("Error in Getting Processing Layer Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_processing_layer_def_list(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)
            # print(data)
            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if  k == "processing_layer_id":
                    processing_layer_id = v

            processing_layer_def = ProcessingLayerDefinition.objects.filter(
                tenants_id = tenant_id,
                groups_id = group_id,
                entities_id = entity_id,
                processing_layer_id = processing_layer_id
            )

            processing_layer = ProcessingLayer.objects.get(id = processing_layer_id)
            # print("id",processing_layer.name)

            file_locations = []
            for pro_lay in processing_layer_def:
                aggregator_details = MasterAggregatorsDetails.objects.filter(m_aggregator = pro_lay.m_aggregators_id)
                m_source_id = 0
                for agg_detail in aggregator_details:
                    m_source_id = agg_detail.m_sources.id
                source_details = MasterSources.objects.get(id = m_source_id)
                file_locations.append({
                    "processing_layer_name" : processing_layer.name,
                    "side" : pro_lay.side,
                    "aggregator_id" : pro_lay.m_aggregators_id,
                    "source_id" : m_source_id,
                    "input_location" : source_details.source_input_location
                })

            return  JsonResponse({"Status": "Success", "file_locations": file_locations})
        else:
            return JsonResponse({"Status": "Error", "Message": "GET Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Processing Layer Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def business_layer_definition(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = ''
            group_id = ''
            entity_id = ''
            user_id = ''
            processing_layer_id=''
            processing_layer_name=''
            rule_set_name_id=''
            business_layer_id=''
            rule_name_id = ''
            factor_type=''
            internal_field=''
            external_field=''
            condition=''
            date_tolerance=''
            amount_tolerance=''
            rule_name=''

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "user_id":
                    user_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "rule_set_name_id":
                    rule_set_name_id = v
                if k == "rule_name_id":
                    rule_name = v
                if k == "bus_lay_def_list":
                    bus_lay_def_list = v



            if len(str(tenant_id)) > 0:
                if len(str(group_id)) > 0:
                    if len(str(entity_id)) > 0:
                        if len(str(user_id)) > 0:
                            # print(bus_lay_def_list)
                            for buslaydeflist in bus_lay_def_list:
                                for k, v in buslaydeflist.items():
                                    if k == "factor_type":
                                        factor_type = v
                                    if k == "internal_field":
                                        internal_field = v
                                    if k == "external_field":
                                        external_field = v
                                    if k == "condition":
                                        condition = v
                                    if k == "date_tolerance":
                                        date_tolerance = v
                                    if k == "amount_tolerance":
                                        amount_tolerance = v


                                BusinessLayerDefinition.objects.create(
                                    tenants_id=tenant_id,
                                    groups_id=group_id,
                                    entities_id=entity_id,
                                    processing_layer_id=processing_layer_id,
                                    processing_layer_name=processing_layer_name,
                                    business_layer_id=1,
                                    rule_set_name='rule_set_name',
                                    rule_name=rule_name,
                                    factor_type=factor_type,
                                    internal_field=internal_field,
                                    external_field=external_field,
                                    condition=condition,
                                    date_tolerance=date_tolerance,
                                    amount_tolerance=amount_tolerance,
                                    is_active=1,
                                    created_by=user_id,
                                    created_date=timezone.now(),
                                    modified_by=user_id,
                                    modified_date=timezone.now()
                                       )
                            return JsonResponse({"Status": "Sucess", "Message": "Business Layer Definition Updated Successfully!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "User Id not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})


    except Exception:
        logger.error("Error in Update Source!!!", exc_info=True)
        return JsonResponse({"Status" : "Error"})

def get_source_insert_queries(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if  k == "tenants_id":
                    tenants_id = v
                if k == "groups_id":
                    groups_id = v
                if  k == "entities_id":
                    entities_id = v
                if k == "m_source_id":
                    m_source_id = v

            source_queries = SourceQueries.objects.filter(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entities_id,
                m_sources_id = m_source_id
            )

            for query in source_queries:
                insert_query = query.source_value

            m_source_definitions = MasterSourceDefinitions.objects.filter(
                tenants_id=tenants_id,
                groups_id=groups_id,
                entities_id=entities_id,
                m_sources_id=m_source_id,
                is_active=1
            ).order_by('attribute_position')

            m_source = MasterSources.objects.filter(
                tenants_id=tenants_id,
                groups_id=groups_id,
                entities_id=entities_id,
                id=m_source_id

            )
            source_config=''
            source_extension=''
            column_start_row=''
            m_source_name=''

            for msource in m_source:
                m_source_name = msource.source_name
                source_config = msource.source_config

            for k, v in source_config.items():
                if k == "source_extension":
                    source_extension = v
                if k == "column_start_row":
                    column_start_row = v

            attribute_name_list = []
            attribute_data_types_list = []
            unique_list = []

            for definition in m_source_definitions:
                attribute_name_list.append(definition.attribute_name)
                attribute_data_types_list.append(definition.attribute_data_type)
                unique_list.append(definition.is_unique)

            return JsonResponse({
                "Status": "Success",
                "insert_query": insert_query,
                "attribute_name_list": attribute_name_list,
                "attribute_data_types_list": attribute_data_types_list,
                "unique_list": unique_list,
                "source_extension": source_extension,
                "column_start_row" : column_start_row,
                "m_source_name" : m_source_name
            })

        else:
            return JsonResponse({"Status": "Error", "Message": "GET Method Not Received!!!"})

    except Exception:
        logger.error("Error in Getting source Insert Queries!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

#26AS

def get26AS_processing_layer_def_list(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)
            # print(data)
            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if  k == "processing_layer_id":
                    processing_layer_id = v
                if  k == "file_uploaded":
                    file_uploaded = v


            processing_layer_def = ProcessingLayerDefinition.objects.filter(
                tenants_id = tenant_id,
                groups_id = group_id,
                entities_id = entity_id,
                processing_layer_id = processing_layer_id,
            )

            m_sources = MasterSources.objects.filter(source_name = 'FORM_26AS_ZIP')
            for source in m_sources:
                source_id_zip = source.id
                source_input_location_zip = source.source_input_location

            m_sources = MasterSources.objects.filter(source_name = 'FORM_26AS_PASSWORD')
            for source in m_sources:
                source_id_pwd = source.id
                source_input_location_pwd = source.source_input_location

            m_sources = MasterSources.objects.filter(source_name = 'ERP_TAN_UPDATE')
            for source in m_sources:
                source_id_erp_tan_update = source.id
                source_input_location_erp_tan_update = source.source_input_location

            file_locations = []
            if file_uploaded == '26AS':
                file_locations.append({
                    "processing_layer_name": "RECON-26AS",
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_zip,
                    "input_location": source_input_location_zip

                })
            elif file_uploaded == 'PWD':
                file_locations.append({
                    "processing_layer_name": "RECON-26AS",
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_pwd,
                    "input_location": source_input_location_pwd
                })
            elif file_uploaded == "TAN":
                file_locations.append({
                    "processing_layer_name": "RECON-26AS",
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_erp_tan_update,
                    "input_location": source_input_location_erp_tan_update
                })

            processing_layer = ProcessingLayer.objects.get(id = processing_layer_id)

            m_aggregator_ids = []
            for pro_lay in processing_layer_def:
                m_aggregator_ids.append(pro_lay.m_aggregators_id)

            m_source_ids = []
            for m_aggregator_id in m_aggregator_ids:
                aggregator_details = MasterAggregatorsDetails.objects.filter(m_aggregator=m_aggregator_id)
                for agg_detail in aggregator_details:
                    m_source_ids.append(agg_detail.m_sources_id)

            for m_source_id in m_source_ids:
                source_details = MasterSources.objects.get(id = m_source_id)
                file_locations.append({
                    "processing_layer_name" : processing_layer.name,
                    "side" : pro_lay.side,
                    "aggregator_id" : pro_lay.m_aggregators_id,
                    "source_id" : m_source_id,
                    "input_location" : source_details.source_input_location
                })

            return  JsonResponse({"Status": "Success", "file_locations": file_locations})
        else:
            return JsonResponse({"Status": "Error", "Message": "GET Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Processing Layer Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_med_ins_processing_layer_def_list(request, *args, **kwargs):
    try:
        if request.method == "GET":
            body = request.body.decode('utf-8')
            data = json.loads(body)
            # print(data)
            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if  k == "processing_layer_id":
                    processing_layer_id = v
                if  k == "file_uploaded":
                    file_uploaded = v


            # processing_layer_def = ProcessingLayerDefinition.objects.filter(
            #     tenants_id = tenant_id,
            #     groups_id = group_id,
            #     entities_id = entity_id,
            #     processing_layer_id = processing_layer_id,
            # )

            m_sources = MasterSources.objects.filter(source_name = 'PR_FILE')
            for source in m_sources:
                source_id_pr_file = source.id
                source_input_location_pr_file = source.source_input_location

            m_sources = MasterSources.objects.filter(source_name = 'EXIT_REPORT')
            for source in m_sources:
                source_id_exit_report = source.id
                source_input_location_exit_report = source.source_input_location

            m_sources = MasterSources.objects.filter(source_name = 'TPA_FILE')
            for source in m_sources:
                source_id_tpa_confirm_report = source.id
                source_input_location_tpa_confirm_report = source.source_input_location

            m_sources = MasterSources.objects.filter(source_name = 'MIS_REPORT')
            for source in m_sources:
                source_id_mis_report = source.id
                source_input_location_mis_report = source.source_input_location

            m_sources = MasterSources.objects.filter(source_name = 'GPA_TRACKER')
            for source in m_sources:
                source_id_gpa_tracker = source.id
                source_input_location_gpa_tracker = source.source_input_location

            processing_layer_name = 'RECON-MED-INS'

            file_locations = []
            if file_uploaded == 'PR':
                file_locations.append({
                    "processing_layer_name": processing_layer_name,
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_pr_file,
                    "input_location": source_input_location_pr_file

                })
            elif file_uploaded == 'ER':
                file_locations.append({
                    "processing_layer_name": processing_layer_name,
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_exit_report,
                    "input_location": source_input_location_exit_report
                })
            elif file_uploaded == "TCR":
                file_locations.append({
                    "processing_layer_name": processing_layer_name,
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_tpa_confirm_report,
                    "input_location": source_input_location_tpa_confirm_report
                })
            elif file_uploaded == "MIS":
                file_locations.append({
                    "processing_layer_name": processing_layer_name,
                    "side": "External",
                    "aggregator_id": 1,
                    "source_id": source_id_mis_report,
                    "input_location": source_input_location_mis_report
                })
            elif file_uploaded == "GDT":
                file_locations.append({
                    "processing_layer_name": processing_layer_name,
                    "side": "External",
                    "aggregator": 1,
                    "source_id": source_id_gpa_tracker,
                    "input_location": source_input_location_gpa_tracker
                })

            return  JsonResponse({"Status": "Success", "file_locations": file_locations})
        else:
            return JsonResponse({"Status": "Error", "Message": "GET Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Processing Layer Details!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_check_spark(request, *args, **kwargs):
    try:
        if request.method == "GET":

            data = [
                {"Name" : "Pandi", "Age": 25, "Designation": "Data Scientist"},
                {"Name" : "Surya", "Age" : 26, "Designation" : "Big Data Architect"}
            ]

            return JsonResponse({"Status" : "Success", "data" : data})
    except Exception:
        logger.error("Error in Check Spark!!!", exc_info=True)