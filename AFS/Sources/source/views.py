import logging
import json
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
import uuid
from django.db import connection
import pandas as pd
from .packages import read_mysql, read_postgres
from rest_framework.generics import GenericAPIView
from rest_framework import mixins
from rest_framework.response import Response
from rest_framework import status
from .serializers import *
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework import viewsets
from rest_framework.decorators import action

# Create your views here.

logger = logging.getLogger("m_sources")

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

# @csrf_exempt
# def get_insert_source(request, *args, **kwargs):
#     try:
#         if request.method == "POST":
#             body = request.body.decode('utf-8')
#             data = json.loads(body)
#
#             source_config = dict()
#
#             for k,v in data.items():
#                 if k == "tenantId":
#                     tenant_id = v
#                 if k == "groupId":
#                     group_id = v
#                 if  k == "entityId":
#                     entity_id = v
#                 if  k == "userId":
#                     user_id = v
#                 if k == "sourceName":
#                     source_name = v
#                 if k == "sourceType":
#                     source_type = v
#                 if  k == "fileType":
#                     file_type = v
#                 if k == "textSeparator":
#                     text_separator = v
#                 if k == "otherSeparator":
#                     other_separator = v
#                 if k == "filePasswordRequired":
#                     file_password_required = v
#                 if k == "filePassword":
#                     file_password = v
#                 if  k == "sheetNameRequired":
#                     sheet_name_required = v
#                 if  k == "sheetName":
#                     sheet_name = v
#                 if  k == "sheetPasswordRequired":
#                     sheet_password_required = v
#                 if  k == "sheetPassword":
#                     sheet_password = v
#                 if k == "columnStartRow":
#                     column_start_row = v
#                 if  k == "database":
#                     database = v
#                 if  k == "dbHost":
#                     db_host = v
#                 if k == "dbPort":
#                     db_port = v
#                 if k == "dbUserName":
#                     db_user_name = v
#                 if  k == "dbPassword":
#                     db_password = v
#                 if k == "schema":
#                     schema = v
#                 if k == "table":
#                     table = v
#                 if k == "psqlDatabase":
#                     psql_database = v
#                 if k == "importSequence":
#                     import_sequence = v
#
#             source_config["source_type"] = source_type
#             source_config["file_type"] = file_type
#             source_config["text_separator"] = text_separator
#             source_config["other_separator"] = other_separator
#             source_config["file_password_required"] = file_password_required
#             source_config["file_password"] = file_password
#             source_config["sheet_name_required"] = sheet_name_required
#             source_config["sheet_name"] = sheet_name
#             source_config["sheet_password_required"] = sheet_password_required
#             source_config["sheet_password"] = sheet_password
#             source_config["column_start_row"] = column_start_row
#             source_config["database"] = database
#             source_config["db_host"] = db_host
#             source_config["db_port"] = db_port
#             source_config["db_user_name"] = db_user_name
#             source_config["db_password"] = db_password
#             source_config["schema"] = schema
#             source_config["table"] = table
#             source_config["psql_database"] = psql_database
#
#             source_settings = SourceSettings.objects.filter(
#                 setting_key = 'source_path'
#             )
#
#             for setting in source_settings:
#                 source_path = setting.setting_value
#
#             source_input_location = source_path["sourceInputPath"].replace("{source_name}", source_name.upper().replace(" ", "_"))
#
#             MasterSources.objects.create(
#                 tenants_id = tenant_id,
#                 groups_id = group_id,
#                 entities_id = entity_id,
#                 source_code = str(uuid.uuid4()),
#                 source_name = source_name,
#                 source_config = source_config,
#                 source_input_location = source_input_location,
#                 source_import_seq = import_sequence,
#                 source_field_number = 1,
#                 is_active = 1,
#                 created_by = user_id,
#                 created_date = timezone.now(),
#                 modified_by = user_id,
#                 modified_date = timezone.now()
#             )
#
#             return JsonResponse({"Status": "Success", "Message": "Records Inserted Successfully"})
#         else:
#             return JsonResponse({"Status": "Error", "Message": "POST Method not received!!!"})
#     except Exception:
#         logger.error("Error in Updating Source!!!", exc_info=True)

class SourceViewGeneric(GenericAPIView, mixins.ListModelMixin, mixins.CreateModelMixin, mixins.RetrieveModelMixin):
    serializer_class = SourceSerializer
    queryset = MasterSources.objects.all()
    lookup_field = 'id'
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]
    _success_message = {"Status" : "Success"}
    _error_message = {"Status" : "Error"}

    def get(self, request, id=None):
        try:
            if id != 'null':
                return self.retrieve(request)
            else:
                return self.list(request)
        except Exception:
            logger.error("Error in Retrieving Source Data!!!", exc_info=True)
            return Response(self._error_message, status=status.HTTP_400_BAD_REQUEST)

    def post(self, request, id=None):
        try:
            self.create(request)
            return Response(self._success_message, status=status.HTTP_201_CREATED)
        except Exception:
            logger.error("Error in Creating Source Records!!!", exc_info=True)
            return Response(self._error_message, status=status.HTTP_400_BAD_REQUEST)

    def perform_create(self, serializer):
        serializer.save(source_code = str(uuid.uuid4()))

class SourceDefinitionViewGenericNo(GenericAPIView, mixins.ListModelMixin, mixins.RetrieveModelMixin, mixins.CreateModelMixin):
    serializer_class = SourceDefintionSerializer
    queryset = MasterSourceDefinitions.objects.all()
    lookup_field = 'id'

    _success_message = {"Status" : "Success"}
    _error_message = {"Status" : "Error"}

    def get(self, request, id=None):
        try:
            if id != 'null':
                return self.retrieve(request)
            else:
                return self.list(request)
        except Exception:
            logger.error("Error in Retrieving Source Definition Data!!!", exc_info=True)
            return Response(self._error_message, status=status.HTTP_400_BAD_REQUEST)

    # def post(self, request, id=None):
    #     try:
    #         body = request.body.decode('utf-8')
    #         data_list = json.loads(body)
    #         print(data_list)
    #         for data in data_list:
    #             print(data)
    #             serializer = SourceDefintionSerializer(data=data)
    #             if serializer.is_valid():
    #                 serializer.save()
    #         return Response(self._success_message, status=status.HTTP_201_CREATED)
    #         # return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    #     except Exception:
    #         logger.error("Error in Creating Source Definition Data!!!", exc_info=True)
    #         return Response(self._error_message, status=status.HTTP_400_BAD_REQUEST)
    def post(self, request, id=None):
        try:
            self.create(request)
            return Response(self._success_message, status=status.HTTP_201_CREATED)
        except Exception:
            logger.error("Error in Creating Source Definition Records!!!", exc_info=True)
            return Response(self._error_message, status=status.HTTP_400_BAD_REQUEST)


# Proper
class SourceViewSet(viewsets.ModelViewSet):
    queryset = MasterSources.objects.all()
    serializer_class = SourceSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]

    def perform_create(self, serializer):
        serializer.save(source_code = str(uuid.uuid4()))

    @action(detail=True, methods=["GET"])
    def get_source_definitions(self, request, pk=None):
        m_sources = self.get_object()
        m_source_definitions = MasterSourceDefinitions.objects.filter(m_sources = m_sources)
        serializer = SourceDefintionSerializer(m_source_definitions, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    # @action(detail=True, methods=["POST"])
    # def post_source_definitions(self, request, pk=None):
    #     m_sources = self.get_object()
    #     data = request.data
    #     data["m_sources"] = m_sources.id
    #     serializer = SourceDefintionSerializer(data = data)
    #     if serializer.is_valid():
    #         serializer.save()
    #         return Response(serializer.data, status=status.HTTP_201_CREATED)
    #     return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class AggregatorViewSet(viewsets.ModelViewSet):
    queryset = MasterAggregators.objects.all()
    serializer_class = AggregatorSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]

    def perform_create(self, serializer):
        serializer.save(aggregator_code = str(uuid.uuid4()))

    @action(detail=True, methods=["GET"])
    def get_aggregator_details(self, request, pk=None):
        m_aggregator = self.get_object()
        m_aggregator_details = MasterAggregatorDetails.objects.filter(m_aggregator = m_aggregator)
        serializer = AggregatorDetilsSerializer(m_aggregator_details, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

class TransformationTypesGeneric(GenericAPIView, mixins.ListModelMixin):
    queryset = MasterTransformationTypes.objects.all()
    serializer_class = TransformationTypesSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]

    def get(self, request):
        return self.list(request)

class TransformationOperatorsGeneric(GenericAPIView, mixins.ListModelMixin):
    queryset = MasterTransformationOperators.objects.all()
    serializer_class = TransformationOperatorsSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]

    def get(self, request):
        return self.list(request)

class AggregatorTransformationsGeneric(GenericAPIView, mixins.ListModelMixin):
    queryset = MasterAggregatorTransformations.objects.all()
    serializer_class = AggregatorTransformationsSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]

    def get(self, request):
        return self.list(request)

class TransformationsViewSet(viewsets.ModelViewSet):
    queryset = MasterTransformations.objects.all()
    serializer_class = MasterTransformationsSerializer
    # authentication_classes = [TokenAuthentication]
    # permission_classes = [IsAuthenticated]





