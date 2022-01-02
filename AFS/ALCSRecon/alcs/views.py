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

logger = logging.getLogger("alcs")

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

