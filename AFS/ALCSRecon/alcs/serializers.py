from rest_framework import serializers
from .models import *

class FileUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = FileUploads
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_source_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'processing_layer_name', 'source_type', 'extraction_type', 'file_name', 'file_path', 'status', 'comments', 'is_processed', 'is_processing', 'is_active', 'created_by', 'modified_by']
