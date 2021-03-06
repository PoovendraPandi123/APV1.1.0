from rest_framework import serializers
from .models import *

class FileUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = FileUploads
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_source_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'processing_layer_name', 'source_type', 'extraction_type', 'file_name', 'file_size_bytes', 'file_upload_type', 'file_path', 'status', 'comments', 'is_processed', 'is_processing', 'input_date', 'is_active', 'created_by', 'modified_by', 'created_date']

class MasterClientDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = MasterClientDetails
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'client_id', 'client_name', 'email_address', 'frequency', 'last_send_on', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']

class ExternalRecordsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExternalRecords
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'processing_layer_name',
                  'ext_processing_status_1', 'ext_reference_text_1', 'ext_reference_text_2', 'ext_reference_text_3', 'ext_reference_text_4', 'ext_reference_text_5',
                  'ext_reference_date_time_1', 'ext_reference_date_time_2', 'ext_generated_num_1', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']

class InternalRecordsSerializer(serializers.ModelSerializer):
    class Meta:
        model = InternalRecords
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'processing_layer_name',
                  'int_processing_status_1', 'int_reference_text_1', 'int_reference_text_2', 'int_reference_text_3', 'int_reference_text_4', 'int_reference_text_5',
                  'int_reference_text_6', 'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11',
                  'int_reference_text_12', 'int_reference_text_13', 'int_reference_text_14', 'int_reference_text_15', 'int_reference_text_16', 'int_reference_text_17',
                  'int_reference_text_18', 'int_reference_text_19', 'int_reference_text_20', 'int_amount_1', 'int_amount_2', 'int_amount_3', 'int_amount_4', 'int_amount_5',
                  'int_reference_date_time_1', 'int_reference_date_time_2', 'int_reference_date_time_3', 'int_reference_date_time_4', 'int_reference_date_time_5',
                  'int_generated_num_1', 'int_generated_num_2', 'int_generated_num_3', 'int_extracted_text_1', 'int_extracted_text_2', 'int_extracted_text_3',
                  'int_extracted_text_4', 'int_extracted_text_5', 'int_extracted_text_6', 'int_extracted_text_7', 'int_extracted_text_8', 'int_extracted_text_9',
                  'int_extracted_text_10', 'int_extracted_text_11', 'int_extracted_text_12', 'int_extracted_text_13', 'int_extracted_text_14', 'int_extracted_text_15',
                  'int_extracted_text_16', 'int_extracted_text_17', 'int_extracted_text_18', 'int_extracted_text_19', 'int_extracted_text_20', 'is_active',
                  'created_by', 'created_date', 'modified_by', 'modified_date']

class RecoSettingsSerializer(serializers.ModelSerializer):
    class Meta:
        model = RecoSettings
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id',
                  'setting_key', 'setting_value', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']
