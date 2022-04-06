from rest_framework import serializers
from .models import *

class SourceDefintionSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    class Meta:
        model = SourceDefinitions
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'sources', 'attribute_name', 'attribute_position', 'attribute_data_type', 'attribute_date_format', 'attribute_pattern', 'attribute_enums', 'attribute_min_length', 'attribute_max_length', 'attribute_formula', 'attribute_reference_field', 'is_validate', 'is_required', 'is_unique', 'is_editable', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']
        read_only_fields = ('sources', )

class SourceSerializer(serializers.ModelSerializer):
    source_definitions = SourceDefintionSerializer(many=True)
    class Meta:
        model = Sources
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'source_code', 'source_name', 'source_config', 'source_input_location', 'source_import_seq', 'source_field_number', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date', 'source_definitions']

class ModuleSetingsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ModuleSettings
        field = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'setting_key', 'setting_value', 'setting_description', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']

class TargetFilesDefinitionsSerializer(serializers.ModelSerializer):
    class Meta:
        model = TargetFileDefinitions
        field = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'field_name', 'field_sequence', 'files_config', 'target_files', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']

class TargetFilesSerializer(serializers.ModelSerializer):
    target_files_definitions = TargetFilesDefinitionsSerializer(many=True)
    class Meta:
        model = TargetFiles
        field = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'name', 'description', 'files_config', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date', 'target_file_definitions']

class ReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Reports
        field = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'name', 'description', 'report_config', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']