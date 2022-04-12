from rest_framework import serializers
from .models import *
import os
import logging
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse


logger = logging.getLogger("consolidation_files")

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

    def create(self, validated_data):
        try:
            m_source_definitions = validated_data.pop("source_definitions")
            tenants_id = validated_data.get("tenants_id")
            groups_id = validated_data.get("groups_id")
            entities_id = validated_data.get("entities_id")
            m_processing_layer_id = validated_data.get("m_processing_layer_id")
            m_processing_sub_layer_id = validated_data.get("m_processing_sub_layer_id")
            processing_layer_id = validated_data.get("processing_layer_id")
            source_name = validated_data.get("source_name")

            source = Sources.objects.filter(
                tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, source_name = source_name, is_active = 1
            )

            if not source:
                module_settings = ModuleSettings.objects.filter(
                    tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, setting_key = 'source_input_path'
                )

                for setting in module_settings:
                    source_path = setting.setting_value["sourceInputPath"]

                source_path_proper = source_path.replace("{source_name}", source_name)


                if not os.path.exists(source_path_proper):
                    os.mkdir(source_path_proper)

                source_path_proper_input = source_path_proper + "/" + "input"
                if not os.path.exists(source_path_proper_input):
                    os.mkdir(source_path_proper_input)
                # print("source_path_proper", source_path_proper)

                validated_data["source_input_location"] = source_path_proper_input

                sources = Sources.objects.create(**validated_data)
                return sources

        except Exception:
            logger.error("Error in Creating Source!!!", exc_info=True)


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