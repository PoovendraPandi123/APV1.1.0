from rest_framework import serializers
from .models import *
import os
import logging
from . import keyword_check as kc

logger = logging.getLogger("consolidation_files")

class SourceDefintionSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    class Meta:
        model = SourceDefinitions
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'sources', 'attribute_name', 'attribute_position', 'attribute_data_type', 'attribute_date_format', 'attribute_pattern', 'attribute_enums', 'attribute_min_length', 'attribute_max_length', 'attribute_formula', 'attribute_reference_field', 'is_validate', 'is_required', 'is_unique', 'is_editable', 'is_primary_date', 'is_status', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date']
        read_only_fields = ('sources', )

class SourceSerializer(serializers.ModelSerializer):
    source_definitions = SourceDefintionSerializer(many=True)
    class Meta:
        model = Sources
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'source_code', 'source_name', 'source_config', 'source_input_location', 'source_import_location', 'source_import_seq', 'source_field_number', 'is_active', 'created_by', 'created_date', 'modified_by', 'modified_date', 'key_words', 'source_definitions']

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
            key_words_dict = validated_data.get("key_words")
            key_words = key_words_dict["keywords"]

            keywords_unique_check = kc.KeywordsUniqueCheck(keyword = key_words.lower())
            keywords_unique_check_output = keywords_unique_check.get_keyword_unique_check_output()

            print("keywords_unique_check_output", keywords_unique_check_output)

            if keywords_unique_check_output:

                source = Sources.objects.filter(
                    tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, source_name = source_name, is_active = 1
                )

                if not source:
                    module_settings_input = ModuleSettings.objects.filter(
                        tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, setting_key = 'source_input_path'
                    )

                    module_settings_import = ModuleSettings.objects.filter(
                        tenants_id = tenants_id, groups_id = groups_id, entities_id = entities_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, setting_key = 'source_import_path'
                    )

                    for setting in module_settings_input:
                        source_path = setting.setting_value["sourceInputPath"]

                    for setting in module_settings_import:
                        import_path = setting.setting_value["sourceImportPath"]

                    source_path_proper = source_path.replace("{source_name}", source_name)
                    source_import_path_proper = import_path.replace("{source_name}", source_name)

                    if not os.path.exists(source_path_proper):
                        os.mkdir(source_path_proper)

                    if not os.path.exists(source_import_path_proper):
                        os.mkdir(source_import_path_proper)

                    source_path_proper_input = source_path_proper + "/" + "input"
                    if not os.path.exists(source_path_proper_input):
                        os.mkdir(source_path_proper_input)
                    # print("source_path_proper", source_path_proper)

                    source_import_path_proper_add = source_import_path_proper + "/" + "import"
                    if not os.path.exists(source_import_path_proper_add):
                        os.mkdir(source_import_path_proper_add)

                    validated_data["source_input_location"] = source_path_proper_input
                    validated_data["source_import_location"] = source_import_path_proper_add
                    validated_data["key_words"] = {
                        "keywords": key_words.split(",")
                    }

                    sources = Sources.objects.create(**validated_data)
                    return sources

        except Exception:
            logger.error("Error in Creating Source!!!", exc_info=True)
            raise serializers.ValidationError({"Status": "Error", "Message": "Error in Creating Source!!!"})

class ModuleSetingsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ModuleSettings
        fields = '__all__'

class TargetFilesDefinitionsSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    class Meta:
        model = TargetFileDefinitions
        fields = '__all__'

class TargetFilesSerializer(serializers.ModelSerializer):
    target_file_definitions = TargetFilesDefinitionsSerializer(many=True)
    class Meta:
        model = TargetFiles
        fields = '__all__'

    def create(self, validated_data):
        try:
            target_file_definitions = validated_data.pop("target_file_definitions")
            tenants_id = validated_data.get("tenants_id")
            groups_id = validated_data.get("groups_id")
            entities_id = validated_data.get("entities_id")
            m_processing_layer_id = validated_data.get("m_processing_layer_id")
            m_processing_sub_layer_id = validated_data.get("m_processing_sub_layer_id")
            processing_layer_id = validated_data.get("processing_layer_id")
            name = validated_data.get("name")

            target_files = TargetFiles.objects.filter(
                tenants_id=tenants_id, groups_id=groups_id, entities_id=entities_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id, name=name, is_active=1
            )

            if not target_files:
                target_files = TargetFiles.objects.create(**validated_data)
                return target_files

        except Exception:
            logger.error("Error in Creating Target!!!", exc_info=True)
            raise serializers.ValidationError({"Status": "Error", "Message": "Error in Creating Target!!!"})

class ReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Reports
        fields = '__all__'

class FileUploadsSerializer(serializers.ModelSerializer):
    class Meta:
        model = FileUploads
        fields = '__all__'