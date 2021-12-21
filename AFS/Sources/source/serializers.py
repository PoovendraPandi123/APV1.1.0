from rest_framework import serializers
from .models import *

class SourceDefintionSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    class Meta:
        model = MasterSourceDefinitions
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'm_sources', 'attribute_name', 'attribute_position', 'attribute_data_type', 'attribute_date_format', 'attribute_pattern', 'attribute_enums', 'attribute_min_length', 'attribute_max_length', 'attribute_formula', 'attribute_reference_field', 'is_validate', 'is_required', 'is_unique', 'is_editable', 'is_active', 'created_by', 'modified_by']
        read_only_fields = ('m_sources', )
        # depth = 1
    # def create(self, validated_data):
    #     print(validated_data)
    #     pass

class SourceSerializer(serializers.ModelSerializer):
    m_source_definitions = SourceDefintionSerializer(many=True)
    class Meta:
        model = MasterSources
        fields = ['id', 'tenants_id', 'groups_id', 'entities_id', 'source_code', 'source_name', 'source_config', 'source_input_location', 'source_import_seq', 'source_field_number', 'is_active', 'created_by', 'modified_by', 'm_source_definitions']
        # fields = '__all__'

    def create(self, validated_data):
        m_source_definitions = validated_data.pop('m_source_definitions')
        m_sources = MasterSources.objects.create(**validated_data)
        for m_source_definition in m_source_definitions:
            MasterSourceDefinitions.objects.create(**m_source_definition, m_sources = m_sources)
        return m_sources

    def update(self, instance, validated_data):
        m_source_definitions_data = validated_data.pop('m_source_definitions')
        instance.tenants_id = validated_data.get('tenants_id', instance.tenants_id)
        instance.groups_id = validated_data.get('groups_id', instance.groups_id)
        instance.entities_id = validated_data.get('entities_id', instance.entities_id)
        instance.source_name = validated_data.get('source_name', instance.source_name)
        instance.source_config = validated_data.get('source_config', instance.source_config)
        instance.source_input_location = validated_data.get('source_input_location', instance.source_input_location)
        instance.source_import_seq = validated_data.get('source_import_seq', instance.source_import_seq)
        instance.modified_by = validated_data.get('modified_by', instance.modified_by)
        instance.save()
        keep_m_source_definitions = []
        # existing_ids = [m_source_definition.id for m_source_definition in instance.m_source_definitions]
        # Loop through m_source_definitions list
        for m_source_definition in m_source_definitions_data:
            if "id" in m_source_definition.keys():
                if MasterSourceDefinitions.objects.filter(id = m_source_definition["id"]).exists():
                    m_source_definitions = MasterSourceDefinitions.objects.get(id = m_source_definition["id"])
                    m_source_definitions.tenants_id = m_source_definition.get('tenants_id', m_source_definitions.tenants_id)
                    m_source_definitions.groups_id = m_source_definition.get('groups_id', m_source_definitions.groups_id)
                    m_source_definitions.entities_id = m_source_definition.get('entities_id', m_source_definitions.entities_id)
                    m_source_definitions.attribute_name = m_source_definition.get('attribute_name', m_source_definitions.attribute_name)
                    m_source_definitions.attribute_position = m_source_definition.get('attribute_position', m_source_definitions.attribute_position)
                    m_source_definitions.attribute_data_type = m_source_definition.get('attribute_data_type', m_source_definitions.attribute_data_type)
                    m_source_definitions.attribute_date_format = m_source_definition.get('attribute_date_format', m_source_definitions.attribute_date_format)
                    m_source_definitions.attribute_pattern = m_source_definition.get('attribute_pattern', m_source_definitions.attribute_pattern)
                    m_source_definitions.attribute_enums = m_source_definition.get('attribute_enums', m_source_definitions.attribute_enums)
                    m_source_definitions.attribute_min_length = m_source_definition.get('attribute_min_length', m_source_definitions.attribute_min_length)
                    m_source_definitions.attribute_max_length = m_source_definition.get('attribute_max_length', m_source_definitions.attribute_max_length)
                    m_source_definitions.attribute_formula = m_source_definition.get('attribute_formula', m_source_definitions.attribute_formula)
                    m_source_definitions.attribute_reference_field = m_source_definition.get('attribute_reference_field', m_source_definitions.attribute_reference_field)
                    m_source_definitions.is_validate = m_source_definition.get('is_validate', m_source_definitions.is_validate)
                    m_source_definitions.is_required = m_source_definition.get('is_required', m_source_definitions.is_required)
                    m_source_definitions.is_unique = m_source_definition.get('is_unique', m_source_definitions.is_unique)
                    m_source_definitions.is_editable = m_source_definition.get('is_editable', m_source_definitions.is_editable)
                    m_source_definitions.is_active = m_source_definition.get('is_active', m_source_definitions.is_active)
                    m_source_definitions.modified_by = m_source_definition.get('modified_by', m_source_definitions.modified_by)
                    m_source_definitions.save()
                    keep_m_source_definitions.append(m_source_definitions.id)
                else:
                    continue
            else:
                m_source_definitions = MasterSourceDefinitions.objects.create(**m_source_definition, m_sources = instance)
                keep_m_source_definitions.append(m_source_definitions.id)

        for m_source_definition in instance.m_source_definitions:
            if m_source_definition.id not in keep_m_source_definitions:
                m_source_definition.delete()

        return instance

class AggregatorDetilsSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required = False)
    class Meta:
        model = MasterAggregatorDetails
        fields = ["id", "tenants_id", "groups_id", "entities_id", "m_sources_id", "m_aggregator", "is_base_source", "is_active", "created_by", "modified_by"]
        read_only_fields = ('m_aggregator',)

class AggregatorSerializer(serializers.ModelSerializer):
    m_aggregator_details = AggregatorDetilsSerializer(many = True)
    class Meta:
        model = MasterAggregators
        fields = ["id", "tenants_id", "groups_id", "entities_id", "aggregator_code", "aggregator_name", "is_active", "created_by", "modified_by", "m_aggregator_details"]

    def create(self, validated_data):
        m_aggregator_details = validated_data.pop('m_aggregator_details')
        m_aggregators = MasterAggregators.objects.create(**validated_data)
        for aggregator_detail in m_aggregator_details:
            MasterAggregatorDetails.objects.create(**aggregator_detail, m_aggregator = m_aggregators)
        return m_aggregators

    def update(self, instance, validated_data):
        m_aggregator_details_data = validated_data.pop('m_aggregator_details')
        instance.tenants_id = validated_data.get('tenants_id', instance.tenants_id)
        instance.groups_id = validated_data.get('groups_id', instance.groups_id)
        instance.entities_id = validated_data.get('entities_id', instance.entities_id)
        instance.aggregator_name = validated_data.get('aggregator_name', instance.aggregator_name)
        instance.modified_by = validated_data.get('modified_by', instance.modified_by)
        instance.save()
        keep_m_aggregator_details = []
        for m_aggregator_detail in m_aggregator_details_data:
            if "id" in m_aggregator_detail.keys():
                if MasterAggregatorDetails.objects.filter(id = m_aggregator_detail["id"]).exists():
                    m_aggregator_details = MasterAggregatorDetails.objects.get(id = m_aggregator_detail["id"])
                    m_aggregator_details.tenants_id = m_aggregator_detail.get("tenants_id", m_aggregator_details.tenants_id)
                    m_aggregator_details.groups_id = m_aggregator_detail.get("groups_id", m_aggregator_details.groups_id)
                    m_aggregator_details.entities_id = m_aggregator_detail.get("entities_id", m_aggregator_details.entities_id)
                    m_aggregator_details.m_sources_id = m_aggregator_detail.get("m_sources_id", m_aggregator_details.m_sources_id)
                    m_aggregator_details.is_base_source = m_aggregator_detail.get("is_base_source", m_aggregator_details.is_base_source)
                    m_aggregator_details.is_active = m_aggregator_detail.get("is_active", m_aggregator_details.is_active)
                    m_aggregator_details.modified_by = m_aggregator_detail.get("modified_by", m_aggregator_details.modified_by)
                    m_aggregator_details.save()
                    keep_m_aggregator_details.append(m_aggregator_details.id)
                else:
                    continue
            else:
                m_aggregator_details = MasterAggregatorDetails.objects.create(**m_aggregator_detail, m_aggregator = instance)
                keep_m_aggregator_details.append(m_aggregator_details.id)

        for m_aggregator_detail in instance.m_aggregator_details:
            if m_aggregator_detail.id not in keep_m_aggregator_details:
                m_aggregator_detail.delete()

        return instance

class TransformationTypesSerializer(serializers.ModelSerializer):
    class Meta:
        model = MasterTransformationTypes
        fields = ["id", "tenants_id", "groups_id", "entities_id", "transformation_name", "transformation_description", "is_active", "created_by", "modified_by"]

class TransformationOperatorsSerializer(serializers.ModelSerializer):
    class Meta:
        model = MasterTransformationOperators
        fields = ["id", "tenants_id", "groups_id", "entities_id", "operator_key", "operator_value", "is_active", "created_by", "modified_by"]

class AggregatorTransformationsSerializer(serializers.ModelSerializer):
    class Meta:
        model = MasterAggregatorTransformations
        fields = ["id", "tenants_id", "groups_id", "entities_id", "m_aggregator_id", "m_aggregator_transforms", "is_active", "created_by", "modified_by"]

class FieldExtarctionSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    class Meta:
        model = FieldExtraction
        fields = ["id", "tenants_id", "groups_id", "entities_id", "m_aggregator_id", "m_transformations", "m_sources_id", "m_source_definitions_id", "pattern_type", "pattern_input", "transaction_placed", "transaction_reference", "is_active", "created_by", "modified_by"]
        read_only_fields = ('m_transformations',)

class MasterTransformationsSerializer(serializers.ModelSerializer):
    field_extraction = FieldExtarctionSerializer(many=True)
    _extractions = dict()
    class Meta:
        model = MasterTransformations
        fields = ["id", "tenants_id", "groups_id", "entities_id", "m_transformation_types_id", "transformation_name", "is_active", "created_by", "modified_by", "field_extraction"]

    def create(self, validated_data):
        field_extraction = validated_data.pop('field_extraction')
        m_transformations = MasterTransformations.objects.create(**validated_data)
        for field in field_extraction:
            FieldExtraction.objects.create(**field, m_transformations = m_transformations)
        if len(field_extraction) > 0:
            self._extractions["field_extraction"] = 1
            tenants_id = validated_data["tenants_id"]
            groups_id = validated_data["groups_id"]
            entities_id = validated_data["entities_id"]
            m_aggregator_id = field_extraction[0]["m_aggregator_id"]
            m_aggregator_transforms = self._extractions
            is_active = 1
            created_by = validated_data["created_by"]
            modified_by = validated_data["modified_by"]
            MasterAggregatorTransformations.objects.create(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entities_id,
                m_aggregator_id = m_aggregator_id,
                m_aggregator_transforms = m_aggregator_transforms,
                is_active = is_active,
                created_by = created_by,
                modified_by = modified_by
            )
        return m_transformations

    def update(self, instance, validated_data):
        field_extraction = validated_data.pop('field_extraction')
        instance.tenants_id = validated_data.get('tenants_id', instance.tenants_id)
        instance.groups_id = validated_data.get('groups_id', instance.groups_id)
        instance.entities_id = validated_data.get('entities_id', instance.entities_id)
        instance.m_aggregator_transformations_id = validated_data.get('m_aggregator_transformations_id', instance.m_aggregator_transformations_id)
        instance.transformation_name = validated_data.get('transformation_name', instance.transformation_name)
        instance.created_by = validated_data.get('created_by', instance.created_by)
        instance.modified_by = validated_data.get('modified_by', instance.modified_by)
        instance.save()
