from django.db import models
from django.utils import timezone

# Create your models here.
# Models For Sources
class SourceSettings(models.Model):
    class Meta:
        db_table = "source_settings"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    setting_key = models.CharField(max_length=64, verbose_name="Setting Key", null=True)
    setting_value = models.JSONField(verbose_name="Setting Value", null=True)
    setting_description = models.TextField(verbose_name="Setting Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class MasterSources(models.Model):
    class Meta:
        db_table = "m_sources"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    source_code = models.CharField(max_length=64, verbose_name="Source Code", null=False, unique=True)
    source_name = models.CharField(max_length=64, verbose_name="Source Name", null=False, unique=True)
    source_config = models.JSONField(verbose_name="Source Configurations")
    source_input_location = models.CharField(max_length=512, verbose_name="Source Input Location", null=True)
    source_archive_location = models.CharField(max_length=512, verbose_name="Source Archive Location", null=True)
    source_error_location = models.CharField(max_length=512, verbose_name="Source Error Location", null=True)
    source_import_location = models.CharField(max_length=512, verbose_name="Source Import Location", null=True)
    source_import_seq = models.PositiveIntegerField(verbose_name="Source Import Sequence", null=True)
    source_field_number = models.PositiveIntegerField(verbose_name="Source Field Number", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


class MasterSourcesAudit(models.Model):
    class Meta:
        db_table = "m_sources_audit"

    id = models.AutoField(primary_key=True)
    operation_flag = models.CharField(max_length=512, verbose_name="Audit Flag")
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_sources_id = models.PositiveIntegerField(verbose_name="Master Source Id")
    source_code = models.CharField(max_length=64, verbose_name="Source Code", null=False, unique=True)
    source_name = models.CharField(max_length=64, verbose_name="Source Name", null=False, unique=True)
    source_config = models.JSONField(verbose_name="Source Configurations")
    source_input_location = models.CharField(max_length=512, verbose_name="Source Input Location", null=True)
    source_archive_location = models.CharField(max_length=512, verbose_name="Source Archive Location", null=True)
    source_error_location = models.CharField(max_length=512, verbose_name="Source Error Location", null=True)
    source_import_location = models.CharField(max_length=512, verbose_name="Source Import Location", null=True)
    source_import_seq = models.PositiveIntegerField(verbose_name="Source Import Sequence", null=True)
    source_field_number = models.PositiveIntegerField(verbose_name="Source Field Number", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

# Models For Source Definition

class MasterSourceFileUploads(models.Model):
    class Meta:
        db_table = "m_source_file_uploads"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_sources = models.ForeignKey(MasterSources, verbose_name="MasterSources Id (Auto Generated)", on_delete=models.CASCADE)
    file_name = models.CharField(max_length=128, verbose_name="File Name", null=True)
    file_size_bytes = models.PositiveIntegerField(verbose_name="File Size Bytes", null=True)
    file_path = models.CharField(max_length=512, verbose_name="File Path", null=True)
    status = models.CharField(max_length=32, verbose_name="Status", null=True)
    comments = models.TextField(verbose_name="Comments", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")



class MasterSourceDefinitions(models.Model):
    class Meta:
        db_table = "m_source_definitions"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_sources = models.ForeignKey(MasterSources, verbose_name="MasterSources Id (Auto Generated)", on_delete=models.CASCADE)
    attribute_name = models.CharField(max_length=64, verbose_name="Attribute Name", null=True)
    attribute_position = models.PositiveIntegerField(verbose_name="Attribute Position", null=True)
    attribute_data_type = models.CharField(max_length=32, verbose_name="Attribute Data Type", null=True)
    attribute_date_format = models.CharField(max_length=32, verbose_name="Attribute Date Format", null=True)
    attribute_pattern = models.CharField(max_length=128, verbose_name="Attribute Pattern", null=True)
    attribute_enums = models.JSONField(verbose_name="Attribute Enums", null=True)
    attribute_min_length = models.PositiveIntegerField(verbose_name="Attribute Minimum Length", null=True)
    attribute_max_length = models.PositiveIntegerField(verbose_name="Attribute Maximum Length", null=True)
    attribute_formula = models.CharField(max_length=128, verbose_name="Attribute Formula", null=True)
    attribute_reference_field = models.CharField(max_length=64, verbose_name="Attribute Reference Field", null=True)
    is_validate = models.BooleanField(default=False, verbose_name="To be Validated or not")
    is_required = models.BooleanField(default=True, verbose_name="Required ?")
    is_unique = models.BooleanField(default=False, verbose_name="Unique ?")
    is_editable = models.BooleanField(default=False, verbose_name="Editable ?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="Business Module - User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")



class SourceQueries(models.Model):
    class Meta:
        db_table = "source_queries"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_sources = models.ForeignKey(MasterSources, verbose_name="MasterSources Id (Auto Generated)", on_delete=models.CASCADE)
    source_key = models.CharField(max_length=64, verbose_name="Source Setting", null=True)
    source_value = models.TextField(verbose_name="Source Key", null=True)
    description = models.TextField(verbose_name="Setting Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="Business Module - User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


# Models For Aggregators

class MasterAggregators(models.Model):
    class Meta:
        db_table = "m_aggregators"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    aggregator_code = models.CharField(max_length=64, verbose_name="Aggregator Code", null=True)
    aggregator_name = models.CharField(max_length=64,verbose_name="Aggregator Name", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class MasterAggregatorsDetails(models.Model):
    class Meta:
        db_table = "m_aggregator_details"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_aggregator = models.ForeignKey(MasterAggregators, verbose_name="MasterAggregator Id (Auto Generated)", on_delete=models.CASCADE)
    m_sources = models.ForeignKey(MasterSources, verbose_name="MasterSources Id (Auto Generated)", on_delete=models.CASCADE)
    is_base_source = models.BooleanField(default=True, verbose_name="Base ?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class AggregatorsQueries(models.Model):
    class Meta:
        db_table = "aggregator_queries"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_aggregator = models.ForeignKey(MasterAggregators, verbose_name="MasterAggregator Id (Auto Generated)", on_delete=models.CASCADE)
    key = models.CharField(max_length=64, verbose_name="Aggregator Setting", null=True)
    value = models.TextField(verbose_name="Aggregator Key", null=True)
    description = models.TextField(verbose_name="Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


# Models For Transformations

class MasterTransformations(models.Model):
    class Meta:
        db_table = "m_transformations"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    transformation_name  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True)
    transformation_description = models.TextField(verbose_name="Transformation Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class MasterTransformationsOperators(models.Model):
    class Meta:
        db_table = "m_transformation_operators"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    operator_key  = models.CharField(max_length=64, verbose_name="Operator Key", null=True)
    operator_value = models.JSONField(verbose_name="Operator Value", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class TransformationsFields(models.Model):
    class Meta:
        db_table = "transformation_fields"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_transformations = models.ForeignKey(MasterTransformations, verbose_name="MasterTransformations Id (Auto Generated)", on_delete=models.CASCADE)
    transformation_name  = models.CharField(max_length=64, verbose_name="Setting Key", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")



class FieldExtraction(models.Model):
    class Meta:
        db_table = "field_extraction"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    transformation_fields_id = models.PositiveIntegerField(default=0,verbose_name="Transformation Field Id(m_transformations)")
    m_sources_id = models.PositiveIntegerField(default=0,verbose_name="Entities Id (Business Module - Entities Id)")
    m_aggregators_id = models.PositiveIntegerField(default=0,verbose_name="Agggregator Id(m_aggregator_id)")
    m_source_definition_id = models.PositiveIntegerField(default=0,verbose_name="Source Definition Id (m_source_definiton_id)")
    attribute_name = models.CharField(max_length=64, verbose_name="Attribute Name", null=True)
    pattern_type = models.CharField(max_length=16, verbose_name="Attribute Type", null=True)
    pattern_input = models.CharField(max_length=32, verbose_name="Attribute Data Type", null=True)
    transaction_placed = models.CharField(max_length=32, verbose_name="Attribute Date Format", null=True)
    transaction_reference = models.CharField(max_length=128, verbose_name="Attribute Pattern", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="Business Module - User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


class LookupExtraction(models.Model):
    class Meta:
        db_table = "lookup_extraction"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    transformation_fields = models.ForeignKey(TransformationsFields, verbose_name="Transformations Fields Id (Auto Generated)", on_delete=models.CASCADE)
    m_aggregator = models.ForeignKey(MasterAggregators, verbose_name="Master Aggregator Id (Auto Generated)", on_delete=models.CASCADE)
    sequence = models.PositiveIntegerField(verbose_name="Sequence", null=True)
    base_source = models.ForeignKey(MasterSources, verbose_name="MasterSources Id (Auto Generated)",
                                     related_name="base_source", on_delete=models.CASCADE)
    base_source_name = models.CharField(max_length=64, verbose_name="Base Source Name", null=True)
    lookup_field = models.ForeignKey(MasterSourceDefinitions, verbose_name="MasterSourcesDefinition Id (Auto Generated)",
                                     related_name="lookup_field", on_delete=models.CASCADE)
    lookup_field_name = models.CharField(max_length=64,verbose_name="Lookup Field Name", null=True)
    extraction_source = models.ForeignKey(MasterSources, verbose_name="MasterSources Id (Auto Generated)",
                                     on_delete=models.CASCADE)
    extraction_source_name = models.CharField(max_length=64, verbose_name="Extraction Source Name", null=True)
    extraction_field = models.ForeignKey(MasterSourceDefinitions, verbose_name="MasterSourcesDefinition Id (Auto Generated)",
                                     related_name="extraction_field", on_delete=models.CASCADE)
    extraction_field_name = models.CharField(max_length=64, verbose_name="Extraction Field Name", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="Business Module - User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")




#Processing Layer Models

class MasterProcessingLayer(models.Model):
    class Meta:
        db_table = "m_processing_layer"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    processing_layer_code  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True)
    processing_layer_name = models.CharField(max_length=64,verbose_name="Transformation Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


class MasterProcessingSubLayer(models.Model):
    class Meta:
        db_table = "m_processing_sub_layer"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    sub_layer_code  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True)
    sub_layer_name = models.CharField(max_length=64,verbose_name="Transformation Description", null=True)
    m_processing_layer = models.ForeignKey(MasterProcessingLayer, verbose_name="MasterProcessingLayer Id (Auto Generated)",
                                     on_delete=models.CASCADE)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


class ProcessingLayer(models.Model):
    class Meta:
        db_table = "processing_layer"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    name  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True,unique=True)
    m_processing_layer_id = models.PositiveIntegerField(default=0,verbose_name="MasterProcessingLayer Id (Auto Generated)" )
    m_processing_sub_layer_id = models.PositiveIntegerField(default=0,verbose_name="MasterProcessingSubLayer Id (Auto Generated)")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class ProcessingLayerDefinition(models.Model):
    class Meta:
        db_table = "processing_layer_definition"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    side = models.CharField(max_length=64, verbose_name="Transformation Name", null=True)
    side_name  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True,unique=True)
    processing_layer_id = models.PositiveIntegerField(default=0,verbose_name="ProcessingLayer Id (Auto Generated)")
    m_aggregators_id = models.PositiveIntegerField(default=0,verbose_name="MasterAggregator Id (Auto Generated)")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")


#BusinessLayer

class MasterRuleCategories(models.Model):
    class Meta:
        db_table = "m_rule_categories"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    rule_category  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True)
    rule_category_description = models.TextField(verbose_name="Transformation Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class MasterRuleType(models.Model):
    class Meta:
        db_table = "m_rule_type"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    rule_type  = models.CharField(max_length=64, verbose_name="Transformation Name", null=True)
    rule_type_description = models.CharField(max_length=64,verbose_name="Transformation Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class BusinessLayer(models.Model):
    class Meta:
        db_table = "business_layer"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    processing_layer_id = models.PositiveIntegerField (verbose_name="ProcessingLayer Id (Auto Generated)" )
    processing_layer_name  = models.CharField(max_length=64, verbose_name="ProcessingLayer Name")
    rule_set_name = models.CharField(max_length=64, verbose_name="RuleSet Name", null=True)
    m_rule_categories = models.ForeignKey(MasterRuleCategories, verbose_name="RuleCategory Id (Auto Generated)",
                                     on_delete=models.CASCADE)
    execution_sequence = models.PositiveIntegerField(verbose_name="RuleCategory Name")
    execution_sub_sequence = models.PositiveIntegerField(verbose_name="RuleCategory Name")
    rule_name = models.CharField(max_length=64, verbose_name="Rule Name", null=True)
    rule_description = models.TextField(verbose_name="Rule Name", null=True)
    rule_type = models.ForeignKey(MasterRuleType, verbose_name="RuleType Id (Auto Generated)",
                                     on_delete=models.CASCADE)
    match_type = models.CharField(max_length=64, verbose_name="Rule Name", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class BusinessLayerDefinition(models.Model):
    class Meta:
        db_table = "business_layer_definition"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    business_layer_id= models.PositiveIntegerField(default=0,verbose_name="BusinessLayer Id (Auto Generated)")
    processing_layer_id = models.PositiveIntegerField(default=0,verbose_name="ProcessingLayer Id (Auto Generated)")
    processing_layer_name = models.CharField(max_length=64, verbose_name="PL Side Name")
    rule_set_name= models.CharField(max_length=64, verbose_name="Rule Set Name")
    rule_name= models.CharField(max_length=64, verbose_name="Rule Name")
    factor_type= models.CharField(max_length=64, verbose_name="Date Name")
    internal_field = models.CharField(max_length=64, verbose_name="Rule Name")
    external_field = models.CharField(max_length=64, verbose_name="Rule Name")
    condition = models.CharField(max_length=64, verbose_name="Rule Name")
    date_tolerance = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    amount_tolerance = models.DecimalField(max_digits=20,  decimal_places=10,verbose_name="Tenants Id (Business Module - Tenant Id)")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class TypeConnections(models.Model):
    class Meta:
        db_table = "type_connections"

    id = models.AutoField(primary_key=True)
    types_id = models.PositiveIntegerField(verbose_name="Sources - Types Id (Auto Generated)", null=True)
    processing_layer = models.ForeignKey(ProcessingLayer, verbose_name="Processing Layer Id (Auto Generated)", on_delete=models.CASCADE)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")