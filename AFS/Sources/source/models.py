from django.db import models
from django.utils import timezone

# Create your models here.

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
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

    def __str__(self):
        return self.setting_description

class MasterSources(models.Model):
    class Meta:
        db_table = "m_sources"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    sources = models.ForeignKey("self", null=True, on_delete=models.CASCADE)
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
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

    def __str__(self):
        return self.source_name

    @property
    def m_source_definitions(self):
        return self.mastersourcedefinitions_set.all()

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
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

    def __str__(self):
        return self.file_name

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
    attribute_min_length = models.CharField(max_length = 5, verbose_name="Attribute Minimum Length", null=True)
    attribute_max_length = models.CharField(max_length = 5, verbose_name="Attribute Maximum Length", null=True)
    attribute_formula = models.CharField(max_length=128, verbose_name="Attribute Formula", null=True)
    attribute_reference_field = models.CharField(max_length=64, verbose_name="Attribute Reference Field", null=True)
    is_validate = models.BooleanField(default=False, verbose_name="To be Validated or not")
    is_required = models.BooleanField(default=True, verbose_name="Required ?")
    is_unique = models.BooleanField(default=False, verbose_name="Unique ?")
    is_editable = models.BooleanField(default=False, verbose_name="Editable ?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="Business Module - User Id", null=True)
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

    def __str__(self):
        return self.attribute_name

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
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

    def __str__(self):
        return self.aggregator_name

    @property
    def m_aggregator_details(self):
        return self.masteraggregatordetails_set.all()

class MasterAggregatorDetails(models.Model):
    class Meta:
        db_table = "m_aggregator_details"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_aggregator = models.ForeignKey(MasterAggregators, verbose_name="MasterAggregator Id (Auto Generated)", on_delete=models.CASCADE)
    m_sources_id = models.PositiveIntegerField(verbose_name="MasterSources Id")
    is_base_source = models.BooleanField(default=True, verbose_name="Base ?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

class MasterTransformationTypes(models.Model):
    class Meta:
        db_table = "m_transformation_types"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    transformation_name = models.CharField(max_length = 255, verbose_name="Transformation Name")
    transformation_description = models.TextField(verbose_name="Transformation Description")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

    def __str__(self):
        return self.transformation_description

class MasterTransformationOperators(models.Model):
    class Meta:
        db_table = "m_transformation_operators"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    operator_key = models.CharField(max_length=255, verbose_name="Operator Key")
    operator_value = models.JSONField(verbose_name="Operator Value")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

    def __str__(self):
        return self.operator_key

class MasterAggregatorTransformations(models.Model):
    class Meta:
        db_table = "m_aggregator_transformations"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_aggregator_id = models.PositiveIntegerField(verbose_name="M Aggregator Id (Master Aggregator)")
    m_aggregator_transforms = models.JSONField(verbose_name="Aggregator Transforms (Field Extraction, Lookup Extraction, Math Transformations..)")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

class MasterTransformations(models.Model):
    class Meta:
        db_table = "m_transformations"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_transformation_types_id = models.PositiveIntegerField(verbose_name="M Transformation Types Id (MasterTransformationTypes)")
    transformation_name = models.CharField(max_length=255, verbose_name="Transformation Name")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

    def __str__(self):
        return self.transformation_name

    @property
    def field_extraction(self):
        return self.fieldextraction_set.all()

class FieldExtraction(models.Model):
    class Meta:
        db_table = "field_extraction"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_aggregator_id = models.PositiveIntegerField(verbose_name="Aggregator Id (MasterAggregators)")
    m_transformations = models.ForeignKey(MasterTransformations, verbose_name="M Transformations Id (MasterTransformations)", on_delete=models.CASCADE)
    m_sources_id = models.PositiveIntegerField(verbose_name="Sources Id (MasterSources)")
    m_source_definitions_id = models.PositiveIntegerField(verbose_name="Source Definitions Id (MasterSourceDefinitions)")
    pattern_type = models.CharField(max_length=255, verbose_name="Pattern Type")
    pattern_input = models.CharField(max_length=255, verbose_name="Pattern Input")
    transaction_placed = models.CharField(max_length=255, verbose_name="Transaction Placed")
    transaction_reference = models.CharField(max_length=255, verbose_name="Transaction Reference")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")




