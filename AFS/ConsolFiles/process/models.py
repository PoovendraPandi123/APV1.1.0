from django.db import models
from django.utils import timezone

# Create your models here.

class ModuleSettings(models.Model):
    class Meta:
        db_table = "module_settings"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)", null=False)
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)", null=False)
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    setting_key = models.CharField(max_length=64, verbose_name="Setting Key", null=True)
    setting_value = models.JSONField(verbose_name="Setting Value", null=True)
    setting_description = models.TextField(verbose_name="Setting Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.setting_description

class Sources(models.Model):
    class Meta:
        db_table = "sources"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)", null=False)
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)", null=False)
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    source = models.ForeignKey("self", null=True, on_delete=models.CASCADE)
    source_code = models.CharField(max_length=64, verbose_name="Source Code", null=False, unique=True)
    source_name = models.CharField(max_length=64, verbose_name="Source Name", null=False, unique=True)
    source_config = models.JSONField(verbose_name="Source Configurations")
    source_input_location = models.CharField(max_length=512, verbose_name="Source Input Location", null=True)
    source_archive_location = models.CharField(max_length=512, verbose_name="Source Archive Location", null=True)
    source_error_location = models.CharField(max_length=512, verbose_name="Source Error Location", null=True)
    source_import_location = models.CharField(max_length=512, verbose_name="Source Import Location", null=True)
    source_import_seq = models.PositiveIntegerField(verbose_name="Source Import Sequence", null=True)
    source_field_number = models.PositiveIntegerField(verbose_name="Source Field Number", null=True)
    key_words = models.JSONField(verbose_name="Key Words", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.source_name

    @property
    def source_definitions(self):
        return self.sourcedefinitions_set.all()

class SourceDefinitions(models.Model):
    class Meta:
        db_table = "source_definitions"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)", null=False)
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)", null=False)
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    sources = models.ForeignKey(Sources, verbose_name="MasterSources Id (Auto Generated)", on_delete=models.CASCADE)
    attribute_name = models.CharField(max_length=128, verbose_name="Attribute Name", null=True)
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
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.attribute_name

class TargetFiles(models.Model):
    class Meta:
        db_table = "target_files"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)", null=False)
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)", null=False)
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    name = models.CharField(max_length=128, verbose_name="Name", null=True)
    description = models.TextField(verbose_name="Description", null=True)
    files_config = models.JSONField(verbose_name="Files Configuration", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.name

    @property
    def target_file_definitions(self):
        return self.targetfiledefinitions_set.all()


class TargetFileDefinitions(models.Model):
    class Meta:
        db_table = "target_file_definitions"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)", null=False)
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)", null=False)
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    field_name = models.CharField(max_length=128, verbose_name="Field Name", null=True)
    field_sequence = models.PositiveIntegerField(verbose_name="Field Sequence", null=True)
    files_config = models.JSONField(verbose_name="Files Config", null=True)
    target_files = models.ForeignKey(TargetFiles, verbose_name="Target Files Id (Auto Generated)", on_delete=models.CASCADE)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.field_name

class Reports(models.Model):
    class Meta:
        db_table = "reports"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)", null=False)
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)", null=False)
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    name = models.CharField(max_length=128, verbose_name="Name", null=True)
    description = models.TextField(verbose_name="Description", null=True)
    report_config = models.JSONField(verbose_name="Report Config", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.name

class FileUploads(models.Model):
    class Meta:
        db_table = "file_uploads"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)", null=False)
    m_sources = models.ForeignKey(Sources, verbose_name="Sources Id (Auto Generated)", on_delete=models.CASCADE)
    source_name = models.CharField(max_length=64, verbose_name="Source Name", null=False)
    source_type = models.CharField(max_length=64, verbose_name="Source Type", null=True)
    extraction_type = models.CharField(max_length=64, verbose_name="Extraction Type", null=True)
    file_name = models.CharField(max_length=128, verbose_name="File Name", null=True)
    file_size_bytes = models.PositiveIntegerField(verbose_name="File Size Bytes", null=True)
    status = models.CharField(max_length=32, verbose_name="Status", null=True)
    comments = models.TextField(verbose_name="Comments", null=True)
    file_row_count = models.PositiveIntegerField(verbose_name="File Row Count", null=True)
    is_processed = models.BooleanField(default=False, verbose_name="Processed ?")
    file_path = models.CharField(max_length=512, verbose_name="File Path", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.CharField(max_length=32, verbose_name="Created Date", null=True)
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.CharField(max_length=32, verbose_name="Modified Date", null=True)

    def __str__(self):
        return self.file_name