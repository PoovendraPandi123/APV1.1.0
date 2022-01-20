from django.db import models

# Create your models here.

class FileUploads(models.Model):
    class Meta:
        db_table = "file_uploads"

    id = models.AutoField(primary_key=True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_source_id = models.PositiveIntegerField(verbose_name="Source Id (Business Module - Source Id)")
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Business Module - M Processing Layer Id)")
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)")
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Business Module - Processing Layer Id)")
    processing_layer_name = models.CharField(max_length=64, verbose_name="Processing Layer Name", null=True)
    source_type = models.CharField(max_length=128, verbose_name="File Name", null=True)
    extraction_type = models.CharField(max_length=128, verbose_name="File Name", null=True)
    file_name = models.CharField(max_length=128, verbose_name="File Name", null=True)
    file_size_bytes = models.PositiveIntegerField(verbose_name="File Size Bytes", null=True)
    file_path = models.CharField(max_length=512, verbose_name="File Path", null=True)
    status = models.CharField(max_length=32, verbose_name="Status", null=True)
    comments = models.TextField(verbose_name="Comments", null=True)
    file_row_count = models.PositiveIntegerField(verbose_name="File Row Count (Business Module - File Row Count)", null=True)
    is_processed = models.BooleanField(default=True, verbose_name="Active ?")
    is_processing = models.PositiveIntegerField(verbose_name="Is Processing?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add = True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now = True, verbose_name="Modified Date")

class MasterClientDetails(models.Model):
    class Meta:
        db_table = "m_client_details"

    id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    client_id = models.CharField(max_length=64, verbose_name="Client ID", null=False)
    client_name = models.CharField(max_length=512, verbose_name="Client Name", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")