# Generated by Django 3.1.7 on 2022-01-21 14:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('alcs', '0002_masterclientdetails'),
    ]

    operations = [
        migrations.CreateModel(
            name='ExternalRecords',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('tenants_id', models.PositiveIntegerField(verbose_name='Tenants Id (Business Module - Tenant Id)')),
                ('groups_id', models.PositiveIntegerField(verbose_name='Groups Id (Business Module - Groups Id)')),
                ('entities_id', models.PositiveIntegerField(verbose_name='Entities Id (Business Module - Entities Id)')),
                ('m_processing_layer_id', models.PositiveIntegerField(verbose_name='M Processing Layer Id (Business Module - M Processing Layer Id)')),
                ('m_processing_sub_layer_id', models.PositiveIntegerField(verbose_name='M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)')),
                ('processing_layer_id', models.PositiveIntegerField(verbose_name='Processing Layer Id (Business Module - Processing Layer Id)')),
                ('processing_layer_name', models.CharField(max_length=64, null=True, verbose_name='Processing Layer Name')),
                ('ext_reference_text_1', models.TextField(null=True, verbose_name='Ext Reference Text 1')),
                ('ext_reference_text_2', models.TextField(null=True, verbose_name='Ext Reference Text 2')),
                ('ext_reference_text_3', models.TextField(null=True, verbose_name='Ext Reference Text 3')),
                ('ext_reference_text_4', models.TextField(null=True, verbose_name='Ext Reference Text 4')),
                ('ext_reference_text_5', models.TextField(null=True, verbose_name='Ext Reference Text 5')),
                ('ext_reference_text_6', models.TextField(null=True, verbose_name='Ext Reference Text 6')),
                ('ext_reference_text_7', models.TextField(null=True, verbose_name='Ext Reference Text 7')),
                ('ext_reference_text_8', models.TextField(null=True, verbose_name='Ext Reference Text 8')),
                ('ext_reference_text_9', models.TextField(null=True, verbose_name='Ext Reference Text 9')),
                ('ext_reference_text_10', models.TextField(null=True, verbose_name='Ext Reference Text 10')),
                ('ext_reference_text_11', models.TextField(null=True, verbose_name='Ext Reference Text 11')),
                ('ext_reference_text_12', models.TextField(null=True, verbose_name='Ext Reference Text 12')),
                ('ext_reference_text_13', models.TextField(null=True, verbose_name='Ext Reference Text 13')),
                ('ext_reference_text_14', models.TextField(null=True, verbose_name='Ext Reference Text 14')),
                ('ext_reference_text_15', models.TextField(null=True, verbose_name='Ext Reference Text 15')),
                ('ext_reference_text_16', models.TextField(null=True, verbose_name='Ext Reference Text 16')),
                ('ext_reference_text_17', models.TextField(null=True, verbose_name='Ext Reference Text 17')),
                ('ext_reference_text_18', models.TextField(null=True, verbose_name='Ext Reference Text 18')),
                ('ext_reference_text_19', models.TextField(null=True, verbose_name='Ext Reference Text 19')),
                ('ext_reference_text_20', models.TextField(null=True, verbose_name='Ext Reference Text 20')),
                ('ext_reference_text_21', models.TextField(null=True, verbose_name='Ext Reference Text 21')),
                ('ext_reference_text_22', models.TextField(null=True, verbose_name='Ext Reference Text 22')),
                ('ext_reference_text_23', models.TextField(null=True, verbose_name='Ext Reference Text 23')),
                ('ext_reference_text_24', models.TextField(null=True, verbose_name='Ext Reference Text 24')),
                ('ext_reference_text_25', models.TextField(null=True, verbose_name='Ext Reference Text 25')),
                ('ext_reference_date_time_1', models.DateTimeField(null=True, verbose_name='Ext Reference Date Time 1')),
                ('ext_reference_date_time_2', models.DateTimeField(null=True, verbose_name='Ext Reference Date Time 2')),
                ('ext_reference_date_time_3', models.DateTimeField(null=True, verbose_name='Ext Reference Date Time 3')),
                ('ext_reference_date_time_4', models.DateTimeField(null=True, verbose_name='Ext Reference Date Time 4')),
                ('ext_reference_date_time_5', models.DateTimeField(null=True, verbose_name='Ext Reference Date Time 5')),
                ('ext_amount_1', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 1')),
                ('ext_amount_2', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 2')),
                ('ext_amount_3', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 3')),
                ('ext_amount_4', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 4')),
                ('ext_amount_5', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 5')),
                ('ext_amount_6', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 6')),
                ('ext_amount_7', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 7')),
                ('ext_amount_8', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 8')),
                ('ext_amount_9', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 9')),
                ('ext_amount_10', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Ext Amount 10')),
                ('ext_generated_num_1', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 1')),
                ('ext_generated_num_2', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 2')),
                ('ext_generated_num_3', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 3')),
                ('ext_generated_num_4', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 4')),
                ('ext_generated_num_5', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 5')),
                ('ext_generated_num_6', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 6')),
                ('ext_generated_num_7', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 7')),
                ('ext_generated_num_8', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 8')),
                ('ext_generated_num_9', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 9')),
                ('ext_generated_num_10', models.PositiveIntegerField(null=True, verbose_name='Ext Generated Number 10')),
                ('is_active', models.BooleanField(default=True, verbose_name='Active ?')),
                ('created_by', models.PositiveSmallIntegerField(null=True, verbose_name='User Id')),
                ('created_date', models.DateTimeField(auto_now_add=True, verbose_name='Created Date')),
                ('modified_by', models.PositiveSmallIntegerField(null=True, verbose_name='User Id')),
                ('modified_date', models.DateTimeField(auto_now=True, verbose_name='Modified Date')),
            ],
            options={
                'db_table': 't_external_records',
            },
        ),
        migrations.CreateModel(
            name='InternalRecords',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('tenants_id', models.PositiveIntegerField(verbose_name='Tenants Id (Business Module - Tenant Id)')),
                ('groups_id', models.PositiveIntegerField(verbose_name='Groups Id (Business Module - Groups Id)')),
                ('entities_id', models.PositiveIntegerField(verbose_name='Entities Id (Business Module - Entities Id)')),
                ('m_processing_layer_id', models.PositiveIntegerField(verbose_name='M Processing Layer Id (Business Module - M Processing Layer Id)')),
                ('m_processing_sub_layer_id', models.PositiveIntegerField(verbose_name='M Processing Sub Layer Id (Business Module - M Processing Sub Layer Id)')),
                ('processing_layer_id', models.PositiveIntegerField(verbose_name='Processing Layer Id (Business Module - Processing Layer Id)')),
                ('processing_layer_name', models.CharField(max_length=64, null=True, verbose_name='Processing Layer Name')),
                ('int_reference_text_1', models.TextField(null=True, verbose_name='Int Reference Text 1')),
                ('int_reference_text_2', models.TextField(null=True, verbose_name='Int Reference Text 2')),
                ('int_reference_text_3', models.TextField(null=True, verbose_name='Int Reference Text 3')),
                ('int_reference_text_4', models.TextField(null=True, verbose_name='Int Reference Text 4')),
                ('int_reference_text_5', models.TextField(null=True, verbose_name='Int Reference Text 5')),
                ('int_reference_text_6', models.TextField(null=True, verbose_name='Int Reference Text 6')),
                ('int_reference_text_7', models.TextField(null=True, verbose_name='Int Reference Text 7')),
                ('int_reference_text_8', models.TextField(null=True, verbose_name='Int Reference Text 8')),
                ('int_reference_text_9', models.TextField(null=True, verbose_name='Int Reference Text 9')),
                ('int_reference_text_10', models.TextField(null=True, verbose_name='Int Reference Text 10')),
                ('int_reference_text_11', models.TextField(null=True, verbose_name='Int Reference Text 11')),
                ('int_reference_text_12', models.TextField(null=True, verbose_name='Int Reference Text 12')),
                ('int_reference_text_13', models.TextField(null=True, verbose_name='Int Reference Text 13')),
                ('int_reference_text_14', models.TextField(null=True, verbose_name='Int Reference Text 14')),
                ('int_reference_text_15', models.TextField(null=True, verbose_name='Int Reference Text 15')),
                ('int_reference_text_16', models.TextField(null=True, verbose_name='Int Reference Text 16')),
                ('int_reference_text_17', models.TextField(null=True, verbose_name='Int Reference Text 17')),
                ('int_reference_text_18', models.TextField(null=True, verbose_name='Int Reference Text 18')),
                ('int_reference_text_19', models.TextField(null=True, verbose_name='Int Reference Text 19')),
                ('int_reference_text_20', models.TextField(null=True, verbose_name='Int Reference Text 20')),
                ('int_reference_text_21', models.TextField(null=True, verbose_name='Int Reference Text 21')),
                ('int_reference_text_22', models.TextField(null=True, verbose_name='Int Reference Text 22')),
                ('int_reference_text_23', models.TextField(null=True, verbose_name='Int Reference Text 23')),
                ('int_reference_text_24', models.TextField(null=True, verbose_name='Int Reference Text 24')),
                ('int_reference_text_25', models.TextField(null=True, verbose_name='Int Reference Text 25')),
                ('int_reference_date_time_1', models.DateTimeField(null=True, verbose_name='Int Reference Date Time 1')),
                ('int_reference_date_time_2', models.DateTimeField(null=True, verbose_name='Int Reference Date Time 2')),
                ('int_reference_date_time_3', models.DateTimeField(null=True, verbose_name='Int Reference Date Time 3')),
                ('int_reference_date_time_4', models.DateTimeField(null=True, verbose_name='Int Reference Date Time 4')),
                ('int_reference_date_time_5', models.DateTimeField(null=True, verbose_name='Int Reference Date Time 5')),
                ('int_amount_1', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 1')),
                ('int_amount_2', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 2')),
                ('int_amount_3', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 3')),
                ('int_amount_4', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 4')),
                ('int_amount_5', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 5')),
                ('int_amount_6', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 6')),
                ('int_amount_7', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 7')),
                ('int_amount_8', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 8')),
                ('int_amount_9', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 9')),
                ('int_amount_10', models.DecimalField(decimal_places=2, max_digits=15, null=True, verbose_name='Int Amount 10')),
                ('int_generated_num_1', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 1')),
                ('int_generated_num_2', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 2')),
                ('int_generated_num_3', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 3')),
                ('int_generated_num_4', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 4')),
                ('int_generated_num_5', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 5')),
                ('int_generated_num_6', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 6')),
                ('int_generated_num_7', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 7')),
                ('int_generated_num_8', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 8')),
                ('int_generated_num_9', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 9')),
                ('int_generated_num_10', models.PositiveIntegerField(null=True, verbose_name='Int Generated Number 10')),
                ('is_active', models.BooleanField(default=True, verbose_name='Active ?')),
                ('created_by', models.PositiveSmallIntegerField(null=True, verbose_name='User Id')),
                ('created_date', models.DateTimeField(auto_now_add=True, verbose_name='Created Date')),
                ('modified_by', models.PositiveSmallIntegerField(null=True, verbose_name='User Id')),
                ('modified_date', models.DateTimeField(auto_now=True, verbose_name='Modified Date')),
            ],
            options={
                'db_table': 't_internal_records',
            },
        ),
    ]
