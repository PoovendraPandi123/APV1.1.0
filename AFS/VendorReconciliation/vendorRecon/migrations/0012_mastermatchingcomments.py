# Generated by Django 3.1.7 on 2022-04-26 03:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vendorRecon', '0011_auto_20220421_2041'),
    ]

    operations = [
        migrations.CreateModel(
            name='MasterMatchingComments',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('tenants_id', models.PositiveIntegerField(null=True, verbose_name='Tenants Id (Business Module - Tenant Id)')),
                ('groups_id', models.PositiveIntegerField(null=True, verbose_name='Groups Id (Business Module - Groups Id)')),
                ('entities_id', models.PositiveIntegerField(null=True, verbose_name='Entities Id (Business Module - Entities Id)')),
                ('m_processing_layer_id', models.PositiveIntegerField(null=True, verbose_name='M Processing Layer Id')),
                ('m_processing_sub_layer_id', models.PositiveIntegerField(null=True, verbose_name='M Processing Sub Layer Id')),
                ('processing_layer_id', models.PositiveIntegerField(null=True, verbose_name='Processing Layer Id')),
                ('name', models.CharField(max_length=256, null=True, verbose_name='Name')),
                ('description', models.TextField(null=True, verbose_name='Description')),
                ('is_active', models.BooleanField(default=True, verbose_name='Active ?')),
                ('created_by', models.PositiveSmallIntegerField(null=True, verbose_name='User Id')),
                ('created_date', models.CharField(max_length=64, null=True, verbose_name='Created Date')),
                ('modified_by', models.PositiveSmallIntegerField(null=True, verbose_name='User Id')),
                ('modified_date', models.DateTimeField(max_length=64, null=True, verbose_name='Modified Date')),
            ],
            options={
                'db_table': 'm_matching_comments',
            },
        ),
    ]
