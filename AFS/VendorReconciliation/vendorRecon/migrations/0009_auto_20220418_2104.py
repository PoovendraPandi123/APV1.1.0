# Generated by Django 3.1.7 on 2022-04-18 15:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vendorRecon', '0008_auto_20220418_1850'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='recoexecutionlog',
            name='end_time',
        ),
        migrations.RemoveField(
            model_name='recoexecutionlog',
            name='error_message',
        ),
        migrations.RemoveField(
            model_name='recoexecutionlog',
            name='error_state',
        ),
        migrations.RemoveField(
            model_name='recoexecutionlog',
            name='file_uploads_id',
        ),
        migrations.RemoveField(
            model_name='recoexecutionlog',
            name='procedure_name',
        ),
        migrations.RemoveField(
            model_name='recoexecutionlog',
            name='start_time',
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='duration',
            field=models.PositiveIntegerField(null=True, verbose_name='Duration'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='end_date',
            field=models.CharField(max_length=64, null=True, verbose_name='End Date'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='entities_id',
            field=models.PositiveIntegerField(null=True, verbose_name='Entities Id'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='groups_id',
            field=models.PositiveIntegerField(null=True, verbose_name='Groups Id'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='m_processing_layer_id',
            field=models.PositiveIntegerField(null=True, verbose_name='M Processing Layer Id'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='m_processing_sub_layer_id',
            field=models.PositiveIntegerField(null=True, verbose_name='M Processing Sub Layer Id'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='processing_layer_id',
            field=models.PositiveIntegerField(null=True, verbose_name='Processing Layer Id'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='start_date',
            field=models.CharField(max_length=64, null=True, verbose_name='Start Date'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='status',
            field=models.TextField(null=True, verbose_name='Status'),
        ),
        migrations.AddField(
            model_name='recoexecutionlog',
            name='tenants_id',
            field=models.PositiveIntegerField(null=True, verbose_name='Tenants Id'),
        ),
    ]
