# Generated by Django 3.1.7 on 2022-01-03 15:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('tasks', '0003_auto_20220103_0342'),
    ]

    operations = [
        migrations.AlterField(
            model_name='jobexecutions',
            name='file_ids',
            field=models.TextField(verbose_name='File Id (Process Module - File Uploads - File Id (Auto Field)'),
        ),
    ]
