# Generated by Django 3.1.7 on 2022-01-23 14:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('alcs', '0003_recosettings'),
    ]

    operations = [
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_1',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 1'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_10',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 10'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_2',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 2'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_3',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 3'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_4',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 4'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_5',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 5'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_6',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 6'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_7',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 7'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_8',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 8'),
        ),
        migrations.AddField(
            model_name='externalrecords',
            name='ext_extracted_text_9',
            field=models.CharField(max_length=512, null=True, verbose_name='Ext Extracted Text 9'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_1',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 1'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_10',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 10'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_2',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 2'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_3',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 3'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_4',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 4'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_5',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 5'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_6',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 6'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_7',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 7'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_8',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 8'),
        ),
        migrations.AddField(
            model_name='internalrecords',
            name='int_extracted_text_9',
            field=models.CharField(max_length=512, null=True, verbose_name='Int Extracted Text 9'),
        ),
    ]
