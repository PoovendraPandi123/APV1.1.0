# Generated by Django 3.2.2 on 2021-05-27 15:59

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('source', '0014_mastertransformations'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='mastertransformations',
            name='m_sources',
        ),
    ]
