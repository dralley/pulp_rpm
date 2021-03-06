# Generated by Django 2.2.10 on 2020-03-18 14:23

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0022_rename_last_version'),
        ('rpm', '0004_add_metadata_signing_service_fk'),
    ]

    operations = [
        migrations.AddField(
            model_name='rpmrepository',
            name='last_sync_remote',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='rpm_rpmrepository', to='core.Remote'),
        ),
        migrations.AddField(
            model_name='rpmrepository',
            name='last_sync_repo_version',
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name='rpmrepository',
            name='last_sync_revision_number',
            field=models.CharField(max_length=20, null=True),
        ),
    ]
