# Generated by Django 2.2.12 on 2020-07-23 11:33

from django.db import migrations, models
import django.db.models.deletion


def unset_main_repo_fk(apps, schema_editor):
    """
    No longer have a Foreign Key pointing to a main repo.
    The only way to determine that it's a main repo is to look at the packages directory path.
    """
    Variant = apps.get_model('rpm', 'Variant')
    Variant.objects.filter(packages='Packages').update(repository=None)


class Migration(migrations.Migration):

    dependencies = [
        ('rpm', '0015_repo_metadata'),
    ]

    operations = [
        migrations.AlterField(
            model_name='variant',
            name='repository',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, related_name='+', to='core.Repository'),
        ),
        migrations.RunPython(unset_main_repo_fk)
    ]
