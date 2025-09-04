from django.db import migrations, models

def update_pu_to_pr(apps, schema_editor):
    Device = apps.get_model('db', 'device')
    Device.objects.filter(device_type='PU', device_role='IN').update(device_type='PR', device_role='PU')

class Migration(migrations.Migration):

    dependencies = [
        ('db', '0009_link_dst_if_role_link_src_if_role'),
    ]

    operations = [
        migrations.RunPython(update_pu_to_pr),
    ]
