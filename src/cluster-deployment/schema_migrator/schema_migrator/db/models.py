from django.db import models


class DeploymentProfile(models.Model):
    profile_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=128)
    cluster_name = models.CharField(max_length=128)
    deployable = models.BooleanField(default=True)


class Device(models.Model):
    device_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=128)
    device_type = models.CharField(max_length=2)
    device_role = models.CharField(max_length=2, default="")
    deployment_stage = models.IntegerField(null=True, default=None)
    deployment_status = models.IntegerField(null=True, default=0)
    deployment_msg = models.CharField(max_length=1024, default="")
    profile = models.ForeignKey(DeploymentProfile, on_delete=models.CASCADE, related_name='devices')
    deleted = models.BooleanField(default=False)
    active = models.BooleanField(default=True)
    swapped_with = models.CharField(max_length=128, default="")


class ProfileDeploymentStage(models.Model):
    stage_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=32)
    status = models.IntegerField(default=0)
    section = models.IntegerField(default=0)


class DeploymentCtx(models.Model):
    ctx_id = models.AutoField(primary_key=True)
    profile = models.ForeignKey(DeploymentProfile, on_delete=models.CASCADE)
    current_stage = models.ForeignKey(ProfileDeploymentStage, default=None, null=True, on_delete=models.CASCADE)
    status = models.IntegerField(null=True, default=None)


class DeviceProperty(models.Model):
    prop_id = models.AutoField(primary_key=True)
    property_name = models.CharField(max_length=256)
    property_attribute = models.CharField(max_length=64)
    property_value = models.CharField(max_length=128)
    device = models.ForeignKey(Device, on_delete=models.CASCADE, related_name='properties')

    class Meta:
        unique_together = ('device', 'property_name', 'property_attribute')


class HealthStatus(models.Model):
    id = models.AutoField(primary_key=True)
    attr = models.CharField(max_length=256)
    status = models.CharField(max_length=16)
    message = models.CharField(max_length=256, default="")
    detail = models.TextField(default="")
    device = models.ForeignKey(Device, on_delete=models.CASCADE, related_name='health_records')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
