from datetime import datetime

from django.db import models


class SimpleModel(models.Model):
    char = models.CharField(max_length=64)
    datetime = models.DateTimeField(default=datetime.now)


class NonAutoPrimaryKeyModel(models.Model):
    id = models.IntegerField(primary_key=True)


class PartialUpdateModel(models.Model):
    """ For partial update checks
    """
    f1 = models.IntegerField()
    f2 = models.IntegerField()


class FkSimpleModel(models.Model):
    fk = models.ForeignKey(SimpleModel, on_delete=models.CASCADE)
    char = models.CharField(max_length=64, blank=True, null=True)


class ManyToManyModel(models.Model):
    """ For m2m add/delete tests
    """
    m2m = models.ManyToManyField(SimpleModel)
