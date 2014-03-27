from datetime import datetime

from django.db import models


class SimpleModel(models.Model):
    char = models.CharField(max_length=64)
    datetime = models.DateTimeField(default=datetime.now)


class NonAutoPrimaryKeyModel(models.Model):
    id = models.IntegerField(primary_key=True)