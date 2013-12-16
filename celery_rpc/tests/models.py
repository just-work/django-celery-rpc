# coding: utf-8
from datetime import datetime

from django.db import models


class SimpleModel(models.Model):
    char = models.CharField(max_length=64)
    datetime = models.DateTimeField(default=datetime.now)