# coding: utf-8
from datetime import datetime

from django.db import models


class SimpleModel(models.Model):
    char = models.CharField()
    datetime = models.DateTimeField(default=datetime.now)