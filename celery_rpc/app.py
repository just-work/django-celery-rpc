# coding: utf-8
from __future__ import absolute_import

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')

from celery import Celery
from django.conf import settings


rpc = Celery('celery-rpc')

rpc.config_from_object('celery_rpc.config')
rpc.autodiscover_tasks(lambda: settings.INSTALLED_APPS,
                       related_name="celery_rpc")