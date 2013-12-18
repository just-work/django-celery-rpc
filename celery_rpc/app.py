from __future__ import absolute_import
import os

from django.conf import settings

from .utils import create_celery_app

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')

rpc = create_celery_app()
rpc.autodiscover_tasks(['celery_rpc'])
rpc.autodiscover_tasks(lambda: settings.INSTALLED_APPS,
                       related_name="celery_rpc")