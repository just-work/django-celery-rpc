from __future__ import absolute_import

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')

from celery import Celery
from django.conf import settings


def create_celery_app(config=None, **opts):
    opts.setdefault('main', 'celery-rpc')
    app = Celery(**opts)
    app.config_from_object('celery_rpc.config')
    if config:
        app.conf.update(config)
    return app

rpc = create_celery_app()

rpc.autodiscover_tasks(['celery_rpc'])
rpc.autodiscover_tasks(lambda: settings.INSTALLED_APPS,
                       related_name="celery_rpc")