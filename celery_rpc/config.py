# coding: utf-8
from __future__ import absolute_import
import json

from django.conf import settings as _settings
from kombu.serialization import registry

from .encoders import XJSONEncoder


# Register enhanced json encoder
def _json_dumps(obj):
    return json.dumps(obj, cls=XJSONEncoder)

registry.register('x-json', _json_dumps, json.loads, 'application/json', 'utf-8')

DEFAULT_FILTER_LIMIT = 1000

# See Celery configuration parameters at
# http://docs.celeryproject.org/en/latest/configuration.html
# Some reasonable defaults are defined below

CELERY_RESULT_BACKEND = 'cache+memory://'

CELERY_DEFAULT_QUEUE = 'celery_rpc.requests'
CELERY_DEFAULT_EXCHANGE = 'celery_rpc'
CELERY_DEFAULT_ROUTING_KEY = 'celery_rpc'

# Do not let skip messages silently (RabbitMQ)
BROKER_TRANSPORT_OPTIONS = {'confirm_publish': True}

CELERY_ACKS_LATE = True
CELERY_TASK_SERIALIZER = 'x-json'
CELERY_RESULT_SERIALIZER = 'x-json'

CELERYD_TASK_SOFT_TIME_LIMIT = 10
CELERYD_TASK_TIME_LIMIT = 60

# Options can be overridden by CELERY_RPC_CONFIG dict in Django settings.py
_CONFIG = getattr(_settings, 'CELERY_RPC_CONFIG', {})

locals().update(_CONFIG)