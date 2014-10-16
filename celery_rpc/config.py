# coding: utf-8
from __future__ import absolute_import
import json
try:
    from django.conf import settings as _settings
except ImportError:
    # No need django for celery_rpc client
    _settings = object()

from kombu.serialization import registry, bytes_t

from .encoders import XJSONEncoder
from .decoders import x_json_decoder_object_hook


# Register enhanced json encoder
def _json_dumps(obj):
    return json.dumps(obj, cls=XJSONEncoder)

def _json_loads(obj):
    if isinstance(obj, bytes_t):
        obj = obj.decode()
    return json.loads(obj, object_hook=x_json_decoder_object_hook)


registry.register('x-rpc-json', _json_dumps, _json_loads, 'application/json', 'utf-8')

# Default limit for results of filter call
FILTER_LIMIT = 1000
# Default timeout for getting results
GET_RESULT_TIMEOUT = 10

# Provide ability to change base task class for celery-rpc server tasks.
# Example: { 'ModelChangeTask': my.own.ModelChangeTask }
# Key - symbolic class name, value - class with suitable interface.
# Do it on your own risk!
OVERRIDE_BASE_TASKS = {}

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
CELERY_TASK_SERIALIZER = 'x-rpc-json'
CELERY_RESULT_SERIALIZER = 'x-rpc-json'


# Options can be overridden by CELERY_RPC_CONFIG dict in Django settings.py
_CONFIG = getattr(_settings, 'CELERY_RPC_CONFIG', {})

locals().update(_CONFIG)

CELERYD_TASK_SOFT_TIME_LIMIT = GET_RESULT_TIMEOUT + 1
CELERYD_TASK_TIME_LIMIT = GET_RESULT_TIMEOUT * 2

