# coding: utf-8
from __future__ import absolute_import

try:
    from django.conf import settings as _settings
except ImportError:
    # No need django for celery_rpc client
    _settings = object()

# Default limit for results of filter call
FILTER_LIMIT = 1000

# Default timeout for getting results
GET_RESULT_TIMEOUT = 10

# Pass exceptions from server to client as instances if true.
# By default exceptions are passed as a string.
WRAP_REMOTE_ERRORS = False

# Provide ability to change base task class for celery-rpc server tasks.
# Example: { 'ModelChangeTask': my.own.ModelChangeTask }
# Key - symbolic class name, value - class with suitable interface.
# Do it on your own risk!
OVERRIDE_BASE_TASKS = {}

# default celery rpc client name which will be passed as referer header
RPC_CLIENT_NAME = "celery_rpc_client"

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
CELERY_ACCEPT_CONTENT = ['json', 'x-json', 'x-rpc-json']
CELERY_TASK_SERIALIZER = 'x-json'
CELERY_RESULT_SERIALIZER = 'x-json'

# Options can be overridden by CELERY_RPC_CONFIG dict in Django settings.py
_CONFIG = getattr(_settings, 'CELERY_RPC_CONFIG', {})

locals().update(_CONFIG)

CELERYD_TASK_SOFT_TIME_LIMIT = GET_RESULT_TIMEOUT + 1
CELERYD_TASK_TIME_LIMIT = GET_RESULT_TIMEOUT * 2

_codecs_registered = False
if not _codecs_registered:
    from .codecs import register_codecs

    register_codecs()
    _codecs_registered = True
