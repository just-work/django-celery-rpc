# coding: utf-8
from __future__ import absolute_import

try:
    from django.conf import settings as _settings
except ImportError:
    # No need django for celery_rpc client
    _settings = object()

# Default limit for results of filter call
filter_limit = 1000

# Default timeout for getting results
get_result_timeout = 10

# Pass exceptions from server to client as instances if true.
# By default exceptions are passed as a string.
wrap_remote_errors = False

# Provide ability to change base task class for celery-rpc server tasks.
# Example: { 'ModelChangeTask': my.own.ModelChangeTask }
# Key - symbolic class name, value - class with suitable interface.
# Do it on your own risk!
override_base_tasks = {}

# default celery rpc client name which will be passed as referer header
rpc_client_name = "celery_rpc_client"

# See Celery configuration parameters at
# http://docs.celeryproject.org/en/latest/configuration.html
# Some reasonable defaults are defined below

result_backend = 'cache+memory://'

task_default_queue = 'celery_rpc.requests'
task_default_exchange = 'celery_rpc'
task_default_routing_key = 'celery_rpc'

# Do not let skip messages silently (RabbitMQ)
broker_transport_options = {'confirm_publish': True}

task_acks_late = True
accept_content = ['json', 'x-json', 'x-rpc-json']
task_serializer = 'x-json'
result_serializer = 'x-json'

# Options can be overridden by CELERY_RPC_CONFIG dict in Django settings.py
_CONFIG = getattr(_settings, 'CELERY_RPC_CONFIG', {})

locals().update(_CONFIG)

task_soft_time_limit = get_result_timeout + 1
task_time_limit = get_result_timeout * 2

_codecs_registered = False
if not _codecs_registered:
    from .codecs import register_codecs

    register_codecs()
    _codecs_registered = True
