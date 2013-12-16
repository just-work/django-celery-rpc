# coding: utf-8

from django.conf import settings as _settings

DEFAULT_FILTER_LIMIT = 1000

CELERY_RESULT_BACKEND = 'cache+memory://'

_CONFIG = getattr(_settings, 'CELERY_RPC_CONFIG', {})

locals().update(_CONFIG)