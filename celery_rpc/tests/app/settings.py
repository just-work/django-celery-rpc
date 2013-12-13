# coding: utf-8
from uuid import uuid4

DEBUG = True
TEMPLATE_DEBUG = DEBUG

INSTALLED_APPS = [
    'celery_rpc.tests.app',
]

DATABASE_ENGINE = 'django.db.backends.sqlite3',
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:'
    }
}

SECRET_KEY = str(uuid4())