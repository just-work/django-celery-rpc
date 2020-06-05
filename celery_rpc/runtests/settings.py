from uuid import uuid4

DEBUG = True
TEMPLATE_DEBUG = DEBUG

INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'celery_rpc.runtests',
    'celery_rpc.tests'
]

DATABASE_ENGINE = 'django.db.backends.sqlite3',
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:'
    }
}

SECRET_KEY = str(uuid4())

CELERY_RPC_CONFIG = {
    'CELERY_ALWAYS_EAGER': True,
    'OVERRIDE_BASE_TASKS': {
        'ModelTask': 'celery_rpc.tests.tasks.CustomModelTask'
    },
    'WRAP_REMOTE_ERRORS': True,
    'CELERY_TASK_SERIALIZER': 'x-rpc-json'
}

MIDDLEWARE_CLASSES = []