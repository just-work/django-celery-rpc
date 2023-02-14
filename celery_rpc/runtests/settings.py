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
    'task_always_eager': True,
    'override_base_tasks': {
        'ModelTask': 'celery_rpc.tests.tasks.CustomModelTask'
    },
    'wrap_remote_errors': True,
    'task_serializer': 'x-rpc-json'
}

MIDDLEWARE_CLASSES = []
