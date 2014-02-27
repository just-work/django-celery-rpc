# coding: utf-8
from celery import Celery


def create_celery_app(config=None, **opts):
    opts.setdefault('main', 'celery-rpc')
    app = Celery(**opts)
    app.config_from_object('celery_rpc.config')
    if config:
        app.conf.update(config)
    return app

FILTER_TASK_NAME = 'celery_rpc.filter'
UPDATE_TASK_NAME = 'celery_rpc.update'
GETSET_TASK_NAME = 'celery.rpc.getset'
UPDATE_OR_CREATE_TASK_NAME = 'celery_rpc.update_or_create'
CREATE_TASK_NAME = 'celery_rpc.create'
DELETE_TASK_NAME = 'celery_rpc.delete'
CALL_TASK_NAME = 'celery_rpc.call'

TASK_NAMES = (FILTER_TASK_NAME, UPDATE_TASK_NAME,
              UPDATE_OR_CREATE_TASK_NAME, CREATE_TASK_NAME,
              DELETE_TASK_NAME, CALL_TASK_NAME,)
