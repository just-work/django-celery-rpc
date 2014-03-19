# coding: utf-8
from celery import Celery
from kombu import Queue


def create_celery_app(config=None, **opts):
    opts.setdefault('main', 'celery-rpc')
    app = Celery(**opts)
    app.config_from_object('celery_rpc.config')
    if config:
        app.conf.update(config)

    # Setup queues in accordance with config and overrides
    q = app.conf['CELERY_DEFAULT_QUEUE']
    rk = app.conf['CELERY_DEFAULT_ROUTING_KEY'] or q
    high_q = q + '.high_priority'
    high_rk = rk + '.high_priority'

    app.conf.update(
        CELERY_HIGH_PRIORITY_QUEUE=high_q,
        CELERY_HIGH_PRIORITY_ROUTING_KEY=high_rk,
        CELERY_QUEUES=(Queue(q, routing_key=rk),
                       Queue(high_q, routing_key=high_rk)))

    return app

FILTER_TASK_NAME = 'celery_rpc.filter'
UPDATE_TASK_NAME = 'celery_rpc.update'
GETSET_TASK_NAME = 'celery_rpc.getset'
UPDATE_OR_CREATE_TASK_NAME = 'celery_rpc.update_or_create'
CREATE_TASK_NAME = 'celery_rpc.create'
DELETE_TASK_NAME = 'celery_rpc.delete'
CALL_TASK_NAME = 'celery_rpc.call'

TASK_NAME_MAP = {n: v for n, v in locals().items() if n.endswith('_TASK_NAME')}
