# coding: utf-8

import six
from celery import Celery
from kombu import Queue, utils
from six.moves import reduce


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


def symbol_by_name(name):
    """ Get symbol by qualified name.
    """
    try:
        return utils.symbol_by_name(name)
    except:
        pass

    if ':' in name:
        name = name.replace(':', '.')
    attrs = name.split('.')
    base_module = utils.symbol_by_name(attrs.pop(0))
    return reduce(getattr, attrs, base_module)


FILTER_TASK_NAME = 'celery_rpc.filter'
UPDATE_TASK_NAME = 'celery_rpc.update'
GETSET_TASK_NAME = 'celery_rpc.getset'
UPDATE_OR_CREATE_TASK_NAME = 'celery_rpc.update_or_create'
CREATE_TASK_NAME = 'celery_rpc.create'
DELETE_TASK_NAME = 'celery_rpc.delete'
CALL_TASK_NAME = 'celery_rpc.call'
PIPE_TASK_NAME = 'celery_rpc.pipe'
TRANSLATE_TASK_NAME = 'celery_rpc.translate'
RESULT_TASK_NAME = 'celery_rpc.result'

TASK_NAME_MAP = {n: v for n, v in locals().items() if n.endswith('_TASK_NAME')}

DEFAULT_EXC_SERIALIZER = 'json'


def unpack_exception(error, wrap_errors, serializer=DEFAULT_EXC_SERIALIZER):
    """ Extracts original error from RemoteException description
    :param error: remote exception stub (or real) instance
    :type error: RemoteException
    :param wrap_errors: flag for enabling errors unpacking
    :type wrap_errors: bool
    :return: original error instance, if unpacking is successful;
        None otherwise.
    :rtype: Exception
    """
    if not wrap_errors:
        return None
    if not error.__class__.__name__ == 'RemoteException':
        return None
    if not hasattr(error, 'unpack_exception'):
        # Stub exception
        from celery_rpc.exceptions import RemoteException
        error = RemoteException(error.args)
    error = error.unpack_exception(serializer)
    return error


def unproxy(errors):
    """ removes ugettext_lazy proxy from ValidationError structure to allow
    errors to be serialized with JSON encoder."""
    for k, v in errors.items():
        unproxied = []
        for i in v:
            unproxied.append(six.text_type(i))
        errors[k] = unproxied
    return errors
