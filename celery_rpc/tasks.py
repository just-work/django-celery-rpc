from __future__ import absolute_import

import django
from django.db import router, transaction
import six

from . import config, utils
from .app import rpc
from .base import get_base_task_class
from .exceptions import RestFrameworkError


_base_model_task = get_base_task_class('ModelTask')


@rpc.task(name=utils.FILTER_TASK_NAME, bind=True, base=_base_model_task,
          shared=False)
def filter(self, model, filters=None, offset=0,
           limit=config.FILTER_LIMIT, fields=None, exclude=[],
           depth=0, manager='objects', database=None, serializer_cls=None,
           order_by=[], *args, **kwargs):
    """ Filter Django models and return serialized queryset.

    :param model: full name of model class like 'app.models:Model'
    :param filters: filter supported by model manager like {'pk__in': [1,2,3]}
    :param offset: offset of first item in the queryset (by default 0)
    :param limit: max number of result list (by default 1000)
    :param fields: shrink serialized fields of result
    :param order_by: order of result list (list, tuple or string), default = []
    :return: list of serialized model data

    """
    filters = filters if isinstance(filters, dict) else {}
    qs = self.default_queryset.filter(**filters)
    if order_by:
        if isinstance(order_by, six.string_types):
            qs = qs.order_by(order_by)
        elif isinstance(order_by, (list, tuple)):
            qs = qs.order_by(*order_by)
    qs = qs[offset:offset+limit]
    return self.serializer_class(instance=qs, many=True).data


_base_model_change_task = get_base_task_class('ModelChangeTask')


@rpc.task(name=utils.UPDATE_TASK_NAME, bind=True, base=_base_model_change_task,
          shared=False)
def update(self, model, data, fields=None, nocache=False,
           manager='objects', database=None, serializer_cls=None, *args, **kwargs):
    """ Update Django models by PK and return new values.

    :param model: full name of model class like 'app.models:ModelClass'
    :param data: values of one or several objects
        {'id': 1, 'title': 'hello'} or [{'id': 1, 'title': 'hello'}]
    :return: serialized model data or list of one or errors

    """
    instance, many = self.get_instance(data)
    return self.perform_changes(instance=instance, data=data, many=many,
                                allow_add_remove=False, force_update=True)


def atomic_commit_on_success(using=None):
    """ Provides context manager for atomic database operations depending on
    Django version.
    """
    ver = django.VERSION
    if ver[0] == 1 and ver[1] < 6:
        return transaction.commit_on_success(using=using)
    elif ver[0] == 1 and ver[1] >= 6:
        return transaction.atomic(using=using)
    else:
        raise RuntimeError('Invalid Django version: {}'.format(ver))


@rpc.task(name=utils.GETSET_TASK_NAME, bind=True, base=_base_model_change_task,
          shared=False)
def getset(self, model, data, fields=None, nocache=False,
           manager='objects', database=None, *args, **kwargs):
    """ Update Django models by PK and return old values as one atomic action.

    :param model: full name of model class like 'app.models:ModelClass'
    :param data: values of one or several objects
        {'id': 1, 'title': 'hello'} or [{'id': 1, 'title': 'hello'}]
    :return: serialized model data or list of one or errors

    """
    db_for_write = router.db_for_write(self.model)
    with atomic_commit_on_success(using=db_for_write):
        instance, many = self.get_instance(data, using=db_for_write)
        serializer = self.serializer_class(instance=instance, data=data,
                                           many=many, allow_add_remove=False,
                                           partial=True)

        old_values = serializer.data

        if not serializer.errors:
            serializer.save(force_update=True)
            return old_values
        else:
            raise RestFrameworkError('Serializer errors happened',
                                     serializer.errors)


@rpc.task(name=utils.UPDATE_OR_CREATE_TASK_NAME, bind=True,
          base=_base_model_change_task, shared=False)
def update_or_create(self, model, data, fields=None, nocache=False,
                     manager='objects', database=None, serializer_cls=None, *args, **kwargs):
    """ Update Django models by PK or create new and return new values.

    :param model: full name of model class like 'app.models:ModelClass'
    :param data: values of one or several objects
        {'id': 1, 'title': 'hello'} or [{'id': 1, 'title': 'hello'}]
    :return: serialized model data or list of one or errors

    """
    try:
        instance, many = self.get_instance(data)
    except self.model.DoesNotExist:
        instance, many = None, False
    return self.perform_changes(instance=instance, data=data, many=many,
                                allow_add_remove=many)


@rpc.task(name=utils.CREATE_TASK_NAME, bind=True, base=_base_model_change_task,
          shared=False)
def create(self, model, data, fields=None, nocache=False,
           manager='objects', database=None, serializer_cls=None, *args, **kwargs):
    """ Update Django models by PK or create new and return new values.

    :param model: full name of model class like 'app.models:ModelClass'
    :param data: values of one or several objects
        {'id': 1, 'title': 'hello'} or [{'id': 1, 'title': 'hello'}]
    :return: serialized model data or list of one or errors

    """
    instance, many = (None, False if isinstance(data, dict) else True)
    return self.perform_changes(instance=instance, data=data, many=many,
                                allow_add_remove=many, force_insert=True)


@rpc.task(name=utils.DELETE_TASK_NAME, bind=True, base=_base_model_change_task,
          shared=False)
def delete(self, model, data, fields=None, nocache=False,
           manager='objects', database=None, serializer_cls=None, *args, **kwargs):
    """ Delete Django models by PK.

    :param model: full name of model class like 'app.models:ModelClass'
    :param data: values of one or several objects
        {'id': 1, 'title': 'hello'} or [{'id': 1, 'title': 'hello'}]
    :return: None or [] if many

    """
    instance, many = self.get_instance(data)
    if not many:
        try:
            instance.delete()
        except Exception as e:
            raise RestFrameworkError('Could not delete instance', e)
    else:
        return self.perform_changes(instance=instance, data=[], many=many,
                                    allow_add_remove=many)


_base_function_task = get_base_task_class('FunctionTask')


@rpc.task(name=utils.CALL_TASK_NAME, bind=True, base=_base_function_task,
          shared=False)
def call(self, function, args, kwargs):
    """ Call function with args & kwargs

    :param function: full function name like 'package.module:function'
    :param args: positional parameters
    :param kwargs: named parameters
        {'id': 1, 'title': 'hello'} or [{'id': 1, 'title': 'hello'}]
    :return: result of function

    """
    args = args or []
    kwargs = kwargs or {}
    if not isinstance(args, list):
        message = "Invalid type of 'args', need: 'list', got: '{}'".format(
            type(args))
        raise TypeError(message)
    if not isinstance(kwargs, dict):
        message = "Invalid type of 'kwargs', need: 'dict', got: '{}'".format(
            type(args))
        raise TypeError(message)
    return self.function(*args, **kwargs)