from __future__ import absolute_import

from django.db import router
from django.db.models import Q
import six

from . import config, utils
from .app import rpc
from .base import get_base_task_class, atomic_commit_on_success
from .exceptions import RestFrameworkError


_base_model_task = get_base_task_class('ModelTask')


@rpc.task(name=utils.FILTER_TASK_NAME, bind=True, base=_base_model_task,
          shared=False)
def filter(self, model, filters=None, offset=0,
           limit=config.FILTER_LIMIT, fields=None, exclude=None,
           depth=0, manager='objects', database=None, serializer_cls=None,
           order_by=None, filters_Q=None, exclude_Q=None, *args, **kwargs):
    """ Filter Django models and return serialized queryset.

    :param model: full name of model class like 'app.models:Model'
    :param filters: supported lookups for filter like {'pk__in': [1,2,3]}
    :param offset: offset of first item in the queryset (by default 0)
    :param limit: max number of result list (by default 1000)
    :param fields: shrink serialized fields of result
    :param exclude: supported lookups for exclude like {'pk__in': [1,2,3]}
    :param order_by: order of result list (list, tuple or string), default = []
    :param filters_Q: Django Q object for filter()
    :param exclude_Q: Django Q object for exclude()
    :return: list of serialized model data

    """
    qs = self.default_queryset
    if filters or filters_Q:
        filters = filters if isinstance(filters, dict) else {}
        filters_Q = filters_Q if isinstance(filters_Q, Q) else Q()
        qs = qs.filter(filters_Q, **filters)
    if exclude or exclude_Q:
        exclude = exclude if isinstance(exclude, dict) else {}
        exclude_Q = exclude_Q if isinstance(exclude_Q, Q) else Q()
        qs = qs.exclude(exclude_Q, **exclude)
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


@rpc.task(name=utils.PIPE_TASK_NAME, bind=True, shared=False)
def pipe(self, pipeline):
    """ Handle pipeline and return results
    :param pipeline: List of pipelined requests.
    :return: list of results of each request.
    """
    global result
    result = []
    r = None
    with atomic_commit_on_success():
        for t in pipeline:
            task = self.app.tasks[t['name']]
            args = t['args']
            if t['options'].get('transformer'):
                if not hasattr(args, 'append'):
                    args = list(args)
                if t['name'] == utils.RESULT_TASK_NAME:
                    args.append(result)
                else:
                    args.append(r)
            r = task.apply(args=args, kwargs=t['kwargs']).get()
            result.append(r)

    return result


@rpc.task(name=utils.TRANSLATE_TASK_NAME, bind=True, shared=False)
def translate(self, map, data, defaults=None):
    defaults = defaults or {}

    def _translate_keys_and_set_defaults(data):
        result = defaults.copy()
        for initial_key, result_key in map.items():
            if initial_key in data:
                result[result_key] = data[initial_key]

        return result

    if isinstance(data, (list, tuple)):
        out = []
        for el in data:
            out.append(_translate_keys_and_set_defaults(el))
        return out
    else:
        return _translate_keys_and_set_defaults(data)


@rpc.task(name=utils.RESULT_TASK_NAME, bind=True, shared=False)
def result(self, index, data):
    return data[index]
