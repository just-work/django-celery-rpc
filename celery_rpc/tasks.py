# coding: utf-8
from __future__ import absolute_import
import inspect

from celery import Task
from kombu.utils import symbol_by_name
import django
from django.db import router, transaction
from django.db.models.base import Model
from rest_framework.serializers import ModelSerializer

from . import config, utils
from .app import rpc
from .exceptions import RestFrameworkError, ModelTaskError


class ModelTask(Task):
    """ Base task for operating with django models.
    """
    abstract = True

    def __call__(self, model, *args, **kwargs):
        """ Prepare context for calling task function.
        """
        self.request.model = self._import_model(model)
        args = [model] + list(args)
        try:
            return self.run(*args, **kwargs)
        except ModelTaskError:
            raise
        except Exception as e:
            raise ModelTaskError('Unhandled model error', str(type(e)), str(e))

    @staticmethod
    def _import_model(model_name):
        """ Import class by full name, check type and return.
        """
        sym = symbol_by_name(model_name)
        if inspect.isclass(sym) and issubclass(sym, Model):
            return sym
        raise TypeError(
            "Symbol '{}' is not a Django model".format(model_name))

    @staticmethod
    def _import_serializer(serializer_name):
        """ Import class by full name, check type and return.
        """
        sym = symbol_by_name(serializer_name)
        if inspect.isclass(sym) and issubclass(sym, ModelSerializer):
            return sym
        raise TypeError(
            "Symbol '{}' is not a DRF serializer".format(serializer_name))

    @staticmethod
    def _create_queryset(model):
        """ Construct queryset by params.
        """
        return model.objects.all()

    def _create_serializer_class(self, model_class):
        """ Return REST framework serializer class for model.
        """

        # default serializer
        base_serializer_class = ModelSerializer

        # custom serializer
        custom_serializer = self.request.kwargs.get('serializer_cls')
        if custom_serializer:
            base_serializer_class = self._import_serializer(custom_serializer)

        class GenericModelSerializer(base_serializer_class):
            class Meta(base_serializer_class.Meta):
                model = model_class

            def get_identity(self, data):
                pk_name = self.Meta.model._meta.pk.attname
                try:
                    return data.get('pk', data.get(pk_name, None))
                except AttributeError:
                    return None

        return GenericModelSerializer

    @property
    def serializer_class(self):
        return self._create_serializer_class(self.model)

    @property
    def model(self):
        return self.request.model

    @property
    def pk_name(self):
        return self.model._meta.pk.attname

    @property
    def default_queryset(self):
        return self._create_queryset(self.model)


@rpc.task(name=utils.FILTER_TASK_NAME, bind=True, base=ModelTask, shared=False)
def filter(self, model, filters=None, offset=0,
           limit=config.FILTER_LIMIT, fields=None,  exclude=[],
           depth=0, manager='objects', database=None, serializer_cls=None,
           order_by=[], *args, **kwargs):
    """ Filter Django models and return serialized queryset.

    :param model: full name of model class like 'app.models:Model'
    :param filters: filter supported by model manager like {'pk__in': [1,2,3]}
    :param offset: offset of first item in the queryset (by default 0)
    :param limit: max number of result list (by default 1000)
    :param order_by: type list, tuple or string - add order_by to queryset, default = []
    :return: list of serialized model data

    """
    filters = filters if isinstance(filters, dict) else {}
    # do main filtering
    qs = self.default_queryset.filter(**filters)
    # add order_by: check list or tuple
    # or string and add params to qs
    if order_by:
        if isinstance(order_by, basestring):
            qs = qs.order_by(order_by)
        elif isinstance(order_by, (list, tuple)):
            if len(order_by) == 1:
                qs = qs.order_by(order_by[0])
            else:
                qs = qs.order_by(order_by)
    # add limit and offset
    qs = qs[offset:offset+limit]
    # return serializer data
    return self.serializer_class(instance=qs, many=True).data


class ModelChangeTask(ModelTask):
    """ Abstract task provides ability to changing model state.
    """
    abstract = True

    def get_instance(self, data, using=None):
        """ Prepare instance (or several instances) to changes.

        :param data: data for changing model
        :param using: send query to specified DB alias
        :return: (Model instance or queryset, many flag)
            Many flag is True if queryset is returned.
        :raise self.model.DoesNotExist: if cannot find object in single mode

        """
        pk_name = self.pk_name
        get_pk_value = lambda item: item.get('pk', item.get(pk_name))
        qs = self.default_queryset
        if using:
            qs.using(using)
        if isinstance(data, dict):
            instance = qs.get(pk=get_pk_value(data))
            many = False
        else:
            pk_values = [get_pk_value(item) for item in data]
            instance = qs.filter(pk__in=pk_values)
            many = True
        return instance, many

    def perform_changes(self, instance, data, many, allow_add_remove=False,
                        partial=True, force_insert=False, force_update=False):
        """ Change model in accordance with params

        :param instance: one or several instances of model
        :param data: data for changing instances
        :param many: True if more than one instances will be changed
        :param allow_add_remove: True if need to create absent or delete missed
            instances.
        :param partial: True if need partial update
        :return: serialized model data or list of one or errors

        """
        serializer = self.serializer_class(instance=instance, data=data,
                                           many=many,
                                           allow_add_remove=allow_add_remove,
                                           partial=partial)

        if not serializer.errors:
            serializer.save(force_insert=force_insert,
                            force_update=force_update)
            return serializer.data
        else:
            raise RestFrameworkError('Serializer errors happened',
                                     serializer.errors)


@rpc.task(name=utils.UPDATE_TASK_NAME, bind=True, base=ModelChangeTask, shared=False)
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
    elif ver[0] == 1 and ver >= 6:
        return transaction.atomic(using=using)
    else:
        raise RuntimeError('Invalid Django version: {}'.format(ver))


@rpc.task(name=utils.GETSET_TASK_NAME, bind=True, base=ModelChangeTask, shared=False)
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


@rpc.task(name=utils.UPDATE_OR_CREATE_TASK_NAME, bind=True, base=ModelChangeTask, shared=False)
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


@rpc.task(name=utils.CREATE_TASK_NAME, bind=True, base=ModelChangeTask, shared=False)
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


@rpc.task(name=utils.DELETE_TASK_NAME, bind=True, base=ModelChangeTask, shared=False)
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


class FunctionTask(Task):
    """ Base task for calling function.
    """
    abstract = True

    def __call__(self, function,  *args, **kwargs):
        """ Prepare context for calling task function.
        """
        self.request.function = self._import_function(function)
        args = [function] + list(args)
        return self.run(*args, **kwargs)

    @staticmethod
    def _import_function(func_name):
        """ Import class by full name, check type and return.
        """
        sym = symbol_by_name(func_name)
        if hasattr(sym, '__call__'):
            return sym
        raise TypeError("Symbol '{}' is not a function".format(func_name))

    @property
    def function(self):
        return self.request.function


@rpc.task(name=utils.CALL_TASK_NAME, bind=True, base=FunctionTask, shared=False)
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