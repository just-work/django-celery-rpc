import inspect

import django
from celery import Task
from kombu.utils import symbol_by_name
from django.db.models import Model
from django.db import transaction
from rest_framework.serializers import ModelSerializer

from . import config
from .exceptions import ModelTaskError, RestFrameworkError


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
                pk_name = self.Meta.model._meta.pk.name
                try:
                    return data.get('pk', data.get(pk_name, None))
                except AttributeError:
                    return None

        fields = self.request.kwargs.get("fields")
        if fields:
            GenericModelSerializer.Meta.fields = fields

        return GenericModelSerializer

    @property
    def serializer_class(self):
        return self._create_serializer_class(self.model)

    @property
    def model(self):
        return self.request.model

    @property
    def pk_name(self):
        return self.model._meta.pk.name

    @property
    def default_queryset(self):
        return self._create_queryset(self.model)


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

        if serializer.is_valid():
            serializer.save(force_insert=force_insert,
                            force_update=force_update)
            return serializer.data
        else:
            raise RestFrameworkError('Serializer errors happened',
                                     serializer.errors)


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


def get_base_task_class(base_task_name):
    """ Provide base task for actual tasks

    Load custom base task if overrides are in config or return default base task.

    :param base_task_name: name of default base task in this module
    :return: base celery task class
    """
    base_task = globals().get(base_task_name)
    custom_task_name = config.OVERRIDE_BASE_TASKS.get(base_task_name)
    if not custom_task_name:
        return base_task
    sym = symbol_by_name(custom_task_name)
    if inspect.isclass(sym) and issubclass(sym, base_task):
        return sym
    raise TypeError(
        "Symbol '{}' has not a base ".format(custom_task_name,
                                             base_task.__name__))


def atomic_commit_on_success():
    """ Select context manager for atomic database operations depending on
    Django version.
    """
    ver = django.VERSION
    if ver[0] == 1 and ver[1] < 6:
        return transaction.commit_on_success
    elif ver[0] == 1 and ver[1] >= 6:
        return transaction.atomic
    else:
        raise RuntimeError('Invalid Django version: {}'.format(ver))

atomic_commit_on_success = atomic_commit_on_success()
