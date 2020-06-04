import inspect
import six
from logging import getLogger

import django
from celery import Task
from django.db.models import Model
from django.db import transaction
from rest_framework import serializers
from rest_framework import VERSION

from . import config
from .utils import symbol_by_name, unproxy
from .exceptions import RestFrameworkError, RemoteException

logger = getLogger(__name__)

DRF_VERSION = tuple(map(int, VERSION.split('.')))

DRF3 = DRF_VERSION >= (3, 0, 0)
DRF34 = DRF_VERSION >= (3, 4, 0)


class remote_error(object):
    """ Transforms all raised exceptions to a RemoteException wrapper,
    if enabled if CELERY_RPC_CONFIG['WRAP_REMOTE_ERRORS'].

    Wrapper serializes exception args with CELERY_TASK_SERIALIZER of rpc app.
    """

    def __init__(self, task):
        self.task = task

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Unpacks exception from RemoteException wrapper, if enabled in
        celery_rpc config."""
        if isinstance(exc_val, RemoteException):
            return
        if exc_val and self.task.app.conf['WRAP_REMOTE_ERRORS']:
            serializer = self.task.app.conf['CELERY_RESULT_SERIALIZER']
            raise RemoteException(exc_val, serializer)


if DRF3:
    class GenericListSerializerClass(serializers.ListSerializer):

        def update(self, instance, validated_data):
            """ Performs bulk delete or update or create.

            * instances are deleted if new data is empty
            * if lengths of instances and new date are equal,
              performs item-by-item update
            * performs bulk creation is no instances passed

            :returns new values
            """
            if not validated_data:
                for obj in instance:
                    obj.delete()
                return self.create(validated_data)
            if len(instance) == len(validated_data):
                for obj, values in zip(instance, validated_data):
                    for k, v in values.items():
                        setattr(obj, k, v)
                        obj.save()
            elif len(instance) == 0:
                return self.create(validated_data)
            else:
                raise RuntimeError("instance and data len differs, "
                                   "don't know what to do")
            return instance


class RpcTask(Task):
    """ Base celery rpc task class
    """

    @property
    def headers(self):
        return self.request.headers or {}

    def __call__(self, *args, **kwargs):
        with remote_error(self):
            self.prepare_context(*args, **kwargs)
            return self.run(*args, **kwargs)

    def prepare_context(self, *args, **kwargs):
        """ Prepare context for calling task function. Do nothing by default.
        """


class ModelTask(RpcTask):
    """ Base task for operating with django models.
    """
    abstract = True

    def __call__(self, model, *args, **kwargs):
        logger.debug("Got task %s", self.name,
                     extra={"referer": self.headers.get("referer"),
                            "piped": self.headers.get("piped"),
                            "model": model})
        return super(ModelTask, self).__call__(model, *args, **kwargs)

    def prepare_context(self, model, *args, **kwargs):
        self.request.model = self._import_model(model)

    @staticmethod
    def _import_model(model_name):
        """ Import class by full name, check type and return.
        """
        sym = symbol_by_name(model_name)
        if isinstance(sym, six.string_types):
            # perhaps model name is a value of 'sym'
            model_name = sym
            sym = symbol_by_name(model_name)
        elif not inspect.isclass(sym) and callable(sym):
            # perhaps model name is a result of call 'sym()'
            model_name = sym()
            sym = symbol_by_name(model_name)
        if issubclass(sym, Model):
            return sym
        raise TypeError(
            "Symbol '{}' is not a Django model".format(model_name))

    @staticmethod
    def _import_serializer(serializer_name):
        """ Import class by full name, check type and return.
        """
        sym = symbol_by_name(serializer_name)
        if inspect.isclass(sym) and issubclass(sym,
                                               serializers.ModelSerializer):
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
        base_serializer_class = serializers.ModelSerializer

        # custom serializer
        custom_serializer = self.request.kwargs.get('serializer_cls')
        if custom_serializer:
            base_serializer_class = self._import_serializer(custom_serializer)

        identity_field = self.identity_field

        # DRF >= 3.4
        base_serializer_fields = (getattr(
            getattr(base_serializer_class, 'Meta', None), 'fields', None))

        class GenericModelSerializer(base_serializer_class):

            class Meta(getattr(base_serializer_class, 'Meta', object)):
                model = model_class

                if DRF3:
                    # connect overriden list serializer to child serializer
                    list_serializer_class = GenericListSerializerClass

                if DRF34:
                    # implicit fields: DRF 3.4 - deprecated , DRF 3.5 - removed
                    fields = base_serializer_fields or '__all__'

            def get_identity(self, data):
                try:
                    return data.get(identity_field, data.get('pk', None))
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
    def identity_field(self):
        """ Name of field which used as key-field
        """
        return self.request.kwargs.get('identity') or self.pk_name

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
        identity_field = self.identity_field
        get_identity = lambda item: item.get(identity_field, item.get('pk'))
        qs = self.default_queryset
        if using:
            qs.using(using)
        if isinstance(data, dict):
            instance = qs.get(**{identity_field: get_identity(data)})
            many = False
        else:
            identity_values = [get_identity(item) for item in data]
            instance = qs.filter(**{identity_field + '__in': identity_values})
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
        kwargs = {'allow_add_remove': allow_add_remove} if not DRF3 else {}
        s = self.serializer_class(instance=instance, data=data, many=many,
                                  partial=partial, **kwargs)

        if s.is_valid():
            if not DRF3:
                s.save(force_insert=force_insert,
                       force_update=force_update)
            elif force_insert:
                s.instance = s.create(s.validated_data)
            elif force_update:
                s.update(s.instance, s.validated_data)
            else:
                s.save()
            return s.data
        else:
            # force ugettext_lazy to unproxy
            errors = unproxy(s.errors)
            raise RestFrameworkError('Serializer errors happened', errors)


class FunctionTask(RpcTask):
    """ Base task for calling function.
    """
    abstract = True

    def __call__(self, function, *args, **kwargs):
        logger.debug("Got task %s", self.name,
                     extra={"referer": self.headers.get("referer"),
                            "piped": self.headers.get("piped"),
                            "function": function})
        return super(FunctionTask, self).__call__(function, *args, **kwargs)

    def prepare_context(self, function, *args, **kwargs):
        self.request.function = self._import_function(function)

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


class PipeTask(RpcTask):
    """ Base Task for pipe function.
    """

    def __call__(self, *args, **kwargs):
        logger.debug("Got task %s", self.name,
                     extra={"referer": self.headers.get("referer")})
        return super(PipeTask, self).__call__(*args, **kwargs)


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
    elif (ver[0] == 1 and ver[1] >= 6) or ver[0] >= 2:
        return transaction.atomic
    else:
        raise RuntimeError('Invalid Django version: {}'.format(ver))


atomic_commit_on_success = atomic_commit_on_success()
