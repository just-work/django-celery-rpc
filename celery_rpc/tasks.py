# coding: utf-8
import inspect

from celery import Task
from django.conf import settings
from django.db.models.base import Model
from celery_rpc.celery import rpc
from kombu.utils import symbol_by_name
from rest_framework.serializers import ModelSerializer


class ModelTask(Task):
    """ Base task for operating with django models.
    """
    abstract = True

    def __call__(self, model_name,  *args, **kwargs):
        """ Prepare context for calling task function.
        """
        self.request.model = self._import_model(model_name)
        args = [model_name] + list(args)
        return super(ModelTask, self).__call__(*args, **kwargs)

    def _import_model(self, model_name):
        """ Import class by full name, check type and return.
        """
        cls = symbol_by_name(model_name)
        if inspect.isclass(cls) and issubclass(cls, Model):
            return cls
        raise TypeError(
            "Symbol '{}' is not a Django model_name".format(model_name))

    def _create_queryset(self, model):
        """ Construct queryset by params.
        """
        return model.objects.all()

    def _get_serializer_class(self, model):
        """ Return REST framework serializer class for model.
        """
        class GenericModelSerializer(ModelSerializer):
            class Meta:
                model = model

        return GenericModelSerializer


DEFAULT_FILTER_LIMIT = getattr(settings.CELERY_RPC_CONFIG,
                               'DEFAULT_FILTER_LIMIT', 1000)

@rpc.task(bind=True, base=ModelTask)
def filter(self, model_name, filters=None, offset=0,
           limit=DEFAULT_FILTER_LIMIT, fields=None,  exclude=[], depth=0,
           manager='objects', database=None, *args, **kwargs):
    """ Filter Django models and return serialized queryset.

    :param model_name: full name of model class like 'app.models:Model'
    :param filters: filter supported by model manager like{'pk__in': [1,2,3]}
        also can contains:
            'start': 5000
            'limit': 5000
    :param fields: limit result by fields, for example ['id', name]
    :return: list of serialized model data

    """
    model = self.request.model
    filters = filters if isinstance(filters, dict) else {}
    qs = self._create_queryset(model).filter(**filters)[offset:offset+limit]
    serializer = self._get_serializer_class(model)
    return serializer(isinstance=qs, many=True)


@rpc.tasks(bind=True, base=ModelTask)
def update(self, model_name, data, fields=None, nocache=False,
           manager='objects',
           database=None, *args, **kwargs):
    """ Update Django models by PK and return new values.

    :param model_name: model class like 'app.models:ModelClass'
    :param data: new values of objects
        [{'id': 1, 'title': 'hello'}]
    :return: list of new model

    """

