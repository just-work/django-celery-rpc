# coding: utf-8
from autofixture import AutoFixture
from django.core.exceptions import ValidationError
from rest_framework import serializers
from celery_rpc.tests.models import SimpleModel
from celery_rpc import utils
from celery_rpc.base import DRF3

def get_model_dict(model):
    result = model.__dict__.copy()
    del result['_state']
    if not DRF3:
        return result
    model_class = model._meta.model
    class Serializer(serializers.ModelSerializer):
        class Meta:
            model = model_class

    s = Serializer(instance=model)
    result = s.data
    return result


def get_model_dict_from_list(models):
    result = []
    for model in models:
        result.append(get_model_dict(model))
    return result


class SimpleModelTestMixin(object):
    """ Helper for tests with model needs.
    """
    MODEL = SimpleModel
    MODEL_SYMBOL = 'celery_rpc.tests.models:SimpleModel'

    def setUp(self):
        super(SimpleModelTestMixin, self).setUp()
        self.models = AutoFixture(self.MODEL).create(5)

    get_model_dict = staticmethod(get_model_dict)


class RemoteException(Exception):
    pass


def fail(*args):
    raise ValidationError({"field": "gavno"})


class unpack_exception(object):
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            return

        if hasattr(exc_val, 'restore'):
            exc_val = exc_val.restore()
        inner = utils.unpack_exception(exc_val, True)
        exc_val = inner or exc_val
        raise exc_val