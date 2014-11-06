# coding: utf-8
from autofixture import AutoFixture
from celery_rpc.tests.models import SimpleModel


def get_model_dict(model):
    result = model.__dict__.copy()
    del result['_state']
    return result

def get_model_dict_from_list(models):
    result = []
    for model in models:
        result.append(get_model_dict(model))
    return result


class SimpleModelTestMixin(object):
    """ Helper for tests with model needs.
    """

    MODEL_SYMBOL = 'celery_rpc.tests.models:SimpleModel'

    def setUp(self):
        self.models = AutoFixture(SimpleModel).create(5)

    get_model_dict = staticmethod(get_model_dict)