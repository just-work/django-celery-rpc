from __future__ import absolute_import

from uuid import uuid4

from autofixture import AutoFixture
from django.test import TestCase

from .. import tasks
from .models import SimpleModel


def get_model_dict(model):
    result = model.__dict__.copy()
    del result['_state']
    return result


class BaseTaskTests(TestCase):

    MODEL_SYMBOL = 'celery_rpc.tests.models:SimpleModel'

    def setUp(self):
        self.models = AutoFixture(SimpleModel).create(5)


class FilterTaskTests(BaseTaskTests):

    def testLimit(self):
        r = tasks.filter.delay(self.MODEL_SYMBOL)
        self.assertEquals(5, len(r.get()))

        r = tasks.filter.delay(self.MODEL_SYMBOL, limit=2)
        self.assertEquals(2, len(r.get()))

    def testOffset(self):
        r = tasks.filter.delay(self.MODEL_SYMBOL, offset=1)
        expected = get_model_dict(self.models[1])
        self.assertEquals(expected, r.get()[0])

    def testFilters(self):
        expected = get_model_dict(self.models[0])
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               filters={'pk': expected['id']})
        self.assertEquals(expected, r.get()[0])


class UpdateTaskTests(BaseTaskTests):

    def testUpdateOne(self):
        expected = get_model_dict(self.models[0])
        expected.update(char=str(uuid4()))
        r = tasks.update.delay(self.MODEL_SYMBOL, expected)
        self.assertEquals(expected, r.get())

        updated = get_model_dict(SimpleModel.objects.get(pk=expected['id']))
        self.assertEquals(expected, updated)

    def testUpdateMulti(self):
        expected = [get_model_dict(e) for e in self.models[0:2]]
        for e in expected:
            e.update(char=str(uuid4()))
        r = tasks.update.delay(self.MODEL_SYMBOL, expected)
        result = r.get()
        self.assertEquals(2, len(result))
        self.assertEquals(expected, result)

        updated = [get_model_dict(o) for o in SimpleModel.objects.all()[0:2]]
        self.assertEquals(expected, updated)


def plus(a, b):
    return a + b


class CallTaskTests(TestCase):

    def testCallPlus(self):
        a = 2
        b = 3
        expected = a + b
        r = tasks.call.delay('celery_rpc.tests.test_tasks:plus', [a, b],
                             None)
        self.assertEquals(expected, r.get())
