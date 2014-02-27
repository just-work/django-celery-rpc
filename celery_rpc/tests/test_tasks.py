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

    task = tasks.update

    def testUpdateOne(self):
        expected = get_model_dict(self.models[0])
        expected.update(char=str(uuid4()))
        r = self.task.delay(self.MODEL_SYMBOL, expected)
        self.assertEquals(expected, r.get())

        updated = get_model_dict(SimpleModel.objects.get(pk=expected['id']))
        self.assertEquals(expected, updated)

    def testUpdateMulti(self):
        expected = [get_model_dict(e) for e in self.models[0:2]]
        for e in expected:
            e.update(char=str(uuid4()))
        r = self.task.delay(self.MODEL_SYMBOL, expected)
        result = r.get()
        self.assertEquals(2, len(result))
        self.assertEquals(expected, result)

        updated = [get_model_dict(o) for o in SimpleModel.objects.all()[0:2]]
        self.assertEquals(expected, updated)

    def testUpdatePartial(self):
        char_val = str(uuid4())
        expected = get_model_dict(self.models[0])
        expected.update(char=char_val)
        r = self.task.delay(self.MODEL_SYMBOL,
                            {'char':char_val, 'id': expected['id']})
        self.assertDictEqual(expected, r.get())

        updated = get_model_dict(SimpleModel.objects.get(pk=expected['id']))
        self.assertEquals(expected, updated)


class GetSetTaskTests(BaseTaskTests):

    task = tasks.getset

    def testGetSetOne(self):
        new = get_model_dict(self.models[0])
        new.update(char=str(uuid4()))
        r = self.task.delay(self.MODEL_SYMBOL, new)
        old = get_model_dict(self.models[0])
        self.assertEquals(old, r.get())

        updated = get_model_dict(SimpleModel.objects.get(pk=old['id']))
        self.assertEquals(new, updated)

    def testGetSetMulti(self):
        new = [get_model_dict(e) for e in self.models[0:2]]
        for e in new:
            e.update(char=str(uuid4()))
        r = self.task.delay(self.MODEL_SYMBOL, new)
        result = r.get()
        self.assertEquals(2, len(result))
        old = [get_model_dict(e) for e in self.models[0:2]]
        self.assertEquals(old, result)

        updated = [get_model_dict(o) for o in SimpleModel.objects.all()[0:2]]
        self.assertEquals(new, updated)


class CreateTaskTests(BaseTaskTests):

    task = tasks.create

    def testCreateOne(self):
        expected = str(uuid4())
        self.assertEquals(0, SimpleModel.objects.filter(char=expected).count())

        r = self.task.delay(self.MODEL_SYMBOL, {'char': expected})

        self.assertEquals(expected, r.get()['char'])
        self.assertEquals(1, SimpleModel.objects.filter(char=expected).count())

    def testCreateMany(self):
        uuids = str(uuid4()), str(uuid4())
        expected = [{'char': v} for v in uuids]
        self.assertEquals(0, SimpleModel.objects.filter(char__in=uuids).count())

        r = self.task.delay(self.MODEL_SYMBOL, expected)

        self.assertEquals(expected, [{'char': i['char']} for i in r.get()])
        self.assertEquals(2, SimpleModel.objects.filter(char__in=uuids).count())


class UpdateOrCreateTaskTests(UpdateTaskTests, CreateTaskTests):

    task = tasks.update_or_create


class DeleteTaskTests(BaseTaskTests):

    def testDeleteOne(self):
        expected = get_model_dict(self.models[0])

        r = tasks.delete.delay(self.MODEL_SYMBOL, expected)

        self.assertEquals(None, r.get())
        self.assertEquals(0, SimpleModel.objects.filter(id=expected['id']).count())

    def testDeleteMany(self):
        expected = (get_model_dict(self.models[0]),
                    get_model_dict(self.models[1]))

        r = tasks.delete.delay(self.MODEL_SYMBOL, expected)

        self.assertEquals([], r.get())
        ids = [v['id'] for v in expected]
        self.assertEquals(0, SimpleModel.objects.filter(id__in=ids).count())


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
