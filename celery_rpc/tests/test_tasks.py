from __future__ import absolute_import
from random import randint

from uuid import uuid4

from autofixture import AutoFixture
from django.test import TestCase
from rest_framework import serializers

from .. import tasks
from ..exceptions import ModelTaskError
from ..tests.tasks import CustomModelTask
from .models import SimpleModel, NonAutoPrimaryKeyModel, PartialUpdateModel


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

    def testSerializerFields(self):
        expected = get_model_dict(self.models[0])
        field = list(expected.keys())[0]
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               filters={'pk': expected['id']},
                               fields=[field])
        self.assertEquals({field: expected[field]}, r.get()[0])

    def testOrdering(self):
        self.models[0].char = 'a'
        self.models[0].save()

        self.models[1].char = 'b'
        self.models[1].save()

        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               filters={'char__in': ['a', 'b']},
                               order_by=['char'])
        self.assertEquals(['a', 'b'], [item['char'] for item in r.get()])

    def testReverseOrdering(self):
        self.models[0].char = 'a'
        self.models[0].save()

        self.models[1].char = 'b'
        self.models[1].save()

        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               filters={'char__in': ['a', 'b']},
                               order_by='-char')
        self.assertEquals(['b', 'a'], [item['char'] for item in r.get()])


class SimpleTaskSerializer(serializers.ModelSerializer):
    """ Test serializer
    """
    class Meta:
        model = SimpleModel
        fields = ('id', )


class SingleObjectsDoesNotExistMixin(object):
    """ Checks behavior of tasks, which modify existing objects.
    """

    def testSingleObjectDoesNotExist(self):
        """ Raise exception if cannot find object in single mode  """
        with self.assertRaisesRegexp(ModelTaskError,
                                     r'matching query does not exist.'):
            self.task.delay(self.MODEL_SYMBOL,
                            {'char': str(uuid4()),
                             'id': randint(100, 1000)}).get()


class UpdateTaskTests(SingleObjectsDoesNotExistMixin, BaseTaskTests):

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
                            {'char': char_val, 'id': expected['id']})
        self.assertDictEqual(expected, r.get())

        updated = get_model_dict(SimpleModel.objects.get(pk=expected['id']))
        self.assertEquals(expected, updated)

    def testSerializer(self):
        """ Test serializer_cls """
        char_val = str(uuid4())
        expected = get_model_dict(self.models[0])
        expected.update(char=char_val)

        serializer_cls = "{}:{}".format(SimpleTaskSerializer.__module__,
                                        SimpleTaskSerializer.__name__)
        r = self.task.delay(self.MODEL_SYMBOL,
                            {'char':char_val, 'id': expected['id']},
                            serializer_cls=serializer_cls)
        self.assertDictEqual({'id': expected['id']}, r.get())

    def testNoExistSerializer(self):
        """ Test not existing serializer """
        char_val = str(uuid4())
        expected = get_model_dict(self.models[0])

        with self.assertRaisesRegexp(
                ModelTaskError, r'No module named (not\.existing|\'not\')'):
            self.task.delay(self.MODEL_SYMBOL,
                            {'char': char_val, 'id': expected['id']},
                            serializer_cls='not.existing.symbol').get()

    def testNoValidSerializer(self):
        """ Test not valid serializer """
        char_val = str(uuid4())
        expected = get_model_dict(self.models[0])

        with self.assertRaisesRegexp(ModelTaskError, r'not a DRF serializer'):
            serializer_cls = 'celery_rpc.tests.models:SimpleModel'
            self.task.delay(self.MODEL_SYMBOL,
                            {'char': char_val, 'id': expected['id']},
                            serializer_cls=serializer_cls).get()


class GetSetTaskTests(SingleObjectsDoesNotExistMixin, BaseTaskTests):

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

    def testPartialUpdate(self):
        """ Check that getset allow update model partially
        """
        m = AutoFixture(PartialUpdateModel).create_one()
        preserve_f2 = m.f2
        expected = randint(1, 1000)
        r = self.task.delay('celery_rpc.tests.models:PartialUpdateModel',
                            {'f1': expected, 'pk': m.pk})
        r = r.get()
        m = PartialUpdateModel.objects.get(pk=m.pk)
        self.assertEquals(expected, m.f1)
        self.assertEquals(preserve_f2, m.f2)

class CreateTaskTests(BaseTaskTests):

    task = tasks.create

    def testCreateOne(self):
        expected = str(uuid4())
        self.assertEquals(0, SimpleModel.objects.filter(char=expected).count())

        r = self.task.delay(self.MODEL_SYMBOL, {'char': expected})

        self.assertEquals(expected, r.get()['char'])
        self.assertEquals(1, SimpleModel.objects.filter(char=expected).count())

    def testCreateMulti(self):
        uuids = str(uuid4()), str(uuid4())
        expected = [{'char': v} for v in uuids]
        self.assertEquals(0, SimpleModel.objects.filter(char__in=uuids).count())

        r = self.task.delay(self.MODEL_SYMBOL, expected)

        self.assertEquals(expected, [{'char': i['char']} for i in r.get()])
        self.assertEquals(2, SimpleModel.objects.filter(char__in=uuids).count())

    def testSingleObjectDoesNotExist(self):
        """ Creates new object if provided ID does not exist """
        expected = str(uuid4())
        self.assertEquals(0, SimpleModel.objects.filter(char=expected).count())

        unexpected_id = randint(100, 1000)
        r = self.task.delay(self.MODEL_SYMBOL, {'char': expected,
                                                'id': unexpected_id})

        self.assertEquals(expected, r.get()['char'])
        self.assertNotEquals(unexpected_id, r.get()['id'])
        self.assertEquals(0, SimpleModel.objects.filter(char=unexpected_id).count())
        self.assertEquals(1, SimpleModel.objects.filter(char=expected).count())

    def testSingleObjectAlreadyExist(self):
        """ Raise exception if object already exists """
        pk = randint(1, 1000)
        obj = NonAutoPrimaryKeyModel.objects.create(pk=pk)
        with self.assertRaisesRegexp(
                ModelTaskError,
                r'primary key|PRIMARY KEY'):
            r = self.task.delay('celery_rpc.tests.models:NonAutoPrimaryKeyModel',
                                {'id': obj.pk})
            self.assertNotEquals(self.models[0].id, r.get()['id'])


class UpdateOrCreateTaskTests(CreateTaskTests, UpdateTaskTests):

    task = tasks.update_or_create

    def testSingleObjectAlreadyExist(self):
        super(UpdateOrCreateTaskTests, self).testUpdateOne()


class DeleteTaskTests(SingleObjectsDoesNotExistMixin, BaseTaskTests):

    task = tasks.delete

    def testDeleteOne(self):
        expected = get_model_dict(self.models[0])

        r = self.task.delay(self.MODEL_SYMBOL, expected)

        self.assertEquals(None, r.get())
        self.assertEquals(0, SimpleModel.objects.filter(id=expected['id']).count())

    def testDeleteMany(self):
        expected = (get_model_dict(self.models[0]),
                    get_model_dict(self.models[1]))

        r = self.task.delay(self.MODEL_SYMBOL, expected)

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


class OverrideTaskTests(TestCase):
    """ Check if base task class overriding is worked.
    """
    def testOverrideModelTask(self):
        self.assertIsInstance(tasks.filter, CustomModelTask)
