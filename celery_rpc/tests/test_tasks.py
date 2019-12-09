from __future__ import absolute_import
from random import randint
from uuid import uuid4

from django.core.exceptions import ObjectDoesNotExist

from celery_rpc.tests import factories
from celery_rpc.tests.utils import (get_model_dict, SimpleModelTestMixin,
                                    get_model_dict_from_list, unpack_exception)
from django.test import TestCase
from django.db.models import Q
from rest_framework import serializers
from .. import tasks
from ..exceptions import ModelTaskError, remote_exception_registry
from ..tests.tasks import CustomModelTask
from .models import SimpleModel, NonAutoPrimaryKeyModel, PartialUpdateModel


class BaseTaskTests(SimpleModelTestMixin, TestCase):
    pass


class FilterTaskTests(BaseTaskTests):
    """ Tests for selecting models located on RPC server.
    """

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

    def testFiltersWithQ(self):
        expected = get_model_dict(self.models[0])
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               filters_Q=Q(pk=expected['id']))
        self.assertEquals(expected, r.get()[0])

    def testFiltersWithLookupsAndQ(self):
        filter_ids = [m.id for m in self.models[3:]]
        filter_Q = Q(pk__lte=self.models[3].pk)
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               filters={'pk__in': filter_ids},
                               filters_Q=filter_Q)
        expected = get_model_dict(self.models[3])
        self.assertEquals(len(r.get()), 1)
        self.assertEquals(expected, r.get()[0])

    def testExclude(self):
        """ Exclude seems good.
        """
        exclude_ids = [m.pk for m in self.models[1:]]
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               exclude={'pk__in': exclude_ids})
        expected = get_model_dict(self.models[0])
        self.assertEquals(expected, r.get()[0])

    def testExcludeWithQ(self):
        """ Exclude with Q-object works nice.
        """
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               exclude_q=Q(pk__gte=self.models[1].pk))
        expected = get_model_dict(self.models[0])
        self.assertEquals(expected, r.get()[0])

    def testExcludeWithLookupsAndQ(self):
        """ Exclude all except first and last by mix of `exclude` and
        `exclude_Q` seems able.
        """
        exclude_char = [m.char for m in self.models[1:]]
        exclude_Q = Q(pk__lte=self.models[3].pk)
        r = tasks.filter.delay(self.MODEL_SYMBOL,
                               exclude={'char__in': exclude_char},
                               exclude_Q=exclude_Q)

        result = r.get()
        self.assertEquals(len(result), 2)
        for i in 4, 0:
            expected = get_model_dict(self.models[i])
            r = result.pop()
            self.assertEquals(expected, r)

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

    def checkSingleObjectDoesNotExist(self, expected_exc=ObjectDoesNotExist):
        with self.assertRaisesRegexp(expected_exc,
                                     r'matching query does not exist.'):
            with unpack_exception():
                self.task.delay(self.MODEL_SYMBOL,
                                {'char': str(uuid4()),
                                 'id': randint(100, 1000)}).get()

    def testSingleObjectDoesNotExist(self):
        """ Raise exception if cannot find object in single mode  """
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = False
        return self.checkSingleObjectDoesNotExist()

    def testSingleObjectDoesNotExistRemoteError(self):
        """ Perform testSingleObjectDoesNotExist with remote errors handling
        enabled."""
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = True
        return self.checkSingleObjectDoesNotExist(remote_exception_registry.RemoteError)


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
                            {'char': char_val, 'id': expected['id']},
                            serializer_cls=serializer_cls)
        self.assertDictEqual({'id': expected['id']}, r.get())

    def testNoExistSerializer(self):
        """ Test not existing serializer """
        char_val = str(uuid4())
        expected = get_model_dict(self.models[0])

        with self.assertRaises(ImportError):
            with unpack_exception():
                self.task.delay(self.MODEL_SYMBOL,
                                {'char': char_val, 'id': expected['id']},
                                serializer_cls='not.existing.symbol').get()

    def testNoExistSerializerRemoteError(self):
        """ Perform testNoExistSerializer with remote errors handling
        in another mode."""
        old = tasks.rpc.conf['WRAP_REMOTE_ERRORS']
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = not old
        return self.testNoExistSerializer()

    def testNoValidSerializer(self):
        """ Test not valid serializer """
        char_val = str(uuid4())
        expected = get_model_dict(self.models[0])

        with self.assertRaisesRegexp(TypeError, r'not a DRF serializer'):
            serializer_cls = 'celery_rpc.tests.models:SimpleModel'
            with unpack_exception():
                self.task.delay(self.MODEL_SYMBOL,
                                {'char': char_val, 'id': expected['id']},
                                serializer_cls=serializer_cls).get()

    def testNoValidSerializerRemoteError(self):
        """ Perform testNoValidSerializer with remote errors handling
        in another mode."""
        old = tasks.rpc.conf['WRAP_REMOTE_ERRORS']
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = not old
        return self.testNoValidSerializer()


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
        m = factories.PartialUpdateModelFactory()
        preserve_f2 = m.f2
        expected = randint(1, 1000)
        r = self.task.delay('celery_rpc.tests.models:PartialUpdateModel',
                            {'f1': expected, 'pk': m.pk})
        r.get()
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

    def checkSingleObjectDoesNotExist(self, *args):
        """ Creates new object if provided ID does not exist """
        expected = str(uuid4())
        self.assertEquals(0, SimpleModel.objects.filter(char=expected).count())

        unexpected_id = randint(100, 1000)
        r = self.task.delay(self.MODEL_SYMBOL, {'char': expected,
                                                'id': unexpected_id})

        self.assertEquals(expected, r.get()['char'])
        self.assertNotEquals(unexpected_id, r.get()['id'])
        self.assertEquals(0, SimpleModel.objects.filter(
            char=unexpected_id).count())
        self.assertEquals(1, SimpleModel.objects.filter(
            char=expected).count())

    def testSingleObjectAlreadyExist(self):
        """ Raise exception if object already exists """
        pk = randint(1, 1000)
        obj = NonAutoPrimaryKeyModel.objects.create(pk=pk)
        with self.assertRaisesRegexp(
                ModelTaskError,
                r'primary key|PRIMARY KEY|This field must be unique'
                r'|with this id already exists') as ctx:
            with unpack_exception():
                r = self.task.delay(
                    'celery_rpc.tests.models:NonAutoPrimaryKeyModel',
                    {'id': obj.pk})
                r.get()
        self.assertNotEquals(self.models[0].id, ctx.exception.args[1]['id'])

    def testSingleObjectAlreadyExistRemoteError(self):
        """ Perform testSingleObjectAlreadyExist with remote errors handling
        in another mode."""
        old = tasks.rpc.conf['WRAP_REMOTE_ERRORS']
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = not old
        return self.testSingleObjectAlreadyExist()


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
        self.assertEquals(0, SimpleModel.objects.filter(
            id=expected['id']).count())

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


class TranslateTaskTests(BaseTaskTests):

    task = tasks.translate
    transform_map = {'title': 'char'}

    def _transform_keys(self, transform_map, data):
        result = {}
        for new_key, old_key in transform_map.items():
            if old_key in data.keys():
                result[new_key] = data[old_key]

        return result

    def testTransformDict(self):
        before = get_model_dict(self.models[0])
        after = self._transform_keys(self.transform_map, before)

        r = self.task.delay(self.transform_map, before)
        self.assertEquals(after, r.get())

    def testTransformList(self):
        before = get_model_dict_from_list(self.models)
        after = before[:]
        for index, el in enumerate(after):
            after[index] = self._transform_keys(self.transform_map, el)

        r = self.task.delay(self.transform_map, before)
        self.assertEquals(after, r.get())

    def testTransformWithDefaults(self):
        defaults = dict(bart='simpson')
        before = get_model_dict(self.models[0])
        after = self._transform_keys(self.transform_map, before)
        after.update(defaults)

        r = self.task.delay(self.transform_map, before, defaults=defaults)
        self.assertEquals(after, r.get())
