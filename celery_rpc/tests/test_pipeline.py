# coding: utf-8
from __future__ import absolute_import
from unittest import expectedFailure
from autofixture import AutoFixture

from django.test import TransactionTestCase

from ..client import Pipe, Client
from .utils import SimpleModelTestMixin
from .models import SimpleModel, FkSimpleModel


class BasePipelineTests(SimpleModelTestMixin, TransactionTestCase):
    """ Abstract base class for pipe tests.
    """

    def setUp(self):
        super(BasePipelineTests, self).setUp()
        self.client = Client()

    @property
    def pipe(self):
        return self.client.pipe()


class PipelineTests(BasePipelineTests):
    """ Pipeline related tests.
    """

    def testClientCanCreatePipe(self):
        """ Client able to start pipeline
        """
        p = self.client.pipe()
        self.assertIsInstance(p, Pipe)

    def testPipeCanSendRequest(self):
        """ Pipe can send complex request to RPC server.
        """
        r = self.client.pipe().run()
        self.assertEqual([], r)

    def testSeveralFilters(self):
        """ Several filters in the chain work well.
        """
        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[0].pk}))
        p = p.filter(self.MODEL_SYMBOL,
                     kwargs=dict(filters={'pk': self.models[1].pk}))

        r = p.run()

        expected = [[self.get_model_dict(self.models[0])],
                    [self.get_model_dict(self.models[1])]]
        self.assertEqual(expected, r)

    def testUpdate(self):
        """ Update works well in pipeline.
        """
        p = self.pipe.update(self.MODEL_SYMBOL,
                             {'pk': self.models[0].pk, 'char': 'hello'})
        r = p.run()

        m = SimpleModel.objects.get(pk=self.models[0].pk)
        self.assertEqual('hello', m.char)
        expected = [self.get_model_dict(m)]
        self.assertEqual(expected, r)

    def testAtomicPipeline(self):
        """ Pipeline is atomic by default.
        """
        p = self.pipe
        p = p.delete(self.MODEL_SYMBOL, self.get_model_dict(self.models[0]))
        p = p.delete('invalid model symbol raise exception', {})

        self.assertRaises(p.client.ResponseError, p.run)
        self.assertTrue(SimpleModel.objects.filter(pk=self.models[0].pk).exists())

    @expectedFailure
    def testPatchTransformer(self):
        """ TODO `patch` updates result of previous task.
        """
        p = self.pipe.filter(self.MODEL_SYMBOL,
                         kwargs=dict(filters={'pk': self.models[0].pk}))
        r = p.patch({'char': 'abc'})

        expected = [[self.get_model_dict(self.models[0])],
                    [self.get_model_dict(self.models[0])]]
        expected[1].update(char='abc')

        self.assertEqual(expected, r)


class TransformTests(BasePipelineTests):
    """ Tests on different transformation.
    """
    FK_MODEL_SYMBOL = 'celery_rpc.tests.models:FkSimpleModel'
    TRANSFORM_MAP = {'id': 'fk'}

    def setUp(self):
        super(TransformTests, self).setUp()

        self.model = AutoFixture(SimpleModel).create_one()
        self.fk_model = AutoFixture(
            FkSimpleModel, field_values={'fk': self.model}).create_one()

    def testDeleteTransformer(self):
        """ Delete transformation works well.
        """
        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[0].pk}))
        p = p.delete(self.MODEL_SYMBOL)
        r = p.run()

        expected = [[self.get_model_dict(self.models[0])], []]
        self.assertEqual(expected, r)
        self.assertRaises(SimpleModel.DoesNotExist,
                          SimpleModel.objects.get, pk=self.models[0].pk)

    def testCreateTransformer(self):
        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[0].pk}))
        p = p.translate(self.TRANSFORM_MAP)
        p = p.create(self.FK_MODEL_SYMBOL)
        r = p.run()

        self.assertTrue(FkSimpleModel.objects.get(**r[2][0]))

    def testUpdateOrCreateCreateTransformer(self):
        """ Test creating with update_or_create
        """
        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[0].pk}))
        p = p.translate(self.TRANSFORM_MAP)
        p = p.update_or_create(self.FK_MODEL_SYMBOL)
        r = p.run()

        self.assertTrue(FkSimpleModel.objects.get(**r[2][0]))

    def testUpdateOrCreateUpdateTransformer(self):
        self.assertNotEqual(self.fk_model.id, self.models[1].pk)

        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[1].pk}))
        p = p.translate(self.TRANSFORM_MAP,
                        kwargs=dict(defaults={'id': self.fk_model.id}))
        p = p.update_or_create(self.FK_MODEL_SYMBOL)
        r = p.run()

        expect_obj = FkSimpleModel.objects.get(**r[2][0])
        self.assertEquals(expect_obj.fk.id, self.models[1].pk)

    def testUpdateTransformer(self):
        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[0].pk}))
        p = p.translate(self.TRANSFORM_MAP,
                        kwargs=dict(defaults={'id': self.fk_model.id}))

        p = p.update(self.FK_MODEL_SYMBOL)
        r = p.run()

        self.assertEqual(r[2][0]['fk'], self.models[0].pk)

    def testGetSetTransformer(self):
        p = self.pipe.filter(self.MODEL_SYMBOL,
                             kwargs=dict(filters={'pk': self.models[3].pk}))
        p = p.translate(self.TRANSFORM_MAP,
                        kwargs=dict(defaults={'id': self.fk_model.id}))

        p = p.getset(self.FK_MODEL_SYMBOL)
        r = p.run()

        expect_obj = FkSimpleModel.objects.get(fk=self.models[3].pk)
        self.assertEquals(expect_obj.fk.id, self.models[3].pk)
        # return previous state
        self.assertNotEqual(r[2][0]['fk'], self.models[3].pk)
