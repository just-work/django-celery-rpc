# coding: utf-8
from __future__ import absolute_import
from unittest import expectedFailure

from django.test import TransactionTestCase

from ..client import Pipe, Client
from .utils import SimpleModelTestMixin
from .models import SimpleModel


class PipelineTests(SimpleModelTestMixin, TransactionTestCase):
    """ Pipeline related tests.
    """

    def setUp(self):
        super(PipelineTests, self).setUp()
        self.client = Client()

    @property
    def pipe(self):
        return self.client.pipe()

    def testClientCanCreatePipe(self):
        """ Client able to start pipelineю
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


