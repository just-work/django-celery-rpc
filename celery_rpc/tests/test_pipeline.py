# coding: utf-8
from __future__ import absolute_import
from unittest import expectedFailure

from django.test import TestCase

from ..client import Pipe, Client
from .utils import SimpleModelTestMixin
from .models import SimpleModel


class PipelineTests(SimpleModelTestMixin, TestCase):
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
        """ Just one filter in chain works well.
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


