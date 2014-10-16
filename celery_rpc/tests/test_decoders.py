# coding: utf-8
from __future__ import unicode_literals
from __future__ import absolute_import

from django.test import TestCase
from django.db.models import Q
from ..decoders import x_json_decoder_object_hook


class DecoderTaskTests(TestCase):

    def testDecoderObjectHook(self):
        example = {
            'q_filter': '{"py/object": "django.db.models.query_utils.Q", "connector": "AND", "children": [], "negated": false}',
            'filter': {'id__exact': 123}
        }
        res = x_json_decoder_object_hook(example)
        self.assertTrue(isinstance(res['q_filter'], Q))

    def testDecoderDeepObjectHook(self):
        example = {
            'a': {
                'b': {
                    'c': {
                        'q_filter': '{"py/object": "django.db.models.query_utils.Q", "connector": "AND", "children": [], "negated": false}',
                    }
                }
            }
        }
        res = x_json_decoder_object_hook(example)
        self.assertTrue(isinstance(res['a']['b']['c']['q_filter'], Q))

