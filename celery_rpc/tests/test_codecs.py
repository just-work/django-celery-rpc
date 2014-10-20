# coding: utf-8
from __future__ import absolute_import

import jsonpickle

from django.test import TestCase
from django.db.models import Q
from kombu import serialization


class RpcJsonCodecTests(TestCase):

    def testContentType(self):
        """ Encode with correct content-type.
        """
        serialized = serialization.dumps(None, 'x-rpc-json')
        self.assertEqual('application/json+celery-rpc:v1', serialized[0])

    def testSupportQ(self):
        """ Encoder/Decoder support Django Q-object
        """
        source = dict(q=Q(a=1) | Q(b=2) & Q(c=3))
        content_type, encoding, result = serialization.dumps(source, 'x-rpc-json')
        restored = serialization.loads(result, content_type, encoding)

        expected = jsonpickle.encode(source)
        result = jsonpickle.encode(restored)
        self.assertEqual(expected, result)

    def testAcceptJsonCeleryRpc(self):
        """ Accepted content type are correct.
        """
        from ..app import rpc
        accept = serialization.prepare_accept_content(
            rpc.conf.CELERY_ACCEPT_CONTENT)

        source = ('a', 1, None)
        for codec in ('json', 'x-json', 'x-rpc-json'):
            content_type, encoding, result = serialization.dumps(source, codec)
            restored = serialization.loads(result, content_type, encoding,
                                           accept=accept)
            self.assertEqual(list(source), restored)
