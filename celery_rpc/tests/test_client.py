from __future__ import absolute_import
from datetime import datetime
import mock

from django.test import TestCase
from rest_framework import serializers

from .. import config, utils
from ..client import Client
from .utils import SimpleModelTestMixin


class HighPriorityRequestTests(TestCase):
    """ High priority request tests
    """
    @classmethod
    def setUpClass(cls):
        """ Creates rpc-client object
        """
        cls.rpc_client = Client()
        cls.task_name = utils.FILTER_TASK_NAME

    def testPrepareNormalPriorityTask(self):
        """ Method `prepare_task` cook normal priority request correctly
        """
        signature = self.rpc_client.prepare_task(self.task_name, None, None)
        # Check, that default routing key is used
        self.assertNotIn('routing_key', signature.options)

    def testPrepareHighPriorityTask(self):
        """ Method `prepare_task` cook high priority request correctly
        """
        signature = self.rpc_client.prepare_task(self.task_name, None, None,
                                                 high_priority=True)
        self.assertEquals(config.CELERY_DEFAULT_ROUTING_KEY + '.high_priority',
                          signature.options['routing_key'])

    def _assertProxyMethodSupportHighPriority(self, method_name, *args,
                                              **kwargs):
        method = getattr(self.rpc_client, method_name)
        args = ['fake_model_or_function_name'] + list(args)
        kwargs.update(high_priority=True, async=True)
        with mock.patch.object(Client, 'send_request') as _send_request:
            method(*args, **kwargs)
        # Get first parameter of args - Celery subtask signature
        signature = _send_request.call_args[0][0]
        msg = 'RPC-client method `{}` does not support high'.format(method_name)
        msg += ' priority requests'
        self.assertEquals(config.CELERY_DEFAULT_ROUTING_KEY + '.high_priority',
                          signature.options.get('routing_key'), msg)

    def testHighPriorityFilter(self):
        """ Method `filter` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('filter')

    def testHighPriorityUpdate(self):
        """ Method `update` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('update', {})

    def testHighPriorityGetset(self):
        """ Method `getset` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('getset', {})

    def testHighPriorityUpdateOrCreate(self):
        """ Method `update_or_create` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('update_or_create', {})

    def testHighPriorityCreate(self):
        """ Method `create` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('create', {})

    def testHighPriorityDelete(self):
        """ Method `delete` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('delete', {})

    def testHighPriorityCall(self):
        """ Method `call` support high priority requests
        """
        self._assertProxyMethodSupportHighPriority('call')


class AlterIdentityTests(SimpleModelTestMixin, TestCase):
    """ Access to models with alter identity field (not only PK field)
    """
    @classmethod
    def setUpClass(cls):
        """ Creates rpc-client object
        """
        __import__('celery_rpc.tasks')
        cls.rpc_client = Client()

    def setUp(self):
        super(AlterIdentityTests, self).setUp()
        self.data = {'datetime': datetime.max, 'char': self.models[0].char}
        self.kwargs = {'identity': 'char'}

    def testUpdate(self):
        """ Update with alter identity field looks good.
        """
        r = self.rpc_client.update(self.MODEL_SYMBOL, self.data, self.kwargs)
        dt = serializers.DateTimeField().to_representation(datetime.max)
        self.assertEqual(dt, r['datetime'])
        self.assertEqual(datetime.max,
                         self.MODEL.objects.get(pk=self.models[0].pk).datetime)

    def testDelete(self):
        """ Update with alter identity field looks good.
        """
        r = self.rpc_client.delete(self.MODEL_SYMBOL, self.data, self.kwargs)
        self.assertIsNone(r)
        self.assertFalse(self.MODEL.objects.filter(pk=self.models[0].pk).exists())
