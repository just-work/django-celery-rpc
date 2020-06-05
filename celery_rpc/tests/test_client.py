from __future__ import absolute_import

import random
import socket
from datetime import datetime
import mock

from django.test import TestCase
from rest_framework import serializers

from celery_rpc.base import DRF3
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
        super(HighPriorityRequestTests, cls).setUpClass()
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
        # TODO: replace with nowait=True
        kwargs.update(high_priority=True, **{'async': True})
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
        super(AlterIdentityTests, cls).setUpClass()
        cls.rpc_client = Client()

    def setUp(self):
        super(AlterIdentityTests, self).setUp()
        self.dt = datetime.max.replace(microsecond=998000)
        self.data = {'datetime': self.dt,
                     'char': self.models[0].char}
        self.kwargs = {'identity': 'char'}

    def testUpdate(self):
        """ Update with alter identity field looks good.
        """
        r = self.rpc_client.update(self.MODEL_SYMBOL, self.data, self.kwargs)
        if DRF3:
            dt = serializers.DateTimeField().to_representation(self.dt)
        else:
            dt = self.dt
        self.assertEqual(dt, r['datetime'])
        self.assertEqual(self.dt,
                         self.MODEL.objects.get(pk=self.models[0].pk).datetime)

    def testDelete(self):
        """ Update with alter identity field looks good.
        """
        r = self.rpc_client.delete(self.MODEL_SYMBOL, self.data, self.kwargs)
        self.assertIsNone(r)
        self.assertFalse(self.MODEL.objects.filter(pk=self.models[0].pk).exists())


class SetRefererTests(SimpleModelTestMixin, TestCase):
    """ Client set referer header when calling tasks
    """

    @classmethod
    def setUpClass(cls):
        """ Creates rpc-client object
        """
        super(SetRefererTests, cls).setUpClass()
        cls.rpc_client = Client()
        cls.task_name = utils.FILTER_TASK_NAME

    def testSetRefererHeader(self):
        """ Method `prepare_task` set referer header
        """
        signature = self.rpc_client.prepare_task(self.task_name, None, None)
        self.assertEqual(
            signature.options["headers"],
            {"referer": "@".join([config.RPC_CLIENT_NAME, socket.gethostname()])})


class TaskExpireTests(TestCase):
    """ Tests expiry time for tasks
    """
    @classmethod
    def setUpClass(cls):
        super(TaskExpireTests, cls).setUpClass()
        cls.rpc_client = Client()
        cls.test_expires = 123
        cls.method_names = ['call', 'create', 'delete', 'update', 'filter',
                            'getset', 'update_or_create']

    def _assertExpires(self, method_name, expected_expires,  **kwargs):
        method = getattr(self.rpc_client, method_name)
        args = ['fake_model_or_function_name', {}]
        mock_name = 'celery_rpc.tasks.{}.apply_async'.format(method_name)
        kwargs.update(nowait=False)
        with mock.patch(mock_name) as _apply_async:
            method(*args, **kwargs)

        expires = _apply_async.call_args[1].get('expires', None)
        self.assertIsNotNone(expires)
        self.assertEqual(expected_expires, expires)

    def testExpiresDefault(self):
        """ Client uses default value for task expiration if timeout is None
        """
        method_name = random.choice(self.method_names)

        self._assertExpires(method_name, config.GET_RESULT_TIMEOUT)

    def testExpiresFromTimeout(self):
        """ Client uses timeout value for task expiration
        """
        method_name = random.choice(self.method_names)

        self._assertExpires(method_name, self.test_expires,
                            timeout=self.test_expires)
