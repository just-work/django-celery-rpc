# coding: utf-8

# $Id: $
import json

from celery.utils.serialization import UnpickleableExceptionWrapper
from django.core.exceptions import PermissionDenied
from django.test import TestCase
import mock

from celery_rpc import tasks
from celery_rpc.client import Client
from celery_rpc import exceptions
from celery_rpc.exceptions import RestFrameworkError
from celery_rpc.tests.utils import SimpleModelTestMixin


class RemoteErrorsTestMixin(SimpleModelTestMixin):
    def setUp(self):
        super(RemoteErrorsTestMixin, self).setUp()
        self.serializer = tasks.rpc.conf['CELERY_RESULT_SERIALIZER']
        self._wrap_errors = tasks.rpc.conf['WRAP_REMOTE_ERRORS']
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = True

    def tearDown(self):
        super(RemoteErrorsTestMixin, self).tearDown()
        tasks.rpc.conf["WRAP_REMOTE_ERRORS"] = self._wrap_errors

    def testCallTask(self):
        self.assertErrorTunnelException(
            'call',
            'celery_rpc.base.FunctionTask.function',
            args=('celery_rpc.tests.utils.fail', (), {}),
        )

    def testFilterTask(self):
        self.assertErrorTunnelException(
            'filter',
            'celery_rpc.base.ModelTask._import_model',
            args=(self.MODEL_SYMBOL,),
        )

    def testUpdateTask(self):
        self.assertErrorTunnelException(
            'update',
            'celery_rpc.base.ModelChangeTask._import_model',
            args=(self.MODEL_SYMBOL, {'char': 'abc'}),
        )

    def testGetSetTask(self):
        self.assertErrorTunnelException(
            'getset',
            'celery_rpc.base.ModelChangeTask._import_model',
            args=(self.MODEL_SYMBOL, {'char': 'abc'}),
        )

    def testUpdateOrCreateTask(self):
        self.assertErrorTunnelException(
            'update_or_create',
            'celery_rpc.base.ModelChangeTask._import_model',
            args=(self.MODEL_SYMBOL, {'char': 'abc'}),
        )

    def testCreateTask(self):
        self.assertErrorTunnelException(
            'create',
            'celery_rpc.base.ModelTask._import_model',
            args=(self.MODEL_SYMBOL, {'char': 'abc'}),
        )

    def testDeleteTask(self):
        self.assertErrorTunnelException(
            'delete',
            'celery_rpc.base.ModelChangeTask._import_model',
            args=(self.MODEL_SYMBOL, {'char': 'abc'}),
        )

    def assertErrorTunnelException(self, task, patch, args=(), kwargs=None):
        raise NotImplementedError()


class ErrorTunnelServerTestCase(RemoteErrorsTestMixin, TestCase):
    def gettestee(self, name):
        return getattr(tasks, name)

    def assertErrorTunnelException(self, name, patch, args=(), kwargs=None):
        kwargs = kwargs or {}
        error = ValueError(100500)

        task = self.gettestee(name)

        with mock.patch(patch, side_effect=error):
            r = task.apply(args=args, kwargs=kwargs)
        remote_exception_stub = r.get(propagate=False)
        expected = exceptions.RemoteException(error, serializer=self.serializer)
        self.assertEqual(remote_exception_stub.__class__.__name__,
                         exceptions.RemoteException.__name__)
        self.assertTupleEqual(remote_exception_stub.args, expected.args)

    def testPackUnpackException(self):
        exc = exceptions.RemoteException(
            ValueError(100500),
            serializer=self.serializer)

        inner = exc.unpack_exception(self.serializer)
        self.assertIsInstance(inner,
                              exceptions.remote_exception_registry.ValueError)
        self.assertEqual(inner.args, (100500,))

    def testTunnelDisabled(self):
        error = ValueError(100500)
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = False
        task = self.gettestee('call')
        patch = 'celery_rpc.base.FunctionTask.function'
        args = ('celery_rpc.tests.utils.fail', (), {}),
        with mock.patch(patch, side_effect=error):
            r = task.apply(*args)
        remote_exception_stub = r.get(propagate=False)
        self.assertIsInstance(remote_exception_stub, ValueError)


class ErrorTunnelClientTestCase(RemoteErrorsTestMixin, TestCase):
    @classmethod
    def setUpClass(cls):
        """ Creates rpc-client object
        """
        super(ErrorTunnelClientTestCase, cls).setUpClass()
        cls.rpc_client = Client()

    def setUp(self):
        super(ErrorTunnelClientTestCase, self).setUp()
        self._wrap_errors = self.rpc_client._app.conf['WRAP_REMOTE_ERRORS']
        self.rpc_client._app.conf['WRAP_REMOTE_ERRORS'] = True
        self.serializer = self.rpc_client._app.conf['CELERY_RESULT_SERIALIZER']

    def tearDown(self):
        super(ErrorTunnelClientTestCase, self).tearDown()
        self.rpc_client._app.conf['WRAP_REMOTE_ERRORS'] = self._wrap_errors

    def gettestee(self, name):
        return getattr(self.rpc_client, name)

    def assertErrorTunnelException(self, name, patch, args=(), kwargs=None):
        kwargs = kwargs or {}
        error = RestFrameworkError(100500)

        method = self.gettestee(name)
        with mock.patch(patch, side_effect=error):
            with self.assertRaises(error.__class__) as r:
                method(*args, **kwargs)

        self.assertTupleEqual(r.exception.args, error.args)

    def testUnpackingFromTunnelDisabled(self):
        """ Error wrapping disabled on server, enabled on client."""
        error = ValueError(100500)
        tasks.rpc.conf['WRAP_REMOTE_ERRORS'] = False
        method = self.gettestee('call')
        patch = 'celery_rpc.base.FunctionTask.function'
        args = ('celery_rpc.tests.utils.fail', (), {})
        with mock.patch(patch, side_effect=error):
            with mock.patch('celery_rpc.utils.unpack_exception',
                            return_value=None) as unpack_mock:
                with self.assertRaises(self.rpc_client.ResponseError) as ctx:
                    method(*args)
        response_error = ctx.exception
        self.assertIsInstance(response_error.args[1], ValueError)
        # checking that non-wrapped exception is passed to unpacking helper
        # and that unpack flag is True.
        unpack_mock.assert_called_with(error, True, serializer=self.serializer)

    def testNotUnpackingFromTunnelEnabled(self):
        """ Error wrapping disabled on client, enabled on server."""
        error = ValueError(100500)
        serializer = tasks.rpc.conf['CELERY_RESULT_SERIALIZER']
        wrapped = exceptions.RemoteException(error, serializer)
        method = self.gettestee('call')
        patch = 'celery_rpc.base.FunctionTask.function'
        args = ('celery_rpc.tests.utils.fail', (), {})
        self.rpc_client._app.conf['WRAP_REMOTE_ERRORS'] = False
        with mock.patch(patch, side_effect=error):
            with mock.patch('celery_rpc.utils.unpack_exception',
                            return_value=None) as unpack_mock:
                with self.assertRaises(self.rpc_client.ResponseError) as ctx:
                    method(*args)
        response = ctx.exception
        remote_error = response.args[1]
        # checking that wrapped exception is passed to unpacking helper
        # and that unpack flag is False.

        unpack_mock.assert_called_with(remote_error, False,
                                       serializer=self.serializer)
        remote_error_cls = remote_error.__class__
        self.assertEqual(remote_error_cls.__name__, "RemoteException")
        self.assertEqual(remote_error_cls.__module__, "celery_rpc.exceptions")
        args = remote_error.args
        self.assertTupleEqual(args, wrapped.args)


class ErrorRegistryTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        """ Creates rpc-client object
        """
        super(ErrorRegistryTestCase, cls).setUpClass()
        cls.rpc_client = Client()

    def setUp(self):
        super(ErrorRegistryTestCase, self).setUp()
        self._wrap_errors = self.rpc_client._app.conf['WRAP_REMOTE_ERRORS']
        self.rpc_client._app.conf['WRAP_REMOTE_ERRORS'] = True
        self.registry = exceptions.remote_exception_registry
        self.registry.flush()
        self.module = ValueError.__module__
        self.name = "ValueError"
        self.args = (100500,)
        self.serializer = 'json'

    @property
    def data(self):
        return json.dumps([self.module, self.name, self.args])

    def tearDown(self):
        super(ErrorRegistryTestCase, self).tearDown()
        self.rpc_client._app.conf['WRAP_REMOTE_ERRORS'] = self._wrap_errors

    def testUnpackNativeException(self):
        exc = self.registry.unpack_exception(self.data, self.serializer)
        self.assertIsInstance(exc, ValueError)
        self.assertIsInstance(exc, self.registry.RemoteError)
        self.assertTupleEqual(exc.args, self.args)

    def testUnpackExistingException(self):
        self.module = "django.core.exceptions"
        self.name = "PermissionDenied"
        exc = self.registry.unpack_exception(self.data, self.serializer)
        self.assertIsInstance(exc, PermissionDenied)
        self.assertIsInstance(exc, self.registry.RemoteError)
        self.assertTupleEqual(exc.args, self.args)

    def testUnpackUnknownException(self):
        self.module = "nonexistent.module"
        self.name = "NonexistentError"
        exc = self.registry.unpack_exception(self.data, self.serializer)
        self.assertIsInstance(exc, self.registry.RemoteError)
        self.assertTupleEqual(exc.args, self.args)

    def assertRemoteErrorInClient(self, error=None):
        error = error or ValueError(100500)
        with mock.patch('celery_rpc.base.FunctionTask.function',
                        side_effect=error):
            with self.assertRaises(self.registry.RemoteError) as ctx:
                self.rpc_client.call('celery_rpc.tests.utils.fail')
        self.assertIsInstance(ctx.exception, error.__class__)

    def testClientRemoteErrorBaseClasses(self):
        self.assertRemoteErrorInClient()  # Native error
        self.assertRemoteErrorInClient(error=PermissionDenied("WTF"))

    def testExceptNativeRemoteError(self):
        with self.assertRaises(self.rpc_client.errors.ValueError):
            raise self.registry.unpack_exception(self.data, self.serializer)

    def testExceptExistingRemoteError(self):
        self.module = "django.core.exceptions"
        self.name = "PermissionDenied"
        with self.assertRaises(self.rpc_client.errors.PermissionDenied):
            raise self.registry.unpack_exception(self.data, self.serializer)

    def testExceptUnknownRemoteError(self):
        self.module = "nonexistent.module"
        self.name = "NonexistentError"
        with self.assertRaises(self.rpc_client.errors.NonexistentError):
            raise self.registry.unpack_exception(self.data, self.serializer)

    def testRemoteErrorHierarchy(self):
        parent = self.rpc_client.errors.IndexError
        error = self.rpc_client.errors.subclass(parent, "ValueError")
        exc = self.registry.unpack_exception(self.data, self.serializer)

        self.assertIsInstance(exc, self.rpc_client.errors.RemoteError)
        self.assertIsInstance(exc, self.rpc_client.errors.IndexError)
        self.assertIsInstance(exc, self.rpc_client.errors.ValueError)
