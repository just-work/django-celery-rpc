from __future__ import absolute_import

import os
import socket
import warnings

from celery.exceptions import TimeoutError
from celery.utils import nodename

from . import utils
from .config import GET_RESULT_TIMEOUT
from .exceptions import RestFrameworkError, remote_exception_registry

TEST_MODE = bool(os.environ.get('CELERY_RPC_TEST_MODE', False))


def _async_to_nowait(nowait=False, **kwargs):
    if 'async' in kwargs:
        warnings.warn("async parameter name is deprecated for python3.5+")
        nowait = kwargs.pop('async')
    return nowait


class Client(object):
    """ Sending requests to server and translating results
    """

    class Error(Exception):
        """ Base client error
        """

    class InvalidRequest(Error):
        """ Request contains invalid params or some params are missed
        """

    class RequestError(Error):
        """ Error of sending request
        """

    class ResponseError(Error):
        """ Error of getting result
        """

    class TimeoutError(Error):
        """ Timeout while getting result
        """

    _app = None
    _task_stubs = None

    def __init__(self, app_config=None):
        """ Adjust server interaction parameters

        :param app_config: alternative configuration parameters for Celery app.

        """
        self._app = utils.create_celery_app(config=app_config)
        if TEST_MODE:
            # XXX Working ONLY while tests running
            from .app import rpc
            self._task_stubs = rpc.tasks
        else:
            self._task_stubs = self._register_stub_tasks(self._app)

        self.errors = remote_exception_registry

    def get_client_name(self):
        return nodename(self._app.conf.get("RPC_CLIENT_NAME"),
                        socket.gethostname())

    def prepare_task(self, task_name, args, kwargs, high_priority=False,
                     **options):
        """ Prepare subtask signature

        :param task_name: task name like 'celery_rpc.filter' which exists
            in `_task_stubs`
        :param kwargs: optional parameters of request
        :param args: optional parameters of request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: celery.canvas.Signature instance

        """
        task = self._task_stubs[task_name]
        options.setdefault("headers", {})
        options["headers"]["referer"] = self.get_client_name()
        if high_priority:
            conf = task.app.conf
            options['routing_key'] = conf['CELERY_HIGH_PRIORITY_ROUTING_KEY']
        return task.subtask(args=args, kwargs=kwargs, **options)

    def filter(self, model, kwargs=None, nowait=False, timeout=None, retries=1,
               high_priority=False, **options):
        """ Call filtering Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param kwargs: optional parameters of request
            filters - dict of terms compatible with django database query
            offset - offset from which return a results
            limit - max number of results
            fields - list of serializer fields, which will be returned
            exclude - lookups for excluding matched models
            order_by - order of results (list, tuple or string),
                minus ('-') set reverse order, default = []
            filters_Q - django Q-object for filtering models
            exclude_Q - django Q-object for excluding matched models

        :param options: optional parameter of apply_async
        :return: list of filtered objects or AsyncResult if nowait is True
        :raise: see get_result()

        """
        nowait = _async_to_nowait(nowait, **options)
        args = (model, )
        signature = self.prepare_task(utils.FILTER_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, nowait, timeout, retries)

    def update(self, model, data, kwargs=None, nowait=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call update Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if nowait is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        nowait = _async_to_nowait(nowait, **options)
        args = (model, data)
        signature = self.prepare_task(utils.UPDATE_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, nowait, timeout, retries)

    def getset(self, model, data, kwargs=None, nowait=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call update Django model objects on server and return previous state

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: dict with old state of model or list of them or
            AsyncResult if nowait is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        nowait = _async_to_nowait(nowait, **options)
        args = (model, data)
        signature = self.prepare_task(utils.GETSET_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, nowait, timeout, retries)

    def update_or_create(self, model, data, kwargs=None, nowait=False,
                         timeout=None, retries=1, high_priority=False, **options):
        """ Call update Django model objects on server. If there is not for some
        data, then a new object will be created.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if nowait is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        nowait = _async_to_nowait(nowait, **options)
        signature = self.prepare_task(
            utils.UPDATE_OR_CREATE_TASK_NAME, args, kwargs,
            high_priority=high_priority, **options)
        return self.send_request(signature, nowait, timeout, retries)

    def create(self, model, data, kwargs=None, nowait=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call create Django model objects on server.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if nowait is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        nowait = _async_to_nowait(nowait, **options)
        args = (model, data)
        signature = self.prepare_task(
            utils.CREATE_TASK_NAME, args, kwargs, high_priority=high_priority,
            **options)
        return self.send_request(signature, nowait, timeout, retries)

    def delete(self, model, data, kwargs=None, nowait=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call delete Django model objects on server.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict (or list with dicts), which can contains ID
        :param kwargs: optional parameters of request (dict)
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: None or [] if multiple delete or AsyncResult if nowait is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        nowait = _async_to_nowait(nowait, **options)
        signature = self.prepare_task(utils.DELETE_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, nowait, timeout, retries)

    def call(self, function, args=None, kwargs=None, nowait=False, timeout=None,
             retries=1, high_priority=False, **options):
        """ Call function on server

        :param function: full name of model symbol like 'package.module:Class'
        :param args: list with positional parameters of function
        :param kwargs: dict with named parameters of function
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param options: optional parameter of apply_async
        :return: result of function call or AsyncResult if nowait is True
        :raise InvalidRequest: if data has non iterable type

        """
        args = (function, args, kwargs)
        nowait = _async_to_nowait(nowait, **options)
        signature = self.prepare_task(utils.CALL_TASK_NAME, args, None,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, nowait, timeout, retries)

    def get_result(self, async_result, timeout=None, **options):
        """ Collect results from delayed result object

        :param async_result: Celery AsyncResult object
        :param timeout: timeout of waiting for results
        :return: results or exception if something goes wrong
        :raise RestFrameworkError: error in the middle of Django REST
            Framework at server (only is serializer is pickle or yaml)
        :raise Client.ResponseError: something goes wrong

        """
        timeout = timeout or GET_RESULT_TIMEOUT

        try:
            return async_result.get(timeout=timeout, **options)
        except TimeoutError:
            raise self.TimeoutError('Timeout exceeded while waiting for results')
        except RestFrameworkError:
            # !!! Not working with JSON serializer
            raise
        except Exception as e:
            exc = self._unpack_exception(e)
            if not exc:
                exc = self.ResponseError(
                    'Something goes wrong while getting results', e)
            raise exc

    def _unpack_exception(self, error):
        wrap_errors = self._app.conf['WRAP_REMOTE_ERRORS']
        serializer = self._app.conf['CELERY_RESULT_SERIALIZER']
        return utils.unpack_exception(error, wrap_errors, serializer=serializer)

    def pipe(self):
        """ Create pipeline for RPC request
        :return: Instance of Pipe
        """
        return Pipe(self)

    def send_request(self, signature, nowait=False, timeout=None, retries=1,
                     **kwargs):
        """ Sending request to a server

        :param signature: Celery signature instance
        :param nowait: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param kwargs: compatibility parameters for async keyword argument
        :return: results or AsyncResult if nowait is True or
            exception if something goes wrong
        :raise RestFrameworkError: error in the middle of Django REST
            Framework at server (if nowait=False).
        :raise Client.ResponseError: something goes wrong (if nowait=False)

        """
        expires = timeout or GET_RESULT_TIMEOUT
        nowait = _async_to_nowait(nowait, **kwargs)
        while True:
            # noinspection PyBroadException
            try:
                try:
                    r = signature.apply_async(expires=expires)
                except Exception as e:
                    raise self.RequestError(
                        'Something goes wrong while sending request', e)
                if nowait:
                    return r
                else:
                    return self.get_result(r, timeout)
            except Exception:
                retries -= 1
                if retries <= 0:
                    raise

    @classmethod
    def _register_stub_tasks(cls, app):
        """ Bind fake tasks to the app

        :param app: celery application
        :return: dict {task_name: task_stub)

        """
        tasks = {}
        for name in utils.TASK_NAME_MAP.values():
            # noinspection PyUnusedLocal
            @app.task(bind=True, name=name, shared=False)
            def task_stub(*args, **kwargs):
                pass
            tasks[name] = task_stub
        return tasks


class Pipe(object):
    """ Builder of pipeline of RPC requests.
    """

    def __init__(self, client):
        if not client:
            raise ValueError("Rpc client is required for Pipe() constructing")
        self.client = client
        self._pipeline = []

    def _clone(self):
        p = Pipe(self.client)
        p._pipeline = self._pipeline[:]
        return p

    def _push(self, task):
        p = self._clone()
        p._pipeline.append(task)
        return p

    def run(self, nowait=False, timeout=None, retries=1, high_priority=False,
            **options):
        """ Run pipeline - send chain of RPC request to server.
        :return: list of result of each chained request.
        """
        task_name = utils.PIPE_TASK_NAME
        nowait = _async_to_nowait(nowait, **options)
        signature = self.client.prepare_task(
            task_name, (self._pipeline,), None, high_priority=high_priority,
            **options)
        return self.client.send_request(signature, nowait, timeout, retries)

    @staticmethod
    def _prepare_task(task_name, args, kwargs, options=None):
        return dict(name=task_name, args=args, kwargs=kwargs,
                    options=options or {})

    def filter(self, model, kwargs=None):
        task = self._prepare_task(utils.FILTER_TASK_NAME, (model, ),
                                  kwargs)
        return self._push(task)

    def delete(self, model, data=None, kwargs=None):
        """ Delete models identified by `data` or by result of previous request.

        If `data` missed acts as transformer accepted on data from output of
        previous task.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict (or list with dicts), which can contains ID
        :param kwargs:
        :return:
        """
        task = self._prepare_model_change_task(utils.DELETE_TASK_NAME, model,
                                               data, kwargs)
        return self._push(task)

    def update(self, model, data=None, kwargs=None):
        if data and not hasattr(data, '__iter__'):
            raise self.client.InvalidRequest(
                "Parameter 'data' must be a dict or list")

        task = self._prepare_model_change_task(utils.UPDATE_TASK_NAME, model,
                                               data, kwargs)
        return self._push(task)

    def update_or_create(self, model, data=None, kwargs=None):
        if data and not hasattr(data, '__iter__'):
            raise self.client.InvalidRequest(
                "Parameter 'data' must be a dict or list")

        task = self._prepare_model_change_task(utils.UPDATE_OR_CREATE_TASK_NAME,
                                               model, data, kwargs)
        return self._push(task)

    def getset(self, model, data=None, kwargs=None):
        if data and not hasattr(data, '__iter__'):
            raise self.client.InvalidRequest(
                "Parameter 'data' must be a dict or list")

        task = self._prepare_model_change_task(utils.GETSET_TASK_NAME, model,
                                               data, kwargs)
        return self._push(task)

    def create(self, model, data=None, kwargs=None):
        if data and not hasattr(data, '__iter__'):
            raise self.client.InvalidRequest(
                "Parameter 'data' must be a dict or list")

        task = self._prepare_model_change_task(utils.CREATE_TASK_NAME, model,
                                               data, kwargs)
        return self._push(task)

    def call(self, function, args, kwargs):
        args = (function, args)
        task = self._prepare_task(utils.CALL_TASK_NAME, args, kwargs)
        return self._push(task)

    def translate(self, mapping, kwargs=None):
        args = (mapping,)
        options = {'transformer': True}

        task = self._prepare_task(utils.TRANSLATE_TASK_NAME, args,
                                  kwargs, options)
        return self._push(task)

    def _prepare_model_change_task(self, task_name, model, data=None,
                                   kwargs=None):
        args = [model]
        options = {}
        if data:
            args.append(data)
        else:
            options['transformer'] = True

        return self._prepare_task(task_name, args, kwargs, options)

    def result(self, index, kwargs=None):
        args = (index,)
        options = {'transformer': True}

        task = self._prepare_task(utils.RESULT_TASK_NAME, args,
                                  kwargs, options)
        return self._push(task)


# Copy task names into client class from utils
for n, v in utils.TASK_NAME_MAP.items():
    setattr(Client, n, v)
