from __future__ import absolute_import
import os

from celery.exceptions import TimeoutError

from . import utils
from .config import GET_RESULT_TIMEOUT
from .exceptions import RestFrameworkError

TEST_MODE = bool(os.environ.get('CELERY_RPC_TEST_MODE', False))


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

    def prepare_task(self, task_name, args, kwargs, high_priority=False,
                     **options):
        """ Prepare subtask signature

        :param task_name: task name like 'celery_rpc.filter' which exists
            in `_task_stubs`
        :param kwargs: optional parameters of request
        :param args: optional parameters of request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: celery.canvas.Signature instance

        """
        task = self._task_stubs[task_name]
        if high_priority:
            conf = task.app.conf
            options['routing_key'] = conf['CELERY_HIGH_PRIORITY_ROUTING_KEY']
        return task.subtask(args=args, kwargs=kwargs, **options)

    def filter(self, model, kwargs=None, async=False, timeout=None, retries=1,
               high_priority=False, **options):
        """ Call filtering Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param async: enables delayed collecting of result
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

        :param **options: optional parameter of apply_async
        :return: list of filtered objects or AsyncResult if async is True
        :raise: see get_result()

        """
        args = (model, )
        signature = self.prepare_task(utils.FILTER_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def update(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call update Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        signature = self.prepare_task(utils.UPDATE_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def getset(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call update Django model objects on server and return previous state

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: dict with old state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        signature = self.prepare_task(utils.GETSET_TASK_NAME, args, kwargs,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def update_or_create(self, model, data, kwargs=None, async=False,
                         timeout=None, retries=1, high_priority=False, **options):
        """ Call update Django model objects on server. If there is not for some
        data, then a new object will be created.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        signature = self.prepare_task(utils.UPDATE_OR_CREATE_TASK_NAME, args,
                                    kwargs, high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def create(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call create Django model objects on server.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        signature = self.prepare_task(utils.CREATE_TASK_NAME, args,
                                      kwargs, high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def delete(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, high_priority=False, **options):
        """ Call delete Django model objects on server.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict (or list with dicts), which can contains ID
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: None or [] if multiple delete or AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        signature = self.prepare_task(utils.DELETE_TASK_NAME, args, kwargs,
                                    high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def call(self, function, args=None, kwargs=None, async=False, timeout=None,
             retries=1, high_priority=False, **options):
        """ Call function on server

        :param function: full name of model symbol like 'package.module:Class'
        :param args: list with positional parameters of function
        :param kwargs: dict with named parameters of function
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param high_priority: ability to speedup consuming of the task
            if server support prioritization, by default False
        :param **options: optional parameter of apply_async
        :return: result of function call or AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        args = (function, args, kwargs)
        signature = self.prepare_task(utils.CALL_TASK_NAME, args, None,
                                      high_priority=high_priority, **options)
        return self.send_request(signature, async, timeout, retries)

    def get_result(self, async_result, timeout=None):
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
            return async_result.get(timeout=timeout)
        except TimeoutError:
            raise self.TimeoutError('Timeout exceeded while waiting for results')
        except RestFrameworkError:
            # !!! Not working with JSON serializer
            raise
        except Exception as e:
            raise self.ResponseError(
                'Something goes wrong while getting results', e)

    def pipe(self):
        """ Create pipeline for RPC request
        :return: Instance of Pipe
        """
        return Pipe(self)

    def send_request(self, signature, async=False, timeout=None, retries=1):
        """ Sending request to a server

        :param signature: Celery signature instance
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :return: results or AsyncResult if async is True or
            exception if something goes wrong
        :raise RestFrameworkError: error in the middle of Django REST
            Framework at server (if async=False).
        :raise Client.ResponseError: something goes wrong (if async=False)

        """
        while True:
            try:
                try:
                    r = signature.apply_async()
                except Exception as e:
                    raise self.RequestError(
                        'Something goes wrong while sending request', e)
                if async:
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
        self._client = client
        self._pipeline = []

    def _clone(self):
        p = Pipe(self._client)
        p._pipeline = self._pipeline.copy()
        return p

    def _push(self, task):
        p = self._clone()
        p._pipeline.append(task)
        return p

    def run(self, async=False, timeout=None, retries=1, high_priority=False,
            **options):
        """ Run pipeline - send chain of RPC request to server.
        :return: list of result of each chained request.
        """
        task_name = self._client.PIPE_TASK_NAME
        signature = self._client.prepare_task(task_name, (self._pipeline,), None,
                                              high_priority=high_priority, **options)
        return self._client.send_request(signature, async, timeout, retries)

    def _prepare_task(self, task_name, args, kwargs, options=None):
        return dict(name=task_name, args=args, kwargs=kwargs,
                    options=options or {})

    def filter(self, model, kwargs=None):
        task = self._prepare_task(self._client.FILTER_TASK_NAME, (model, ),
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
        args = []
        options = {}
        if data:
            args.append(data)
        else:
            options['transformer'] = True

        task = self._prepare_task(self._client.DELETE_TASK_NAME, (model, ),
                                  kwargs, options)
        return self._push(task)

    def update(self, model, data, kwargs=None):
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        task = self.prepare_task(utils.UPDATE_TASK_NAME, args, kwargs)
        return self._push(task)

    def update_or_create(self, model, data, kwargs=None):
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        task = self.prepare_task(utils.UPDATE_OR_CREATE_TASK_NAME, args, kwargs)
        return self._push(task)

    def getset(self, model, data, kwargs=None):
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        task = self.prepare_task(utils.GETSET_TASK_NAME, args, kwargs)
        return self._push(task)

    def create(self, model, data, kwargs=None):
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        task = self.prepare_task(utils.CREATE_TASK_NAME, args, kwargs)
        return self._push(task)

    def call(self, function, args, kwargs):
        task = self.prepare_task(utils.CALL_TASK_NAME, args, kwargs)
        return self._push(task)


# Copy task names into client class from utils
for n, v in utils.TASK_NAME_MAP.items():
    setattr(Client, n, v)
