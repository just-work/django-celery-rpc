from __future__ import absolute_import

from celery.exceptions import TimeoutError

from .config import GET_RESULT_TIMEOUT
from .exceptions import RestFrameworkError
from .utils import create_celery_app


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

    FILTER_TASK_NAME = 'celery_rpc.filter'
    UPDATE_TASK_NAME = 'celery_rpc.update'
    UPDATE_OR_CREATE_TASK_NAME = 'celery_rpc.update_or_create'
    CREATE_TASK_NAME = 'celery_rpc.create'
    DELETE_TASK_NAME = 'celery_rpc.delete'
    CALL_TASK_NAME = 'celery_rpc.call'

    TASK_NAMES = (FILTER_TASK_NAME, UPDATE_TASK_NAME, CALL_TASK_NAME, )

    _app = None
    _task_stubs = None

    def __init__(self, app_config=None):
        """ Adjust server interaction parameters

        :param app_config: alternative configuration parameters for Celery app.

        """
        self._app = create_celery_app(config=app_config)
        self._task_stubs = self._register_stub_tasks(self._app)


    def prepare_task(self, task_name, args, kwargs, **options):
        """ Prepare subtask signature

        :param task_name: task name like 'celery_rpc.filter' which exists
            in `_task_stubs`
        :param kwargs: optional parameters of request
        :param args: optional parameters of request
        :param **options: optional parameter of apply_async
        :return: celery.canvas.Signature instance

        """
        task = self._task_stubs[task_name]
        return task.subtask(args=args, kwargs=kwargs, **options)

    def filter(self, model, retries=1, kwargs=None, async=False, timeout=None,
               **options):
        """ Call filtering Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param kwargs: optional parameters of request
            filters - dict of terms compatible with django database query
            offset - offset from which return a results
            limit - max number results
        :param **options: optional parameter of apply_async
        :return: list of filtered objects or AsyncResult if async is True
        :raise: see get_result()

        """
        args = (model, )
        subtask = self.prepare_task(self.FILTER_TASK_NAME, args, kwargs,
                                    **options)
        return self._send_request(subtask, async, timeout, retries)

    def update(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, **options):
        """ Call update Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param **options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        subtask = self.prepare_task(self.UPDATE_TASK_NAME, args, kwargs,
                                    **options)
        return self._send_request(subtask, async, timeout, retries)

    def update_or_create(self, model, data, kwargs=None, async=False,
                         timeout=None, retries=1, **options):
        """ Call update Django model objects on server. If there is not for some
        data, then a new object will be created.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param **options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        subtask = self.prepare_task(self.UPDATE_OR_CREATE_TASK_NAME, args,
                                    kwargs, **options)
        return self._send_request(subtask, async, timeout, retries)

    def create(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, **options):
        """ Call create Django model objects on server.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param **options: optional parameter of apply_async
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        subtask = self.prepare_task(self.CREATE_TASK_NAME, args,
                                    kwargs, **options)
        return self._send_request(subtask, async, timeout, retries)

    def delete(self, model, data, kwargs=None, async=False, timeout=None,
               retries=1, **options):
        """ Call delete Django model objects on server.

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict (or list with dicts), which can contains ID
        :param kwargs: optional parameters of request (dict)
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param **options: optional parameter of apply_async
        :return: None or [] if multiple delete or AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        subtask = self.prepare_task(self.DELETE_TASK_NAME, args, kwargs,
                                    **options)
        return self._send_request(subtask, async, timeout, retries)

    def call(self, function, args=None, kwargs=None, async=False, timeout=None,
             retries=1, **options):
        """ Call function on server

        :param function: full name of model symbol like 'package.module:Class'
        :param args: list with positional parameters of function
        :param kwargs: dict with named parameters of function
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param retries: number of tries to send request
        :param **options: optional parameter of apply_async
        :return: result of function call or AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        args = (function, args, kwargs)
        subtask = self.prepare_task(self.CALL_TASK_NAME, args, None,
                                    **options)
        return self._send_request(subtask, async, timeout, retries)

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

    def _send_request(self, signature, async=False, timeout=None, retries=1):
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
        for name in cls.TASK_NAMES:
            @app.task(bind=True, name=name)
            def task_stub(*args, **kwargs):
                pass
            tasks[name] = task_stub
        return tasks
