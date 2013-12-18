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

    _app = None
    _task_stubs = None

    def __init__(self, app_config=None):
        """ Adjust server interaction parameters

        :param app_config: alternative configuration parameters for Celery app.

        """
        self._app = create_celery_app(config=app_config)
        self._task_stubs = self._register_stub_tasks(self._app)

    def filter(self, model, async=False, timeout=None, **options):
        """ Call filtering Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param options: optional parameters of request
            filters - dict of terms compatible with django database query
            offset - offset from which return a results
            limit - max number results
        :return: list of filtered objects or AsyncResult if async is True
        :raise: see get_result()

        """
        task = self._task_stubs[self.FILTER_TASK_NAME]
        args = (model, )
        return self._send_request(task, args, options, async, timeout)

    def update(self, model, data, async=False, timeout=None, **options):
        """ Call update Django model objects on server

        :param model: full name of model symbol like 'package.module:Class'
        :param data: dict with new data or list of them
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param options: optional parameters of request
        :return: dict with updated state of model or list of them or
            AsyncResult if async is True
        :raise InvalidRequest: if data has non iterable type

        """
        if not hasattr(data, '__iter__'):
            raise self.InvalidRequest("Parameter 'data' must be a dict or list")
        args = (model, data)
        task = self._task_stubs[self.UPDATE_TASK_NAME]
        return self._send_request(task, args, options, async, timeout)

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

    def _send_request(self, task, args, kwargs, async=False, timeout=None,
                      **options):
        """ Sending request to a server

        :param task: Celery task
        :param args: args for apply_async
        :param kwargs: kwargs for apply_async
        :param async: enables delayed collecting of result
        :param timeout: timeout of waiting for results
        :param options: optional parameters for apply_async
        :return: results or AsyncResult if async is True or
            exception if something goes wrong
        :raise RestFrameworkError: error in the middle of Django REST
            Framework at server (if async=False).
        :raise Client.ResponseError: something goes wrong (if async=False)

        """
        try:
            r = task.apply_async(args=args, kwargs=kwargs, **options)
        except Exception as e:
            raise self.RequestError(
                'Something goes wrong while sending request', e)
        if async:
            return r
        else:
            return self.get_result(r, timeout)

    @classmethod
    def _register_stub_tasks(cls, app):
        """ Bind fake tasks to the app

        :param app: celery application
        :return: dict {task_name: task_stub)

        """
        names = (cls.FILTER_TASK_NAME, cls.UPDATE_TASK_NAME)
        tasks = {}
        for name in names:
            @app.task(bind=True, name=name)
            def task_stub(*args, **kwargs):
                pass
            tasks[name] = task_stub
        return tasks
