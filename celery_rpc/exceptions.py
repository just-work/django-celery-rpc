# coding: utf-8

from kombu.serialization import dumps, loads
from celery.backends.base import create_exception_cls

from celery_rpc.utils import symbol_by_name


class ModelTaskError(Exception):
    """ Base model tasks exception class
    """


class RestFrameworkError(ModelTaskError):
    """ REST framework encountered with problems while handling request
    """


class RemoteException(Exception):
    """ Wrapper for remote exceptions."""

    def __init__(self, exc, serializer='pickle'):
        """
        :param exc: Exception instance or RemoteException.args
        :type exc: BaseException subclass, list or tuple
        :param serializer: CELERY_RESULT_SERIALIZER for celery_rpc app
        :type serializer: str
        """
        if isinstance(exc, BaseException):
            cls = exc.__class__
            exc_args = dumps(exc.args, serializer=serializer)
            args = (cls.__module__, cls.__name__, exc_args)
        elif isinstance(exc, (list, tuple)):
            args = exc
        else:
            raise ValueError("Need a BaseException object")
        super(RemoteException, self).__init__(*args)

    def unpack_exception(self):
        module, name, args = self.args
        return remote_exception_registry.unpack_exception(module, name, args)


class RemoteExceptionRegistry(object):
    # XXX: TBD
    instance = None

    @classmethod
    def unpack_exception(cls, module, name, args):
        return cls.instance._unpack_exception(module, name, args)

    def __init__(self):
        self.__registry = {}
        self.__class__.instance = self

    def _unpack_exception(self, module, name, args):
        try:
            content_type, content_encoding, data = args
            fullname = "%s.%s" % (module, name)
            try:
                exc_class = symbol_by_name(fullname)
            except AttributeError:
                exc_class = create_exception_cls(name, module)
            args = loads(data, content_type, content_encoding)
            return exc_class(*args)
        except (ImportError, ValueError):
            return None

remote_exception_registry = RemoteExceptionRegistry()