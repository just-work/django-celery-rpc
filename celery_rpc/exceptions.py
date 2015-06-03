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
    """ remote exception stub registry

    Allows to instantiate or acquire remote exception stubs for using on the
    client side.
    """

    class RemoteError(Exception):
        """ Parent class for all remote exception stubs."""

    def __init__(self):
        self.__registry = {}

    def unpack_exception(self, module, name, args):
        """ Instantiates exception stub for original exception

        :param module: module name for original exception
        :param name: class name for original exception
        :param args: RemoteException.args
        :return: new constructed exception
        :rtype: self.RemoteError subclass
        """
        try:
            # unpacking RemoteException args
            content_type, content_encoding, data = args
            try:
                # trying to import original exception
                original = symbol_by_name("%s.%s" % (module, name))
                # creating parent class for original error and self.RemoteError
                parent = type("Remote" + name, (original, self.RemoteError),
                              {'__module__': module})
            except (AttributeError, ImportError):
                # alternative way for unknown errors
                parent = self.RemoteError

            # create and cache exception stub class
            if name not in self.__registry:
                self.__registry[name] = create_exception_cls(
                    name, module, parent=parent)
            exc_class = self.__registry[name]

            # deserialize exception args and instantiate exception object
            args = loads(data, content_type, content_encoding)
            return exc_class(*args)
        except ValueError:
            # loads error
            return None

    def __getattr__(self, item):
        """ creates exception stub class for all missing attributes.
        """
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if item not in self.__registry:
                exception = create_exception_cls(item, "celery_rpc.exceptions",
                                                 parent=self.RemoteError)
                self.__registry[item] = exception
            return self.__registry[item]

    def subclass(self, parent, name):
        """ creates exception stub class with custom parent exception."""
        if name not in self.__registry:
            exception = create_exception_cls(name, "celery_rpc.exceptions",
                                             parent=parent)
            self.__registry[name] = exception
        return self.__registry[name]

    def flush(self):
        self.__registry = {}

# Global remote exception registry
remote_exception_registry = RemoteExceptionRegistry()
