# coding: utf-8
import six
from celery.backends.base import create_exception_cls
from kombu.exceptions import ContentDisallowed
from kombu.serialization import dumps, loads, registry
from kombu.utils.encoding import from_utf8

from celery_rpc.utils import symbol_by_name, DEFAULT_EXC_SERIALIZER


class ModelTaskError(Exception):
    """ Base model tasks exception class
    """


class RestFrameworkError(ModelTaskError):
    """ REST framework encountered with problems while handling request
    """


class RemoteException(Exception):
    """ Wrapper for remote exceptions."""

    def __init__(self, exc, serializer=DEFAULT_EXC_SERIALIZER):
        """
        :param exc: Exception instance or RemoteException.args
        :type exc: BaseException subclass, list or tuple
        :param serializer: CELERY_RESULT_SERIALIZER for celery_rpc app
        :type serializer: str
        """
        if isinstance(exc, BaseException):
            cls = exc.__class__
            exc_args = exc.args
            args = (cls.__module__, cls.__name__, exc_args)
            args = [dumps(args, serializer=serializer)[2]]
        elif isinstance(exc, (list, tuple)):
            args = exc
        elif isinstance(exc, six.string_types):
            args = [exc]
        else:
            raise ValueError("Need a BaseException object")
        super(RemoteException, self).__init__(*args)

    def unpack_exception(self, serializer):
        return remote_exception_registry.unpack_exception(
            self.args[0], serializer)


class RemoteExceptionRegistry(object):
    """ remote exception stub registry

    Allows to instantiate or acquire remote exception stubs for using on the
    client side.
    """

    class RemoteError(Exception):
        """ Parent class for all remote exception stubs."""

    def __init__(self):
        self.__registry = {}

    def unpack_exception(self, data, serializer):
        """ Instantiates exception stub for original exception

        :param module: module name for original exception
        :param name: class name for original exception
        :param args: RemoteException.args
        :return: new constructed exception
        :rtype: self.RemoteError subclass
        """
        try:
            # unpacking RemoteException args
            content_type, content_encoding, dumps = registry._encoders[serializer]

            data = loads(data, content_type, content_encoding)
            module, name, args = data
            try:
                # trying to import original exception
                original = symbol_by_name("%s.%s" % (module, name))
                # creating parent class for original error and self.RemoteError

                class_name = from_utf8("Remote" + name)
                parent = type(class_name, (original, self.RemoteError),
                              {'__module__': module})
            except (AttributeError, ImportError):
                # alternative way for unknown errors
                parent = self.RemoteError

            # create and cache exception stub class
            if name not in self.__registry:
                self.__registry[name] = create_exception_cls(
                    from_utf8(name), module, parent=parent)
            exc_class = self.__registry[name]

            return exc_class(*args)
        except (ValueError, ContentDisallowed):
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
