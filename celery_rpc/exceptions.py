

class ModelTaskError(Exception):
    """ Base model tasks exception class
    """


class RestFrameworkError(ModelTaskError):
    """ REST framework encountered with problems while handling request
    """
