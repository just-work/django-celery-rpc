import datetime
import decimal
import json
import re
import uuid

import six
import jsonpickle
from kombu.serialization import registry

try:
    # Django support
    from django.utils.functional import Promise  # noqa
    from django.utils.encoding import smart_str  # noqa
    from django.db.models import Q  # noqa
    has_django = True
except ImportError:
    has_django = False


class RpcJsonEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time/timedelta,
    decimal types, Q-objects and generators.

    Originated from
    https://github.com/tomchristie/django-rest-framework/blob/master/rest_framework/utils/encoders.py

    """

    def _default(self, o):
        # For Date Time string spec, see ECMA 262
        # http://ecma-international.org/ecma-262/5.1/#sec-15.9.1.15
        if isinstance(o, datetime.datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith('+00:00'):
                r = r[:-6] + 'Z'
            return r
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, datetime.time):
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, datetime.timedelta):
            return str(o.total_seconds())
        elif isinstance(o, decimal.Decimal):
            return str(o)
        elif isinstance(o, uuid.UUID):
            return o.hex
        elif hasattr(o, 'tolist'):
            return o.tolist()
        elif hasattr(o, '__iter__'):
            return [i for i in o]
        return super(RpcJsonEncoder, self).default(o)

    if has_django:
        # Handling django-specific classes only if django package is installed
        def default(self, o):
            if isinstance(o, Promise):
                return smart_str(o)
            elif isinstance(o, Q):
                return jsonpickle.encode(o)
            else:
                return self._default(o)
    else:
        default = _default


class XJsonEncoder(RpcJsonEncoder):
    """ Backward compatibility for task serializing.
    """

    if has_django:
        def default(self, o):
            if isinstance(o, Q):
                raise RuntimeError("Django Q-objects does not supported by "
                                   "'x-json' codec. For running with Q-objects "
                                   "use celery_rpc>0.16 on both sides and "
                                   "set 'x-rpc-json' as task serializer for "
                                   "client")
            return super(XJsonEncoder, self).default(o)


class RpcJsonDecoder(json.JSONDecoder):
    """ Add support for Django Q-objects in dicts
    """
    Q_OBJECT_SIGNATURE = re.compile(
        r'"py/object": "django\.db\.models\.query_utils\.Q"')

    def __init__(self, *args, **kwargs):
        kwargs['object_hook'] = self._object_hook
        super(RpcJsonDecoder, self).__init__(*args, **kwargs)

    def _object_hook(self, val):
        """ Iterate through dict for additional conversion.
        """

        for k, v in six.iteritems(val):
            if (isinstance(v, six.string_types) and re.search(
                    self.Q_OBJECT_SIGNATURE, v)):
                val[k] = jsonpickle.decode(v)
        return val


def x_rpc_json_dumps(obj):
    return json.dumps(obj, cls=RpcJsonEncoder)


def x_rpc_json_loads(s):
    if isinstance(s, six.binary_type):
        s = s.decode()
    return json.loads(s, cls=RpcJsonDecoder)


# XXX: Compatibility for versions <= 0.16
def x_json_dumps(obj):
    return json.dumps(obj, cls=XJsonEncoder)


# XXX: Compatibility for versions <= 0.16
def x_json_loads(s):
    if isinstance(s, six.binary_type):
        s = s.decode()
    return json.loads(s)


def register_codecs():
    registry.register('x-rpc-json', x_rpc_json_dumps, x_rpc_json_loads,
                      'application/json+celery-rpc:v1', 'utf-8')
    # XXX: Compatibility for ver <= 0.16
    registry.register('x-json', x_json_dumps, x_json_loads,
                      'application/json', 'utf-8')
