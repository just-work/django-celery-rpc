import datetime
import decimal
import json
import re

import six
import jsonpickle
from kombu.utils.encoding import bytes_t

try:
    # Django support
    from django.utils.functional import Promise
    from django.utils.encoding import smart_str
    from django.db.models import Q

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
    if isinstance(s, bytes_t):
        s = s.decode()
    return json.loads(s, cls=RpcJsonDecoder)