from __future__ import unicode_literals

try:
    from django.utils.functional import Promise
    from django.utils.encoding import force_unicode
    has_django = True
except ImportError:
    has_django = False
import datetime
import decimal
import json
import jsonpickle


class XJSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time/timedelta,
    decimal types, and generators.

    Copy from
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
        return super(XJSONEncoder, self).default(o)

    if has_django:
        # Handling django-specific classes only if django package is installed
        def default(self, o):
            if isinstance(o, Promise):
                return force_unicode(o)
            elif isinstance(o, Q):
                return jsonpickle.encode(o)
            else:
                return self._default(o)
    else:
        default = _default
