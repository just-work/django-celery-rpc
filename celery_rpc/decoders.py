from __future__ import unicode_literals

from six import string_types
import jsonpickle
import re


def x_json_decoder_object_hook(val):
    """
    Iter through dict for additional conversion
    """
    for k, v in val.iteritems():
        if isinstance(v, dict):
            x_json_decoder_object_hook(v)
        else:
            # django Q object decode
            reg_ex = '\"py/object\": \"django.db.models.query_utils.Q\"'
            if isinstance(v, string_types) and re.search(reg_ex, v):
                val[k] = jsonpickle.decode(v)
    return val