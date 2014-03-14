from __future__ import absolute_import

from django.test import TestCase


class ClientTests(TestCase):
    """ Test celery RPC client
    """
    # TODO Check high_priority supported