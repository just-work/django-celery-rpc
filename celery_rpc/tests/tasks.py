# coding: utf-8

from celery_rpc.tests import setup, teardown

from django.test import TestCase
from celery_rpc.tests.app.models import SimpleModel


class FilterTaskTests(TestCase):

    def testSimple(self):
        self.assertEqual(1, SimpleModel.objects.count())
