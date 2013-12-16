
from django.test import TestCase
from celery_rpc.tests.models import SimpleModel


class FilterTaskTests(TestCase):

    def testSimple(self):
        self.assertEqual(1, SimpleModel.objects.count())
