# coding: utf-8
from __future__ import absolute_import

from django import VERSION as django_version

from celery_rpc.tests import factories
from .. import tasks
from .utils import get_model_dict
from .test_tasks import BaseTaskTests


class ManyToManyUpdateTests(BaseTaskTests):
    """ Test for add/remove m2m relations
    """
    M2M_THROUGH_SYMBOL = 'celery_rpc.tests.models:ManyToManyModel.m2m.through'

    def setUp(self):
        super(ManyToManyUpdateTests, self).setUp()
        self.m2m_model = factories.ManyToManyModelFactory()

    def testAdd(self):
        """ Add m2m relations working fine
        """
        if django_version[0] > 1:
            self.m2m_model.m2m.set([self.models[0]])
        else:
            self.m2m_model.m2m = [self.models[0]]

        # # pre-conditions
        self.assertEquals(1, self.m2m_model.m2m.count())

        data = {'manytomanymodel': self.m2m_model.pk,
                'simplemodel': self.models[1].pk}
        r = tasks.create.delay(self.M2M_THROUGH_SYMBOL, data)

        self.assertTrue(r.successful())
        self.assertEquals(2, self.m2m_model.m2m.count())

        expected = self.m2m_model.m2m.get(id=self.models[1].pk)
        expected = get_model_dict(expected)
        self.assertEquals(expected, get_model_dict(self.models[1]))

    def testDelete(self):
        """ Remove m2m relations working fine
        """
        if django_version[0] > 1:
            self.m2m_model.m2m.set(self.models[:2])
        else:
            self.m2m_model.m2m = self.models[:2]

        # pre-conditions
        self.assertEquals(2, self.m2m_model.m2m.count())

        through = self.m2m_model.m2m.through.objects

        delete = through.get(simplemodel=self.models[0])
        # FIXME actually is not a good idea drop relations by PK
        data = {'pk': delete.pk}
        r = tasks.delete.delay(self.M2M_THROUGH_SYMBOL, data)

        self.assertTrue(r.successful())
        self.assertEquals(1, self.m2m_model.m2m.count())

        expected = get_model_dict(self.models[1])
        value = get_model_dict(self.m2m_model.m2m.all()[0])
        self.assertEquals(expected, value)