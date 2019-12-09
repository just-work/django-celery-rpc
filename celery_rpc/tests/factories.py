# coding=utf-8
import factory

from celery_rpc.tests import models


def create_m2m(field_name, field_factory=None):
    """ Вспомогательная функция для создания Many-To-Many полей
    для PostGeneration декрарации factory_boy.

    Сделано на основе документации по factory_boy:
    https://factoryboy.readthedocs.io/en/latest/recipes.html#simple-many-to-many-relationship

    Если указан field_factory, то поле поле заполняется единственным объектом,
    созданным указанной фабрикой.

    :param field_name: Имя поля, в которое будут добавляться объекты.
    :param field_factory: Фабрика для создания одного объекта.
    """
    def basic_m2m(obj, create, extracted, **kwargs):
        if not create:
            return

        if field_factory is not None:
            getattr(obj, field_name).add(field_factory.create())
        elif extracted:
            for item in extracted:
                getattr(obj, field_name).add(item)

    return basic_m2m


class SimpleModelFactory(factory.DjangoModelFactory):
    class Meta:
        model = models.SimpleModel

    char = factory.Sequence(lambda n: 'char{}'.format(n))


class NonAutoPrimaryKeyModelFactory(factory.DjangoModelFactory):
    class Meta:
        model = models.NonAutoPrimaryKeyModel


class PartialUpdateModelFactory(factory.DjangoModelFactory):
    class Meta:
        model = models.PartialUpdateModel

    f1 = factory.Sequence(lambda n: n)
    f2 = factory.Sequence(lambda n: n)


class FkSimpleModelFactory(factory.DjangoModelFactory):
    class Meta:
        model = models.FkSimpleModel

    fk = factory.SubFactory(SimpleModelFactory)


class ManyToManyModelFactory(factory.DjangoModelFactory):
    class Meta:
        model = models.ManyToManyModel

    m2m = factory.PostGeneration(create_m2m('m2m'))


