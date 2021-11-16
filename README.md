django-celery-rpc
=================

[![Build Status](https://github.com/just-work/django-celery-rpc/workflows/build/badge.svg?branch=master&event=push)](https://github.com/just-work/django-celery-rpc/actions?query=event%3Apush+branch%3Amaster+workflow%3Abuild)
[![codecov](https://codecov.io/gh/just-work/django-celery-rpc/branch/master/graph/badge.svg)](https://codecov.io/gh/just-work/django-celery-rpc)
[![PyPI version](https://badge.fury.io/py/django-celery-rpc.svg)](https://badge.fury.io/py/djangoceleryrpc)

Remote access from one system to models and functions of other one using Celery machinery.

Relies on three outstanding python projects:

 - [Celery](http://www.celeryproject.org/)
 - [Django Rest Framework](http://www.djang)
 - [Django](https://www.djangoproject.com/)

## Main features

Client and server are designed to:

 - filter models with Django ORM lookups, Q-objects and excludes;
 - change model state (create, update, update or create, delete);
 - change model state in bulk mode (more than one object per request);
 - atomic get-set model state with bulk mode support;
 - call function;
 - client does not require Django;
 
## Installation
Install client:
```shell script
pip install djangoceleryrpc
```
Install server:
```shell script
pip install djangoceleryrpc[server]
```

## Basic Configuration

Default configuration of **django-celery-rpc** must be overridden in settings.py by **CELERY_RPC_CONFIG**.
The **CELERY_RPC_CONFIG** is a dict which must contains at least two keys: **BROKER_URL** and **CELERY_RESULT_BACKEND**.
Any Celery config params also permitted
(see [Configuration and defaults](http://celery.readthedocs.org/en/latest/configuration.html))

### server **span**

setting.py:

```python
# minimal required configuration
CELERY_RPC_CONFIG = {
	'BROKER_URL': 'amqp://guest:guest@rabbitmq:5672//',
	'CELERY_RESULT_BACKEND': 'redis://redis:6379/0',
}
```

### server **eggs**

setting.py:

```python
# alternate request queue and routing key
CELERY_RPC_CONFIG = {
	'BROKER_URL': 'amqp://guest:guest@rabbitmq:5672/',
	'CELERY_RESULT_BACKEND': 'amqp://guest:guest@rabbitmq:5672/',
	'CELERY_DEFAULT_QUEUE': 'celery_rpc.requests.alter_queue',
	'CELERY_DEFAULT_ROUTING_KEY': 'celery_rpc.alter_routing_key'
}
```

### client

setting.py:

```python
# this settings will be used in clients by default
CELERY_RPC_CONFIG = {
	'BROKER_URL': 'amqp://guest:guest@rabbitmq:5672/',
	'CELERY_RESULT_BACKEND': 'redis://redis:6379/0',
}

# 'eggs' alternative configuration will be explicitly passed to the client constructor
CELERY_RPC_EGGS_CLIENT = {
	# BROKER_URL will be used by default from section above
	'CELERY_RESULT_BACKEND': 'amqp://guest:guest@rabbitmq:5672/',
	'CELERY_DEFAULT_QUEUE': 'celery_rpc.requests.alter_queue',
	'CELERY_DEFAULT_ROUTING_KEY': 'celery_rpc.alter_routing_key'
}
```

*Note:
1. client and server must share the same __BROKER_URL__, __RESULT_BACKEND__, __DEFAULT_EXCHANGE__, __DEFAULT_QUEUE__, __DEFAULT_ROUTING_KEY__
2. different server must serve different request queues with different routing keys or must work with different exchanges*

example.py

```python
from celery_rpc.client import Client
from django.conf import settings

# create client with default settings
span_client = Client()

# create client for `eggs` server
eggs_client = Client(CELERY_RPC_EGGS_CLIENT)
```

## Using client

You can find more examples in tests.

### Filtering

Simple filtering example

```
span_client.filter('app.models:MyModel', kwargs=dict(filters={'a__exact':'a'}))
```

Filtering with Q object

```
from django.db.models import Q
span_client.filter('app.models:MyModel', kwargs=dict(filters_Q=(Q(a='1') | Q(b='1')))
```

Also, we can use both Q and lookups

```
span_client.filter('app.models:MyModel', kwargs=dict(filters={'c__exact':'c'}, filters_Q=(Q(a='1') | Q(b='1')))
```

Exclude supported

```
span_client.filter('app.models:MyModel', kwargs=dict(exclude={'c__exact':'c'}, exclude_Q=(Q(a='1') | Q(b='1')))
```

You can mix filters and exclude, Q-object with lookups. Try it yourself. ;)

Full list of available kwargs:

    filters - dict of terms compatible with django lookup fields
    offset - offset from which return a results
    limit - max number of results
    fields - list of serializer fields, which will be returned
    exclude - lookups for excluding matched models
    order_by - order of results (list, tuple or string),
        minus ('-') set reverse order, default = []
    filters_Q - django Q-object for filtering models
    exclude_Q - django Q-object for excluding matched models


List of all MyModel objects with high priority

```
span_client.filter('app.models:MyModel', high_priority=True)
```

### Creating

Create one object

```python
span_client.create('apps.models:MyModel', data={"a": "a"})
```

Bulk creating

```python
span_client.create('apps.models:MyModel', data=[{"a": "a"}, {"a": "b"}])
```

### Updating

Update one object by PK field name

```python
span_client.update('apps.models:MyModel', data={"id": 1, "a": "a"})
```

Update one object by special alias 'pk' which matched automatically to PK field

```python
span_client.update('apps.models:MyModel', data={"id": 1, "a": "a"})
```

Attention! Magic area! Update one object by any field you wish

```python
span_client.update('apps.models:MyModel', data={"alternative_key_field": 42, "a": "a"}, 
				   {'identity': 'alternative_key_field'})
```

### Update or create, Delete and so on

All cases are very similar. Try it you console!

### Full list of supported model methods
 
 - `filter` - select models
 - `create` - create new models, raise exception if model exists
 - `update` - update existing models
 - `update_or_create` - update if exist or create new
 - `delete` - delete existing models
 - `getset` - set new state and return old state atomically 
 
All method support options:

 - `fields` - shrink result fields
 - `serializer_cls` - fully qualified symbol name to DRF serializer class on server
 - `identity` - field name which will be used rather than PK field (mindless for `filter`)

### Pipe

It's possible to pipeline tasks, so they will be executed in one transaction.

```python
p = span_client.pipe()
p = p.create('apps.models:MyModel', data={"a": "a"})
p = p.create('apps.models:MyAnotherModel', data={"b": "b"})
p.run()
```

You can pass some arguments from previous task to the next.

Suppose you have those models on the server

```python
class MyModel(models.Model):
    a = models.CharField()
    
class MyAnotherModel(models.Model):
    fk = models.ForeignKey(MyModel)
    b = models.CharField()
```

You need to create instance of MyModel and instance of MyAnotherModel which reffers to MyModel

```python
p = span_client.pipe()
p = p.create('apps.models:MyModel', data={"a": "a"})
p = p.translate({"fk": "id"}, defaults={"b": "b"})
p = p.create('apps.models:MyAnotherModel')
p.run()
```

In this example the `translate` task: 
 - take result of the previous `create` task
 - extract value of "id" field from it
 - add this value to "defaults" by key "fk"
 
After that next `create` task takes result of `translate` as input data

### Add/delete m2m relations

Lets take such models:

```python
class MyModel(models.Model):
    str = models.CharField()
    
class MyManyToManyModel(models.Model):
    m2m = models.ManyToManyField(MyModel, null=True)
```

Add relation between existing objects

```python
my_models = span_client.create('apps.models:MyModel', 
							   [{'str': 'monthy'}, {'str': 'python'}])
m2m_model = span_client.create('apps.models:MyManyToManyModel',
                               {'m2m': [my_models[0]['id']]})

# Will add 'python' to m2m_model.m2m where 'monty' already is
data = {'mymodel': my_models[1]['id'], 'mymanytomanymodel': m2m_model['id']}
through = span_client.create('apps.models:MyManyToManyModel.m2m.through', data)
```

And then delete some of existing relations

```python
# Next `pipe` will eliminate all relations where `mymodel__str` equals 'monty'
p = span_client.pipe()
p = p.filter('apps.models:MyManyToManyModel.m2m.through', {'mymodel__str': 'monthy'})
p = p.delete('apps.models:MyManyToManyModel.m2m.through')
p.run()
```

## Run server instance

```python
celery worker -A celery_rpc.app
```

Server with support task consuming prioritization

```python
celery multi start 2 -A celery_rpc.app -Q:1 celery_rpc.requests.high_priority
```

*Note, you must replace 'celery_rpc.request' with actual value of config param __CELERY_DEFAULT_QUEUE__*

Command will start two instances. First instance will consume from high priority queue only. Second instance will serve both queues.

For daemonization see [Running the worker as a daemon](http://celery.readthedocs.org/en/latest/tutorials/daemonizing.html)

## Run tests

```shell
python django-celery-rpc/celery_rpc/runtests/runtests.py
```

## More Configuration

### Overriding base task class

```python
OVERRIDE_BASE_TASKS = {
    'ModelTask': 'package.module.MyModelTask',
    'ModelChangeTask': 'package.module.MyModelChangeTask',
    'FunctionTask': 'package.module.MyFunctionTask'
}


```
Supported class names: `ModelTask`, `ModelChangeTask`, `FunctionTask`

### Handling remote exceptions individually

```python
# Both server and client
CELERY_RPC_CONFIG['WRAP_REMOTE_ERRORS'] = True
```

After enabling remote exception wrapping client will raise same errors happened
 on the server side.
If client side has no error defined (i.e. no package installed), 
`Client.RemoteError` will be raised.
Also, `Client.RemoteError` is a base for all exceptions on the client side.

For unknown exceptions this code is valid:

```python
try:
    result = rpc_client.call("remote_func")
except rpc_client.errors.SomeUnknownError as e:
    # here a stub for remote SomeUnknownError is handled
    print (e.args)
```

For known exceptions both variants work:

```python

try:
    result = rpc_client.call("remote_func")
except rpc_client.errors.MultipleObjectsReturned as e:
    # django.core.exceptions.MultipleObjectsReturned
    handle_error(e)
except django.core.exceptions.ObjectDoesNotExist as e:
    # django.core.exceptions.ObjectDoesNotExist 
    handle_error(e)
```

If original exception hierarchy is needed:

```python

SomeBaseError = rpc_client.errors.SomeBaseError

DerivedError = rpc_client.errors.subclass(SomeBaseError, "DerivedError")
```


## TODO

 - Set default non-generic model serializer.
 - Test support for RPC result backend from Celery.
 - Token auth and permissions support (like DRF).
 - Resource map and strict mode.
 - ...
 
## Acknowledgements

Thanks for all who contributing to this project:
 - https://github.com/voron3x
 - https://github.com/dtrst
 - https://github.com/anatoliy-larin
 - https://github.com/bourivouh
 - https://github.com/tumb1er
