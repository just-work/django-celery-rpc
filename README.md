django-celery-rpc
=================

[![Build Status](https://travis-ci.org/anatoliy-larin/django-celery-rpc.png?branch=master)](https://travis-ci.org/anatoliy-larin/django-celery-rpc)

Remote access from one system to models and functions of other one using Celery machinery.

Relies on three outstanding python projects:

 - [Celery](http://www.celeryproject.org/)
 - [Django Rest Framework](http://www.djang)
 - [Django](https://www.djangoproject.com/)

## Main features

Client and server are designed to:

 - filter models with Django ORM lookups;
 - change model state (create, update, update or create, delete);
 - change model state in bulk mode (more than one object per request);
 - atomic get-set model state with bulk mode support;
 - call function;

## Configure

Default configuration of **django-celery-rpc** must be overridden in settings.py by **CELERY_RPC_CONFIG**.
The **CELERY_RPC_CONFIG** is a dict which must contains at least two keys: **BROKER_URL** and **CELERY_RESULT_BACKEND**.
Any Celery config params also permitted
(see [Configuration and defaults](http://celery.readthedocs.org/en/latest/configuration.html))

### server **span**

setting.py:

```python
# minimal required configuration
CELERY_RPC_CONFIG = {
	'BROKER_URL': amqp://10.9.200.1/,
	'CELERY_RESULT_BACKEND': 'redis://10.9.200.2/0',
}
```

### server **eggs**

setting.py:

```python
# alternate request queue and routing key
CELERY_RPC_CONFIG = {
	'BROKER_URL': amqp://10.9.200.1/,
	'CELERY_RESULT_BACKEND': amqp://10.9.200.1/',
	'CELERY_DEFAULT_QUEUE': 'celery_rpc.requests.alter_queue',
	'CELERY_DEFAULT_ROUTING_KEY': 'celery_rpc.alter_routing_key'
}
```

### client/

setting.py:

```python
# this settings will be used in clients by default
CELERY_RPC_CONFIG = {
	'BROKER_URL': amqp://10.9.200.1/,
	'CELERY_RESULT_BACKEND': 'redis://10.9.200.2/0',
}

# 'eggs' alternative configuration will be explicitly passed to the client constructor
CELERY_RPC_EGGS_CLIENT = {
	# BROKER_URL will be used by default from section above
	'CELERY_RESULT_BACKEND': amqp://10.9.200.1/',
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

List of all MyModel objects with high priority

```
span_client.filter('app.models:MyModel', high_priority=True)
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

## TODO

 - Shrink fields of result object for **filter** method.
 - Set default non-generic model serializer.
 - Django-free mode for client and server.
 - Token auth and permissions support (like DRF).
