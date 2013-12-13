# coding: utf-8

import os


def setup(module):
    os.environ["DJANGO_SETTINGS_MODULE"] = "app.settings"
    from django.core import management
    from django.db.models.loading import get_app
    try:
        management.install(get_app("your.app"))
    except SystemExit:
        pass


def teardown(module):
    from django.db import connection
    # explicit disconnect to destroy the in-memory db
    connection.close()