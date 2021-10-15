#!/usr/bin/env python

import sys
from optparse import OptionParser
import django

from django.conf import settings

if not settings.configured:
    from celery_rpc.runtests import settings as test_settings
    kwargs = {k: getattr(test_settings, k) for k in dir(test_settings)
              if not k.startswith('_') and k.isupper()}
    settings.configure(**kwargs)


from django_nose import NoseTestSuiteRunner

if django.VERSION >= (1, 7):
    # New Apps loading mechanism
    django.setup()

def runtests(*test_args, **kwargs):
    if 'south' in settings.INSTALLED_APPS:
        from south.management.commands import patch_for_test_db_setup
        patch_for_test_db_setup()

    if not test_args:
        test_args = ['celery_rpc']

    if sys.version_info >= (3, 10, 0):
        from django.test.runner import DiscoverRunner
        test_runner = DiscoverRunner(**kwargs)
    else:
        test_runner = NoseTestSuiteRunner(**kwargs)

    failures = test_runner.run_tests(test_args)
    sys.exit(failures)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--verbosity', dest='verbosity', action='store',
                      default=1, type=int)
    opts = getattr(NoseTestSuiteRunner, 'options', None)
    if opts:
        parser.add_options(opts)
    (options, args) = parser.parse_args()

    runtests(*args, **options.__dict__)
