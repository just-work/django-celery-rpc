#!/usr/bin/env python

from setuptools import setup, find_packages

# https://github.com/msabramo/virtualenv/commit/ddd0aa02cf822fc690ff9c4bfead70c3e6767eee
try:
    import multiprocessing
except ImportError:
    pass

setup(
    name='django-celery-rpc',
    version='1.0',
    packages=find_packages(),
    url='https://github.com/ttyS15/django-celery-rpc',
    license='Public',
    author='axeman',
    author_email='alex.manaev@gmail.com',
    description='Remote access from one system to models and functions of '
                'another one using Celery machinery.',
    install_requires=[
        'celery >=3.1.5, <3.2.0',
        'jsonpickle>=0.8.0',
        'six',
        # celery_rpc server still needs django and djangorestframework packages,
        # but they are unnecessary for client

        # 'django >=1.3, <1.10',
        # 'djangorestframework >= 2.3, <3.4'
    ],
    tests_require=[
        'nose>=1.0',
        'django >=1.3, <1.10',
        'djangorestframework >= 2.3, <3.4',
        'django-nose >= 1.2, <1.5',
        'django-autofixture>=0.3.2,<0.12.0',
        'mock==1.0.1',
    ],
    test_suite='runtests.runtests',
    include_package_data=True,
    classifiers=[
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7',
    ],
)
