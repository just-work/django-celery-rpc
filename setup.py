from distutils.core import setup

setup(
    name='django-celery-rpc',
    version='0.4',
    packages=['celery_rpc'],
    url='https://github.com/ttyS15/django-celery-rpc',
    license='Public',
    author='axeman',
    author_email='alex.manaev@gmail.com',
    description='Remote access from one system to models and functions of '
                'another one using Celery machinery.',
    install_requires=[
        'celery >=3.1.5, <3.2.0',
        'django >=1.3, <1.7',
        'djangorestframework >= 2.3, <2.4'
    ],
)
