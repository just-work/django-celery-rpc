import os
import re
import subprocess
from setuptools import setup, find_packages  # type: ignore
# TODO: use pathlib in get_version
base_dir = os.path.dirname(__file__)

try:
    with open(os.path.join(base_dir, 'README.md')) as f:
        long_description = f.read()
except OSError:
    long_description = None

version_re = re.compile('^Version: (.+)$', re.M)
package_name = 'djangoceleryrpc'


def get_version():
    """
    Reads version from git status or PKG-INFO

    https://gist.github.com/pwithnall/7bc5f320b3bdf418265a
    """
    # noinspection PyUnresolvedReferences
    git_dir = os.path.join(base_dir, '.git')
    if os.path.isdir(git_dir):
        # Get the version using "git describe".
        cmd = 'git describe --tags --match [0-9]*'.split()
        try:
            version = subprocess.check_output(cmd).decode().strip()
        except subprocess.CalledProcessError:
            return None

        # PEP 386 compatibility
        if '-' in version:
            version = '.post'.join(version.split('-')[:2])

        # Don't declare a version "dirty" merely because a time stamp has
        # changed. If it is dirty, append a ".dev1" suffix to indicate a
        # development revision after the release.
        with open(os.devnull, 'w') as fd_devnull:
            subprocess.call(['git', 'status'],
                            stdout=fd_devnull, stderr=fd_devnull)

        cmd = 'git diff-index --name-only HEAD'.split()
        try:
            dirty = subprocess.check_output(cmd).decode().strip()
        except subprocess.CalledProcessError:
            return None

        if dirty != '':
            version += '.dev1'
    else:
        # Extract the version from the PKG-INFO file.
        try:
            with open('PKG-INFO') as v:
                version = version_re.search(v.read()).group(1)
        except OSError:
            version = None

    return version


setup(
    name=package_name,
    version=get_version() or 'dev',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    url='https://github.com/just-work/django-celery-rpc',
    license='Public',
    author='axeman',
    author_email='alex.manaev@gmail.com',
    description='Remote access from one system to models and functions of '
                'another one using Celery machinery.',
    install_requires=[
        'celery >=3.1.5, <5.3.0',
        'jsonpickle >=0.8.0, <2.1.0',
        'six',
    ],
    extras_require={
        'server': [
            'django >=1.3, <4.1', 
            'djangorestframework >= 2.3, <3.14',
        ],
    },
    tests_require=[
        'nose>=1.0',
        'django >=1.3, <4.1',
        'djangorestframework >= 2.3, <3.13',
        'django-nose >= 1.2, <1.5',
        'factory-boy==2.8.1',
        'mock',
    ],
    test_suite='runtests.runtests',
    classifiers=[
        'Development Status :: 6 - Mature',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Framework :: Django',
        'Framework :: Django :: 1.4',
        'Framework :: Django :: 1.5',
        'Framework :: Django :: 1.6',
        'Framework :: Django :: 1.7',
        'Framework :: Django :: 1.8',
        'Framework :: Django :: 1.9',
        'Framework :: Django :: 1.10',
        'Framework :: Django :: 1.11',
        'Framework :: Django :: 2.0',
        'Framework :: Django :: 2.1',
        'Framework :: Django :: 2.2',
        'Framework :: Django :: 3.0',
        'Framework :: Django :: 3.1',
        'Framework :: Django :: 3.2',
        'Framework :: Django :: 4.0',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ]
)
