#!/usr/bin/env python
from setuptools import setup

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Operating System :: POSIX',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Software Development',
    'Topic :: Utilities',
]

setup(
    author='davo',
    author_email='davo.fastcall@gmail.com',
    name='s3sync',
    description='S3 sync tool',
    version='0.1.0',
    url='https://bitbucket.org/davo/s3sync/',
    platforms=CLASSIFIERS,
    install_requires=[
        'argcomplete',
        'boto',
        'reprint',
        'pyyaml',
    ],
    entry_points={'console_scripts': [
        's3sync = s3sync.sync:main',
    ]},
    data_files=[
        ('/usr/share/bash-completion/completions/', [
            'extras/completion/s3sync'])
    ],
    packages=['s3sync'],
    include_package_data=False,
    zip_safe=False,
    test_suite='tests',
    python_requires='~=3.0',
)
