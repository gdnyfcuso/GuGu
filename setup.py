#!/usr/bin/env python

from setuptools import setup, find_packages

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ''


setup(
    name='GuGu',
    version='0.1.0',
    description='an api for getting financial data',
    long_description=long_description,
    author='TabQ',
    author_email='16621596@qq.com',
    url='http://www.infodata.cc',
    license="Apache License, Version 2.0",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'requests',
        'lxml',
        'simplejson',
        'xlrd'
    ],
    keywords='Financial Data Crawler',
    classifiers=['Development Status :: 4 - Beta',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5'],
)
