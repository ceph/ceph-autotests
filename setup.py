#!/usr/bin/python
from setuptools import setup, find_packages, Extension

setup(
    name='teuthology',
    version='0.0.1',
    packages=find_packages(),

    author='Tommi Virtanen',
    author_email='tommi.virtanen@dreamhost.com',
    description='Ceph Autotest infrastructure',
    license='MIT',
    keywords='ceph autotest',

    install_requires=[
        'restish >=0.11',
        ],

    entry_points={
        'paste.app_factory': [
            'main=teuthology.web:app_factory',
            ],
        },
    )
