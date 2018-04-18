# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from setuptools import setup


setup(
    name='torpeewee',
    version='0.0.7',
    packages=['torpeewee'],
    install_requires=[
        'tornado>=5.0',
        'peewee>=3.2.2'
    ],
    author=['snower'],
    author_email=['sujian199@gmail.com'],
    url='https://github.com/snower/torpeewee',
    license='MIT',
    keywords=[
        "tornado", "mysql", "postgresql", "orm", "tormysql", "momoko"
    ],
    description='Tornado asynchronous ORM by peewee',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
