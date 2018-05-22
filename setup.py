# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from setuptools import setup


setup(
    name='torpeewee',
    version='1.0.1',
    packages=['torpeewee'],
    install_requires=[
        'peewee>=3.2.2'
    ],
    extras_require={
        'tornado': ['tornado>=5.0'],
        'tormysql': ['tormysql>=0.3.8'],
        'asyncpg': ['aiopg>=0.14.0'],
    },
    author=['snower'],
    author_email=['sujian199@gmail.com'],
    url='https://github.com/snower/torpeewee',
    license='MIT',
    keywords=[
        "tornado", "asyncio", "mysql", "postgresql", "orm", "tormysql", "asyncpg"
    ],
    description='Tornado and asyncio asynchronous ORM by peewee',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
