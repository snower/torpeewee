# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from setuptools import setup


setup(
    name='torpeewee',
    version='0.0.5',
    packages=['torpeewee'],
    install_requires=[
        'tornado>=4.1',
        'peewee==2.9.1'
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
