# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from setuptools import setup


setup(
    name='torpeewee',
    version='0.0.1',
    packages=['torpeewee'],
    install_requires=[
        'tornado>=4.1',
        'TorMySQL>=0.2.5',
    ],
    author=['snower'],
    author_email=['sujian199@gmail.com'],
    url='https://github.com/snower/torpeewee',
    license='MIT',
    keywords=[
        "tornado", "mysql", "orm", "tormysql"
    ],
    description='Tornado asynchronous ORM by peewee',
    long_description=open("README.MD").read(),
    zip_safe=False,
)
