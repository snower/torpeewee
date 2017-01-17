torpeewee
=========

|Build Status|

Tornado asynchronous ORM by peewee

About
=====

torpeewee - Tornado asynchronous ORM by peewee.

Installation
============

::

    pip install torpeewee

Examples
========

.. code:: python

    # -*- coding: utf-8 -*-
    # 16/7/7
    # create by: snower

    import datetime
    from tornado import gen
    from tornado.ioloop import IOLoop
    from torpeewee import *

    db = MySQLDatabase("test", host="127.0.0.1", port=3306, user="root", passwd="123456")

    class BaseModel(Model):
        class Meta:
            database = db

    class Test(BaseModel):
        id = IntegerField(primary_key= True)
        data = CharField(max_length=64, null=False)
        count = IntegerField(default=0)
        created_at = DateTimeField()
        updated_at = DateTimeField()

    ioloop = IOLoop.instance()

    @db.transaction()
    @gen.coroutine
    def run_transaction(transaction):
        for i in (yield Test.use(transaction).select().order_by(Test.id.desc()).limit(2)):
            print i.id, i.data

        print ""
        t = yield Test.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                               updated_at=datetime.datetime.now())
        print t.id, t.data

        for i in (yield Test.select().order_by(Test.id.desc()).limit(2)):
            print i.id, i.data

        print ""
        for i in (yield Test.use(transaction).select().order_by(Test.id.desc()).limit(2)):
            print i.id, i.data

    @gen.coroutine
    def run():
        t = yield Test.select().where(Test.id == 5).first()
        print t

        c = yield Test.select().where(Test.id > 5).count()
        print c

        c = yield Test.select().where(Test.id > 5).group_by(Test.data).count()
        print c

        for i in (yield Test.select().where(Test.id > 5).where(Test.id<=10)):
            print i.id, i.data

        for i in (yield Test.select().order_by(Test.id.desc()).limit(2)):
            print i.id, i.data
        t = yield Test.create(data = "test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        print t.id, t.data
        for i in (yield Test.select().order_by(Test.id.desc()).limit(2)):
            print i.id, i.data

        print ""
        print ""

        t = yield Test.select().order_by(Test.id.desc()).limit(1)[0]
        print t.id, t.data, t.count
        t.count += 1
        yield t.save()
        t = yield Test.select().order_by(Test.id.desc()).limit(1)[0]
        print t.id, t.data, t.count

        print ""
        print ""

        with (yield db.transaction()) as transaction:
            for i in (yield Test.use(transaction).select().order_by(Test.id.desc()).limit(2)):
                print i.id, i.data

            print ""
            t = yield Test.use(transaction).create(data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
            print t.id, t.data

            for i in (yield Test.select().order_by(Test.id.desc()).limit(2)):
                print i.id, i.data

            print ""
            for i in (yield Test.use(transaction).select().order_by(Test.id.desc()).limit(2)):
                print i.id, i.data

        print ""
        print ""

        yield run_transaction()

    ioloop.run_sync(run)

License
=======

torpeewee uses the MIT license, see LICENSE file for the details.

.. |Build Status| image:: https://travis-ci.org/snower/torpeewee.svg?branch=master
   :target: https://travis-ci.org/snower/torpeewee