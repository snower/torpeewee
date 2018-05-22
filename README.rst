torpeewee
=========

|Build Status|

Tornado and asyncio asynchronous ORM by peewee

About
=====

torpeewee - Tornado and asyncio asynchronous ORM by peewee.

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
    import asyncio
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

    @db.transaction()
    async def run_transaction(transaction):
        async for i in Test.use(transaction).select().order_by(Test.id.desc()).limit(2):
            print(i.id, i.data)

        print("")
        t = await Test.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                               updated_at=datetime.datetime.now())
        print(t.id, t.data)

        async for i in Test.select().order_by(Test.id.desc()).limit(2):
            print(i.id, i.data)

        print("")
        for i in (await Test.use(transaction).select().order_by(Test.id.desc()).limit(2)):
            print(i.id, i.data)

    async def run():
        t = await Test.select().where(Test.id == 5).first()
        print(t)

        c = await Test.select().where(Test.id > 5).count()
        print(c)

        c = await Test.select().where(Test.id > 5).group_by(Test.data).count()
        print(c)

        for i in (await Test.select().where(Test.id > 5).where(Test.id<=10)):
            print(i.id, i.data)

        async for i in Test.select().order_by(Test.id.desc()).limit(2):
            print(i.id, i.data)
        t = await Test.create(data = "test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        print(t.id, t.data)
        async for i in Test.select().order_by(Test.id.desc()).limit(2):
            print(i.id, i.data)

        print("")
        print("")

        t = await Test.select().order_by(Test.id.desc()).limit(1)[0]
        print(t.id, t.data, t.count)
        t.count += 1
        await t.save()
        t = await Test.select().order_by(Test.id.desc()).limit(1)[0]
        print(t.id, t.data, t.count)

        print("")
        print("")

        async with await db.transaction() as transaction:
            t = await Test.use(transaction).select().order_by(Test.id.desc()).limit(1)[0]
            print(t.id, t.data, t.count)
            t.count += 1
            await t.use(transaction).save()

            async for i in Test.use(transaction).select().order_by(Test.id.desc()).limit(2):
                print(i.id, i.data)

            print("")
            t = await Test.use(transaction).create(data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
            print(t.id, t.data)

            async for i in Test.select().order_by(Test.id.desc()).limit(2):
                print(i.id, i.data)

            print("")
            for i in (await Test.use(transaction).select().order_by(Test.id.desc()).limit(2)):
                print(i.id, i.data)

        print("")
        print("")

        await run_transaction()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

License
=======

torpeewee uses the MIT license, see LICENSE file for the details.

.. |Build Status| image:: https://travis-ci.org/snower/torpeewee.svg?branch=master
   :target: https://travis-ci.org/snower/torpeewee