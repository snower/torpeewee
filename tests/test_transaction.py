# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado import gen
from tornado.testing import gen_test
from . import BaseTestCase
from .model import Test, db

class TestTestCaseTransaction(BaseTestCase):
    async def run_transaction(self, transaction):
        await Test.use(transaction).create(data="test_run_transaction", created_at=datetime.datetime.now(),
                                                updated_at=datetime.datetime.now())

        count = await Test.select().count()
        assert count == 2, ""
        count = await Test.use(transaction).select().count()
        assert count == 3, ""

    @gen_test
    async def test(self):
        await Test.delete()
        await Test.create(data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())

        async with await db.transaction() as transaction:
            await Test.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                                   updated_at=datetime.datetime.now())

            count = await Test.select().count()
            assert count == 1, ""
            count = await Test.use(transaction).select().count()
            assert count == 2, ""

            t = await Test.use(transaction).select().order_by(Test.id.desc()).first()
            td = t.data
            t.data = "222"
            await t.use(transaction).save()

            t = await Test.use(transaction).select().order_by(Test.id.desc()).first()
            assert t.data == '222'

            t = await Test.select().order_by(Test.id.desc()).first()
            assert t.data == td

        await db.transaction()(self.run_transaction)()

        transaction = await db.transaction()
        try:
            await self.run_transaction(transaction)
        except:
            await transaction.rollback()
        else:
            await transaction.commit()

        async with await db.transaction() as transaction:
            t = await Test.use(transaction).select().order_by(Test.id.desc()).first()
            t.data = "aaa"
            await t.use(transaction).save()

        t = await Test.select().order_by(Test.id.desc()).first()
        assert t.data == 'aaa'

        async with await db.transaction() as transaction:
            t = await Test.use(transaction).select().order_by(Test.id.desc()).first()
            await t.use(transaction).delete_instance()

        t = await Test.select().where(Test.id == t.id).first()
        assert t is None

        async with await db.transaction() as transaction:
            await Test.use(transaction).update(data='12345')

        t = await Test.select().order_by(Test.id.desc()).first()
        assert t.data == '12345', ''

        async with await db.transaction() as transaction:
            await Test.use(transaction).delete()

        c = await Test.select().count()
        assert c == 0, ''

        await Test.delete()