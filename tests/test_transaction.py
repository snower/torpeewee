# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado import gen
from tornado.testing import gen_test
from . import BaseTestCase
from .model import Test, db

class TestTestCaseTransaction(BaseTestCase):
    @gen.coroutine
    def run_transaction(self, transaction):
        yield Test.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                                updated_at=datetime.datetime.now())

        count = yield Test.select().count()
        assert count == 2, ""
        count = yield Test.use(transaction).select().count()
        assert count == 3, ""

    @gen_test
    def test(self):
        yield Test.delete()
        yield Test.create(data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())

        with (yield db.transaction()) as transaction:
            yield Test.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                                   updated_at=datetime.datetime.now())

            count = yield Test.select().count()
            assert count == 1, ""
            count = yield Test.use(transaction).select().count()
            assert count == 2, ""

            t = yield Test.use(transaction).select().order_by(Test.id.desc()).first()
            td = t.data
            t.data = "222"
            yield t.use(transaction).save()

            t = yield Test.use(transaction).select().order_by(Test.id.desc()).first()
            assert t.data == '222'

            t = yield Test.select().order_by(Test.id.desc()).first()
            assert t.data == td

        yield db.transaction()(self.run_transaction)()

        transaction = yield db.transaction()
        try:
            yield self.run_transaction(transaction)
        except:
            yield transaction.rollback()
        else:
            yield transaction.commit()

        with (yield db.transaction()) as transaction:
            t = yield Test.use(transaction).select().order_by(Test.id.desc()).first()
            t.data = "aaa"
            yield t.use(transaction).save()

        t = yield Test.select().order_by(Test.id.desc()).first()
        assert t.data == 'aaa'

        with (yield db.transaction()) as transaction:
            t = yield Test.use(transaction).select().order_by(Test.id.desc()).first()
            yield t.use(transaction).delete_instance()

        t = yield Test.select().where(Test.id == t.id).first()
        assert t is None

        with (yield db.transaction()) as transaction:
            yield Test.use(transaction).update(data='12345')

        t = yield Test.select().order_by(Test.id.desc()).first()
        assert t.data == '12345', ''

        with (yield db.transaction()) as transaction:
            yield Test.use(transaction).delete()

        c = yield Test.select().count()
        assert c == 0, ''

        yield Test.delete()