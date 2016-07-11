# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado import gen
from tornado.testing import gen_test
from . import BaseTestCase, Test

class TestTransaction(BaseTestCase):
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
        yield Test.create(data="test", created_at=datetime.datetime.now(),
                                                updated_at=datetime.datetime.now())

        with (yield self.db.transaction()) as transaction:
            yield Test.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                                   updated_at=datetime.datetime.now())

            count = yield Test.select().count()
            assert count == 1, ""
            count = yield Test.use(transaction).select().count()
            assert count == 2, ""

        yield self.db.transaction()(self.run_transaction)()