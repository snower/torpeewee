# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado import gen
from tornado.testing import gen_test
from . import BaseTestCase, TestModel, db

class TestCreate(BaseTestCase):
    @gen_test
    def test_create(self):
        yield TestModel.create_table()

    @gen_test
    def test_create_data(self):
        yield TestModel.create(id=1, data="test", created_at=datetime.datetime.now(),
                               updated_at=datetime.datetime.now())
        yield TestModel.create(id=1, data="test", created_at=datetime.datetime.now(),
                               updated_at=datetime.datetime.now())

    @db.transaction()
    @gen.coroutine
    def run_transaction(self, transaction):
        yield TestModel.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                                updated_at=datetime.datetime.now())

        count = yield TestModel.select().count()
        assert count == 2, ""
        count = yield TestModel.use(transaction).select().count()
        assert count == 3, ""

    @gen_test
    def test_transaction(self):
        with (yield db.transaction()) as transaction:
            yield TestModel.use(transaction).create(data="test", created_at=datetime.datetime.now(),
                                                   updated_at=datetime.datetime.now())

            count = yield TestModel.select().count()
            assert count == 2, ""
            count = yield TestModel.use(transaction).select().count()
            assert count == 3, ""

    @gen_test
    def test_drop(self):
        yield TestModel.drop_table()