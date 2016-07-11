# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado.testing import gen_test
from . import BaseTestCase, TestModel

class TestQuery(BaseTestCase):
    @gen_test
    def test_create(self):
        yield TestModel.create_table()

    @gen_test
    def test_create_data(self):
        yield TestModel.create(id =1, data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        yield TestModel.create(id = 1, data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())

    @gen_test
    def test_count(self):
        c = yield TestModel.select().count()
        assert c == 2, ''

    @gen_test
    def test_select(self):
        data = [i for i in (yield TestModel.select())]
        assert len(data) == 2, ''

    @gen_test
    def test_where(self):
        data = [i for i in (yield TestModel.select().where(TestModel.id>0))]
        assert len(data) == 2, ''

    @gen_test
    def test_group_by(self):
        data = [i for i in (yield TestModel.select().group_by(TestModel.data))]
        assert len(data) == 1, ''

    @gen_test
    def test_limit(self):
        data = [i for i in (yield TestModel.select().limit(1))]
        assert len(data) == 1, ''

    @gen_test
    def test_order_by(self):
        data = [i for i in (yield TestModel.select().order_by(TestModel.id.desc()))]
        assert data[0].id == 2

    @gen_test
    def test_save(self):
        t = yield TestModel.select().order_by(TestModel.id.desc()).first()
        t.data = "aaa"
        yield t.save()

    @gen_test
    def test_delete(self):
        t = yield TestModel.select().order_by(TestModel.id.desc()).first()
        yield t.delete_instance()

    @gen_test
    def test_update(self):
        yield TestModel.update(data = '12345')
        t = yield TestModel.select().order_by(TestModel.id.desc()).first()
        assert t.data == '12345', ''

    @gen_test
    def test_drop(self):
        yield TestModel.drop_table()