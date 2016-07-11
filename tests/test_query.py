# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado.testing import gen_test
from . import BaseTestCase, Test

class TestQuery(BaseTestCase):
    @gen_test
    def test(self):
        yield Test.delete()

        yield Test.create(id=1, data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        yield Test.create(id=2, data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())

        c = yield Test.select().count()
        assert c == 2, ''

        data = [i for i in (yield Test.select())]
        assert len(data) == 2, ''

        data = [i for i in (yield Test.select().where(Test.id>0))]
        assert len(data) == 2, ''

        data = [i for i in (yield Test.select().group_by(Test.data))]
        assert len(data) == 1, ''

        data = [i for i in (yield Test.select().limit(1))]
        assert len(data) == 1, ''

        data = [i for i in (yield Test.select().order_by(Test.id.desc()))]
        assert data[0].id == 2

        t = yield Test.select().order_by(Test.id.desc()).first()
        t.data = "aaa"
        yield t.save()

        t = yield Test.select().order_by(Test.id.desc()).first()
        yield t.delete_instance()

        yield Test.update(data = '12345')
        t = yield Test.select().order_by(Test.id.desc()).first()
        assert t.data == '12345', ''