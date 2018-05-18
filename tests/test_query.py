# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado.testing import gen_test
from . import BaseTestCase
from .model import Test

class TestQueryTestCase(BaseTestCase):
    @gen_test
    async def test(self):
        await Test.delete()

        await Test.create(id=1, data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        await Test.create(id=2, data="test", created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())

        c = await Test.select().count()
        assert c == 2, ''

        data = [i for i in (await Test.select())]
        assert len(data) == 2, ''

        data = []
        async for i in Test.select():
            data.append(i)
        assert len(data) == 2, ''

        data = [i for i in (await Test.select().where(Test.id>0))]
        assert len(data) == 2, ''

        data = [i for i in (await Test.select(Test.data).group_by(Test.data))]
        assert len(data) == 1, ''

        data = [i for i in (await Test.select().limit(1))]
        assert len(data) == 1, ''

        data = [i for i in (await Test.select().order_by(Test.id.desc()))]
        assert data[0].id == 2

        t = await Test.select().order_by(Test.id.desc()).first()
        t.data = "aaa"
        await t.save()
        t = await Test.select().order_by(Test.id.desc()).first()
        assert  t.data == 'aaa'

        t = await Test.select().order_by(Test.id.desc()).first()
        await t.delete_instance()
        t = await Test.select().where(Test.id == t.id).first()
        assert t is None

        await Test.update(data = '12345')
        t = await Test.select().order_by(Test.id.desc()).first()
        assert t.data == '12345', ''

        await Test.delete()
        c = await Test.select().count()
        assert c == 0, ''