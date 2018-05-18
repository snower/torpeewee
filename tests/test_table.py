# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado.testing import gen_test
from . import BaseTestCase
from .model import TestTableModel

class TestTableTestCase(BaseTestCase):
    @gen_test
    async def test(self):
        await TestTableModel.create_table()
        await TestTableModel.create(id = 1, data = 'a', count = 1, created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        count = await TestTableModel.select().count()
        assert count == 1
        await TestTableModel.drop_table()