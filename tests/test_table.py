# -*- coding: utf-8 -*-
# 16/7/11
# create by: snower

import datetime
from tornado.testing import gen_test
from . import BaseTestCase
from .model import TestTableModel

class TestTableTestCase(BaseTestCase):
    @gen_test
    def test(self):
        yield TestTableModel.create_table()
        yield TestTableModel.create(data = 'a', count = 1, created_at=datetime.datetime.now(), updated_at=datetime.datetime.now())
        count = yield TestTableModel.select().count()
        assert count == 1
        yield TestTableModel.drop_table()