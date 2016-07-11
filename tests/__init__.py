#!/usr/bin/env python
# encoding: utf-8
import os
from torpeewee import *
from tornado.testing import AsyncTestCase

PARAMS = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    passwd=os.getenv("MYSQL_PASSWD", ""),
    db=os.getenv("MYSQL_DB", "test"),
    charset=os.getenv("MYSQL_CHARSET", "utf8"),
    no_delay=True,
    sql_mode="REAL_AS_FLOAT",
    init_command="SET max_join_size=DEFAULT"
)

db = MySQLDatabase(
    "test",
    max_connections=int(os.getenv("MYSQL_POOL", 5)),
    idle_seconds=7200,
    **PARAMS
)

class TestModel(Model):
    id = IntegerField(primary_key=True)
    data = CharField(max_length=64, null=False)
    count = IntegerField(default=0)
    created_at = DateTimeField()
    updated_at = DateTimeField()

class BaseTestCase(AsyncTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.db = db

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
        self.db.close()