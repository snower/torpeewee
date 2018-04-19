# -*- coding: utf-8 -*-
# 18/4/18
# create by: snower

import os
from torpeewee import *

PARAMS = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    passwd=os.getenv("MYSQL_PASSWD", ""),
    charset=os.getenv("MYSQL_CHARSET", "utf8"),
    no_delay=True,
    sql_mode="REAL_AS_FLOAT",
    init_command="SET max_join_size=DEFAULT"
)

db = MySQLDatabase(
    os.getenv("MYSQL_DB", "test"),
    max_connections=int(os.getenv("MYSQL_POOL", 5)),
    idle_seconds=7200,
    **PARAMS
)

class Test(Model):
    id = IntegerField(primary_key=True)
    data = CharField(max_length=64, null=False)
    count = IntegerField(default=0)
    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = db

class TestTableModel(Model):
    id = IntegerField(primary_key=True)
    data = CharField(max_length=64, null=False)
    count = IntegerField(default=0)
    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = db