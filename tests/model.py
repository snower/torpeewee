# -*- coding: utf-8 -*-
# 18/4/18
# create by: snower

import os
from torpeewee import *

if os.getenv("TEST_DRIVER", "mysql") == "mysql":
    PARAMS = dict(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        passwd=os.getenv("MYSQL_PASSWD", ""),
        charset=os.getenv("MYSQL_CHARSET", "utf8"),
        sql_mode="REAL_AS_FLOAT",
        init_command="SET max_join_size=DEFAULT"
    )

    db = MySQLDatabase(
        os.getenv("MYSQL_DB", "test"),
        max_connections=int(os.getenv("MYSQL_POOL", 20)),
        idle_seconds=7200,
        **PARAMS
    )
else:
    PARAMS = dict(
        host=os.getenv("POSTGRESQL_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRESQL_PORT", "5432")),
        user=os.getenv("POSTGRESQL_USER", "root"),
        passwd=os.getenv("POSTGRESQL_PASSWD", ""),
    )

    db = PostgresqlDatabase(
        os.getenv("POSTGRESQL_DB", "test"),
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