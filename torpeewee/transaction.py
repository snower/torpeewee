# -*- coding: utf-8 -*-
# 16/8/30
# create by: snower

import sys
from functools import wraps
from tornado.util import raise_exc_info
from tornado import gen
from peewee import SENTINEL

class Transaction(object):
    def __init__(self):
        self.database = None

    def _connect(self, database, **kwargs):
        raise NotImplementedError

    @gen.coroutine
    def connection(self):
        if self.connection is None:
            yield self.begin()

        raise gen.Return(self.connection)

    @gen.coroutine
    def get_cursor(self):
        if self.connection is None:
            yield self.begin()

        cursor = yield self.connection.cursor()
        raise gen.Return(cursor)

    @gen.coroutine
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        if self.connection is None:
            yield self.begin()

        cursor = self.connection.cursor()
        yield cursor.execute(sql, params or ())
        yield cursor.close()
        raise gen.Return(cursor)

    @gen.coroutine
    def begin(self):
        self.connection = yield self.database.connection()
        yield self.connection.begin()

    @gen.coroutine
    def commit(self):
        if self.connection:
            yield self.connection.commit()
            self.close()

    @gen.coroutine
    def rollback(self):
        if self.connection:
            yield self.connection.rollback()
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is None:
            return

        if exc_type:
            self.rollback()
        else:
            try:
                self.commit()
            except:
                exc_info = sys.exc_info()
                self.rollback()
                raise_exc_info(exc_info)

    def close(self):
        if self.connection:
            self.database._close(self.connection)
            self.connection = None

    def __del__(self):
        self.rollback()

class TransactionFuture(gen.Future):
    def __init__(self, args_name):
        super(TransactionFuture, self).__init__()

        self.args_name = args_name
        self.transaction = None
        self._transaction_begin_future = None

    @gen.coroutine
    def connection(self):
        conn = yield self.transaction.connection()
        raise gen.Return(conn)

    @gen.coroutine
    def get_cursor(self):
        cursor = yield self.transaction.get_cursor()
        raise gen.Return(cursor)

    @gen.coroutine
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        cursor = yield self.transaction.execute_sql(sql, params, commit)
        raise gen.Return(cursor)

    @gen.coroutine
    def begin(self):
        yield self.transaction.begin()

    @gen.coroutine
    def commit(self):
        yield self.transaction.commit()

    @gen.coroutine
    def rollback(self):
        yield self.transaction.rollback()

    def close(self):
        self.transaction.close()

    def __enter__(self):
        return self.transaction.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.transaction.__exit__(exc_type, exc_val, exc_tb)

    def __call__(self, func):
        @gen.coroutine
        @wraps(func)
        def _(*args, **kwargs):
            yield self.transaction.begin()
            try:
                kwargs[self.args_name] = self.transaction
                result = yield func(*args, **kwargs)
            except:
                exc_info = sys.exc_info()
                yield self.transaction.rollback()
                raise_exc_info(exc_info)
            else:
                yield self.transaction.commit()
                raise gen.Return(result)
        return _

    def add_done_callback(self, fn):
        if self._transaction_begin_future is not None:
            return super(TransactionFuture, self).add_done_callback(fn)

        self._transaction_begin_future = self.transaction.begin()

        def on_done(future):
            try:
                future.result()
            except Exception as e:
                self.set_exception(e)
            else:
                self.set_result(self.transaction)

        self._transaction_begin_future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)