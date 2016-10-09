# -*- coding: utf-8 -*-
# 16/8/30
# create by: snower

import sys
from functools import wraps
from tornado.util import raise_exc_info
from tornado import gen

class Transaction(object):
    def _connect(self, database, **kwargs):
        raise NotImplementedError

    @gen.coroutine
    def get_conn(self):
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
    def execute_sql(self, sql, params=None, require_commit=True):
        if self.connection is None:
            yield self.begin()

        with self.database.exception_wrapper():
            cursor = self.connection.cursor()
            yield cursor.execute(sql, params or ())
            yield cursor.close()
        raise gen.Return(cursor)

    @gen.coroutine
    def begin(self):
        self.connection = yield self.database.get_conn()
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


class TransactionFuture(gen.Future):
    def __init__(self, args_name):
        super(TransactionFuture, self).__init__()

        self.args_name = args_name
        self.transaction = None

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
        if self._future is not None:
            return super(TransactionFuture, self).add_done_callback(fn)

        self._future = self.transaction.begin()

        def on_done(future):
            if future._exc_info is not None:
                self.set_exc_info(future.exc_info())
            else:
                self.set_result(self)

        self._future.add_done_callback(on_done)
        super(TransactionFuture, self).add_done_callback(fn)