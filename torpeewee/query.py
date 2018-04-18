# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from tornado import gen
from peewee import database_required
from peewee import ModelSelect as BaseModelSelect, NoopModelSelect as BaseNoopModelSelect, ModelUpdate as BaseModelUpdate, \
    ModelInsert as BaseModelInsert, ModelDelete as BaseModelDelete, ModelRaw as BaseModelRaw


class QueryIsDoneError(Exception):
    pass


class ModelSelect(gen.Future, BaseModelSelect):
    def __init__(self, *args, **kwargs):
        BaseModelSelect.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    @database_required
    @gen.coroutine
    def peek(self, database, n=1):
        rows = (yield self.execute(database))[:n]
        if rows:
            raise gen.Return(rows[0] if n == 1 else rows)

    @database_required
    @gen.coroutine
    def scalar(self, database, as_tuple=False):
        row = yield self.tuples().peek(database)
        raise gen.Return(row[0] if row and not as_tuple else row)

    @gen.coroutine
    def get(self):
        clone = self.paginate(1, 1)
        clone._cursor_wrapper = None
        try:
            result = (yield clone.execute())[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist('%s instance matching query does '
                                          'not exist:\nSQL: %s\nParams: %s' %
                                          (clone.model, sql, params))
        raise gen.Return(result)

    def __iter__(self):
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))

    @gen.coroutine
    def _ensure_execution(self):
        if not self._cursor_wrapper:
            if not self._database:
                raise ValueError('Query has not been executed.')
            yield self.execute()

    @gen.coroutine
    def __getitem__(self, value):
        yield self._ensure_execution()
        if isinstance(value, slice):
            index = value.stop
        else:
            index = value
        if index is not None and index >= 0:
            index += 1
        self._cursor_wrapper.fill_cache(index)
        raise gen.Return(self._cursor_wrapper.row_cache[value])

    def __len__(self):
        raise NotImplementedError()

    @gen.coroutine
    def len(self):
        raise gen.Return(len((yield self.execute())))

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except Exception as e:
                self.set_exception(e)

        self._future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)


class NoopModelSelect(gen.Future, BaseNoopModelSelect):
    def __init__(self, *args, **kwargs):
        BaseNoopModelSelect.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    @database_required
    @gen.coroutine
    def peek(self, database, n=1):
        rows = (yield self.execute(database))[:n]
        if rows:
            raise gen.Return(rows[0] if n == 1 else rows)

    @database_required
    @gen.coroutine
    def scalar(self, database, as_tuple=False):
        row = yield self.tuples().peek(database)
        raise gen.Return(row[0] if row and not as_tuple else row)

    @gen.coroutine
    def get(self):
        clone = self.paginate(1, 1)
        clone._cursor_wrapper = None
        try:
            result = (yield clone.execute())[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist('%s instance matching query does '
                                          'not exist:\nSQL: %s\nParams: %s' %
                                          (clone.model, sql, params))
        raise gen.Return(result)

    def __iter__(self):
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))

    @gen.coroutine
    def _ensure_execution(self):
        if not self._cursor_wrapper:
            if not self._database:
                raise ValueError('Query has not been executed.')
            yield self.execute()

    @gen.coroutine
    def __getitem__(self, value):
        yield self._ensure_execution()
        if isinstance(value, slice):
            index = value.stop
        else:
            index = value
        if index is not None and index >= 0:
            index += 1
        self._cursor_wrapper.fill_cache(index)
        raise gen.Return(self._cursor_wrapper.row_cache[value])

    def __len__(self):
        raise NotImplementedError()

    @gen.coroutine
    def len(self):
        raise gen.Return(len((yield self.execute())))

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except Exception as e:
                self.set_exception(e)

        self._future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)

class ModelUpdate(gen.Future, BaseModelUpdate):
    def __init__(self, *args, **kwargs):
        BaseModelUpdate.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self, database):
        if self._returning:
            cursor = yield self.execute_returning(database)
        else:
            cursor = yield database.execute(self)
        raise gen.Return(self.handle_result(database, cursor))

    @gen.coroutine
    def execute_returning(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    def __iter__(self):
        if not self.model_class._meta.database.returning_clause:
            raise ValueError('UPDATE queries cannot be iterated over unless '
                             'they specify a RETURNING clause, which is not '
                             'supported by your database.')
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except Exception as e:
                self.set_exception(e)

        self._future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)


class ModelInsert(gen.Future, BaseModelInsert):
    def __init__(self, *args, **kwargs):
        BaseModelInsert.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self, database):
        if self._returning:
            cursor = yield self.execute_returning(database)
        else:
            cursor = yield database.execute(self)
        raise gen.Return(self.handle_result(database, cursor))

    @gen.coroutine
    def execute_returning(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except Exception as e:
                self.set_exception(e)

        self._future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)


class ModelDelete(gen.Future, BaseModelDelete):
    def __init__(self, *args, **kwargs):
        BaseModelDelete.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self, database):
        if self._returning:
            cursor = yield self.execute_returning(database)
        else:
            cursor = yield database.execute(self)
        raise gen.Return(self.handle_result(database, cursor))

    @gen.coroutine
    def execute_returning(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except Exception as e:
                self.set_exception(e)

        self._future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)


class ModelRaw(gen.Future, BaseModelRaw):
    def __init__(self, *args, **kwargs):
        BaseModelRaw.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except Exception as e:
                self.set_exception(e)

        self._future.add_done_callback(on_done)
        gen.Future.add_done_callback(self, fn)

    def __iter__(self):
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))