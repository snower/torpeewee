# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from tornado import gen
from peewee import database_required, SQL, fn, Select as BaseSelect
from peewee import ModelSelect as BaseModelSelect, NoopModelSelect as BaseNoopModelSelect, ModelUpdate as BaseModelUpdate, \
    ModelInsert as BaseModelInsert, ModelDelete as BaseModelDelete, ModelRaw as BaseModelRaw


class QueryFuture(gen.Future):
    _query_future = None
    _query_class = None
    _query = None

    def __init__(self, query_class, *args, **kwargs):
        super(QueryFuture, self).__init__()

        self._query_future = None
        self._query_class = query_class
        self._query = kwargs.pop("__query__") if "__query__" in kwargs else query_class(*args, **kwargs)

    def __getattr__(self, item):
        attr = getattr(self._query, item)
        if callable(attr):
            def inner(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, gen.Future):
                    return result
                elif result is self._query:
                    return self
                elif isinstance(result, self._query_class):
                    return self.__class__(self._query_class, __query__ = result)
                return result
            super(QueryFuture, self).__setattr__(item, inner)
            return inner
        return attr

    def __setattr__(self, key, value):
        if self._query is not None and not hasattr(self, key):
            return setattr(self._query, key, value)
        return super(QueryFuture, self).__setattr__(key, value)

    def add_done_callback(self, fn):
        if self._query_future is not None:
            return super(QueryFuture, self).add_done_callback(fn)

        self._query_future = self._query.execute()

        def on_done(future):
            try:
                self.set_result(future.result())
            except:
                self.set_exception(future.exception())

        self._query_future.add_done_callback(on_done)
        return super(QueryFuture, self).add_done_callback(fn)


class FutureSelect(object):
    @gen.coroutine
    def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    @database_required
    @gen.coroutine
    def peek(self, database, n=1):
        rows = (yield self._execute(database))[:n]
        if rows:
            raise gen.Return(rows[0] if n == 1 else rows)

    @database_required
    def first(self, database, n=1):
        if self._limit != n:
            self._limit = n
            self._cursor_wrapper = None
        return self.peek(database, n=n)

    @database_required
    @gen.coroutine
    def scalar(self, database, as_tuple=False):
        row = yield self.tuples().peek(database)
        raise gen.Return(row[0] if row and not as_tuple else row)

    @database_required
    def count(self, database, clear_limit=False):
        clone = self.order_by().alias('_wrapped')
        if clear_limit:
            clone._limit = clone._offset = None
        try:
            if clone._having is None and clone._windows is None and \
                            clone._distinct is None and clone._simple_distinct is not True:
                clone = clone.select(SQL('1'))
        except AttributeError:
            pass
        return Select([clone], [fn.COUNT(SQL('1'))]).scalar(database)

    @database_required
    @gen.coroutine
    def exists(self, database):
        clone = self.columns(SQL('1'))
        clone._limit = 1
        clone._offset = None
        raise gen.Return(bool((yield clone.scalar())))

    @database_required
    @gen.coroutine
    def get(self, database):
        self._cursor_wrapper = None
        try:
            raise gen.Return((yield self.execute(database))[0])
        except IndexError:
            pass

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))

    @gen.coroutine
    def _ensure_execution(self):
        if not self._cursor_wrapper:
            if not self._database:
                raise ValueError('Query has not been executed.')
            yield self.execute()

    def __iter__(self):
        raise NotImplementedError()

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


class Select(FutureSelect, BaseSelect):
    pass


class ModelSelect(FutureSelect, BaseModelSelect):
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


class ModelSelectFuture(QueryFuture):
    def __init__(self, *args, **kwargs):
        super(ModelSelectFuture, self).__init__(ModelSelect, *args, **kwargs)


class NoopModelSelect(FutureSelect, BaseNoopModelSelect):
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


class NoopModelSelectFuture(QueryFuture):
    def __init__(self, *args, **kwargs):
        super(NoopModelSelectFuture, self).__init__(NoopModelSelect, *args, **kwargs)


class ModelUpdate(BaseModelUpdate):
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
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))


class ModelUpdateFuture(QueryFuture):
    def __init__(self, *args, **kwargs):
        super(ModelUpdateFuture, self).__init__(ModelUpdate, *args, **kwargs)


class ModelInsert(BaseModelInsert):
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


class ModelInsertFuture(QueryFuture):
    def __init__(self, *args, **kwargs):
        super(ModelInsertFuture, self).__init__(ModelInsert, *args, **kwargs)


class ModelDelete(BaseModelDelete):
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


class ModelDeleteFuture(QueryFuture):
    def __init__(self, *args, **kwargs):
        super(ModelDeleteFuture, self).__init__(ModelDelete, *args, **kwargs)


class ModelRaw(BaseModelRaw):
    @gen.coroutine
    def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = yield database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        raise gen.Return(self._cursor_wrapper)

    def __iter__(self):
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self, database=None):
        raise gen.Return(iter((yield self.execute(database)).iterator()))


class ModelRawFuture(QueryFuture):
    def __init__(self, *args, **kwargs):
        super(ModelRawFuture, self).__init__(ModelRaw, *args, **kwargs)