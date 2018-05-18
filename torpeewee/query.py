# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from peewee import database_required, SQL, fn, Select as BaseSelect
from peewee import ModelSelect as BaseModelSelect, NoopModelSelect as BaseNoopModelSelect, ModelUpdate as BaseModelUpdate, \
    ModelInsert as BaseModelInsert, ModelDelete as BaseModelDelete, ModelRaw as BaseModelRaw

class FutureSelect(object):
    _cursor_wrapper_iter = None

    async def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = await database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        return self._cursor_wrapper

    @database_required
    async def peek(self, database, n=1):
        rows = (await self._execute(database))[:n]
        if rows:
            return rows[0] if n == 1 else rows

    @database_required
    def first(self, database, n=1):
        if self._limit != n:
            self._limit = n
            self._cursor_wrapper = None
        return self.peek(database, n=n)

    @database_required
    async def scalar(self, database, as_tuple=False):
        row = await self.tuples().peek(database)
        return row[0] if row and not as_tuple else row

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
    async def exists(self, database):
        clone = self.columns(SQL('1'))
        clone._limit = 1
        clone._offset = None
        return bool((await clone.scalar()))

    @database_required
    async def get(self, database):
        self._cursor_wrapper = None
        try:
            return (await self.execute(database))[0]
        except IndexError:
            pass

    async def iterator(self, database=None):
        return iter((await self.execute(database)).iterator())

    async def _ensure_execution(self):
        if not self._cursor_wrapper:
            if not self._database:
                raise ValueError('Query has not been executed.')
            await self.execute()

    def __iter__(self):
        raise NotImplementedError()

    async def __getitem__(self, value):
        await self._ensure_execution()
        if isinstance(value, slice):
            index = value.stop
        else:
            index = value
        if index is not None and index >= 0:
            index += 1
        self._cursor_wrapper.fill_cache(index)
        return self._cursor_wrapper.row_cache[value]

    def __len__(self):
        raise NotImplementedError()

    async def len(self):
        return len((await self.execute()))

    async def __aiter__(self):
        self._cursor_wrapper_iter = iter(await self.execute())
        return self

    async def __anext__(self):
        try:
            value = next(self._cursor_wrapper_iter)
        except StopIteration:
            raise StopAsyncIteration
        return value


class Select(FutureSelect, BaseSelect):
    pass


class ModelSelect(FutureSelect, BaseModelSelect):
    async def get(self):
        clone = self.paginate(1, 1)
        clone._cursor_wrapper = None
        try:
            return (await clone.execute())[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist('%s instance matching query does '
                                          'not exist:\nSQL: %s\nParams: %s' %
                                          (clone.model, sql, params))

    def __await__(self):
        coroutine = self.execute()
        return coroutine.__await__()


class NoopModelSelect(FutureSelect, BaseNoopModelSelect):
    async def get(self):
        clone = self.paginate(1, 1)
        clone._cursor_wrapper = None
        try:
            return (await clone.execute())[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist('%s instance matching query does '
                                          'not exist:\nSQL: %s\nParams: %s' %
                                          (clone.model, sql, params))

    def __await__(self):
        coroutine = self.execute()
        return coroutine.__await__()


class ModelUpdate(BaseModelUpdate):
    async def _execute(self, database):
        if self._returning:
            cursor = await self.execute_returning(database)
        else:
            cursor = await database.execute(self)
        return self.handle_result(database, cursor)

    async def execute_returning(self, database):
        if self._cursor_wrapper is None:
            cursor = await database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        return self._cursor_wrapper

    def __iter__(self):
        raise NotImplementedError()

    def __await__(self):
        coroutine = self.execute()
        return coroutine.__await__()

    async def iterator(self, database=None):
        return iter((await self.execute(database)).iterator())


class ModelInsert(BaseModelInsert):
    async def _execute(self, database):
        if self._returning:
            cursor = await self.execute_returning(database)
        else:
            cursor = await database.execute(self)
        return self.handle_result(database, cursor)

    async def execute_returning(self, database):
        if self._cursor_wrapper is None:
            cursor = await database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        return self._cursor_wrapper

    async def iterator(self, database=None):
        return iter((await self.execute(database)).iterator())


    def __await__(self):
        coroutine = self.execute()
        return coroutine.__await__()


class ModelDelete(BaseModelDelete):
    async def _execute(self, database):
        if self._returning:
            cursor = await self.execute_returning(database)
        else:
            cursor = await database.execute(self)
        return self.handle_result(database, cursor)

    async def execute_returning(self, database):
        if self._cursor_wrapper is None:
            cursor = await database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        return self._cursor_wrapper

    def __await__(self):
        coroutine = self.execute()
        return coroutine.__await__()


class ModelRaw(BaseModelRaw):
    async def _execute(self, database):
        if self._cursor_wrapper is None:
            cursor = await database.execute(self)
            self._cursor_wrapper = self._get_cursor_wrapper(cursor)
        return self._cursor_wrapper

    def __iter__(self):
        raise NotImplementedError()

    async def iterator(self, database=None):
        return iter((await self.execute(database)).iterator())

    def __await__(self):
        coroutine = self.execute()
        return coroutine.__await__()