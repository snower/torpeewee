# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from tornado import gen
from peewee import SQL, operator, RESULTS_TUPLES, RESULTS_DICTS, RESULTS_NAIVE
from peewee import SelectQuery as BaseSelectQuery, UpdateQuery as BaseUpdateQuery, InsertQuery as BaseInsertQuery, DeleteQuery as BaseDeleteQuery, RawQuery as BaseRawQuery


class QueryIsDoneError(Exception):
    pass


class SelectQuery(gen.Future, BaseSelectQuery):
    def __init__(self, *args, **kwargs):
        BaseSelectQuery.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self):
        sql, params = self.sql()
        result = yield self.database.execute_sql(sql, params, self.require_commit)
        raise gen.Return(result)

    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = yield self.tuples().first()
        else:
            row = (yield self._execute()).fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            raise gen.Return(row)

    @gen.coroutine
    def execute(self):
        if self._dirty or self._qr is None:
            model_class = self.model_class
            query_meta = self.get_query_meta()
            ResultWrapper = self._get_result_wrapper()
            self._qr = ResultWrapper(model_class, (yield self._execute()), query_meta)
            self._dirty = False
            raise gen.Return(self._qr)
        else:
            raise gen.Return(self._qr)

    @gen.coroutine
    def get(self):
        clone = self.paginate(1, 1)
        try:
            result = next((yield clone.execute()))
        except StopIteration:
            raise self.model_class.DoesNotExist(
                'Instance matching query does not exist:\nSQL: %s\nPARAMS: %s'
                % self.sql())
        raise gen.Return(result)

    @gen.coroutine
    def first(self):
        res = yield self.execute()
        res.fill_cache(1)
        try:
            result = res._result_cache[0]
        except IndexError:
            result = None
        raise gen.Return(result)

    @gen.coroutine
    def exists(self):
        clone = self.paginate(1, 1)
        clone._select = [SQL('1')]
        raise gen.Return(bool((yield clone.scalar())))

    def __iter__(self):
        raise NotImplementedError()

    @gen.coroutine
    def iterator(self):
        raise gen.Return(iter((yield self.execute()).iterator()))

    @gen.coroutine
    def __getitem__(self, value):
        res = yield self.execute()
        if isinstance(value, slice):
            index = value.stop
        else:
            index = value
        if index is not None and index >= 0:
            index += 1
        res.fill_cache(index)
        raise gen.Return(res._result_cache[value])

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
            if future._exc_info is not None:
                self.set_exc_info(future.exc_info())
            else:
                self.set_result(future.result())

        self._future.add_done_callback(on_done)
        super(SelectQuery, self).add_done_callback(fn)


class UpdateQuery(gen.Future, BaseUpdateQuery):
    def __init__(self, *args, **kwargs):
        BaseUpdateQuery.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self):
        sql, params = self.sql()
        result = yield self.database.execute_sql(sql, params, self.require_commit)
        raise gen.Return(result)

    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = yield self.tuples().first()
        else:
            row = (yield self._execute()).fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            raise gen.Return(row)

    @gen.coroutine
    def _execute_with_result_wrapper(self):
        ResultWrapper = self.get_result_wrapper()
        meta = (self._returning, {self.model_class: []})
        self._qr = ResultWrapper(self.model_class, (yield self._execute()), meta)
        raise gen.Return(self._qr)

    @gen.coroutine
    def execute(self):
        if self._returning is not None and self._qr is None:
            result = yield self._execute_with_result_wrapper()
        elif self._qr is not None:
            result = self._qr
        else:
            result = self.database.rows_affected((yield self._execute()))
        raise gen.Return(result)

    def __iter__(self):
        if not self.model_class._meta.database.returning_clause:
            raise ValueError('UPDATE queries cannot be iterated over unless '
                             'they specify a RETURNING clause, which is not '
                             'supported by your database.')
        raise NotImplementedError()

    def iterator(self):
        raise gen.Return(iter((yield self.execute()).iterator()))

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            if future._exc_info is not None:
                self.set_exc_info(future.exc_info())
            else:
                self.set_result(future.result())

        self._future.add_done_callback(on_done)
        super(UpdateQuery, self).add_done_callback(fn)


class InsertQuery(gen.Future, BaseInsertQuery):
    def __init__(self, *args, **kwargs):
        BaseInsertQuery.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self):
        sql, params = self.sql()
        result = yield self.database.execute_sql(sql, params, self.require_commit)
        raise gen.Return(result)

    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = yield self.tuples().first()
        else:
            row = (yield self._execute()).fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            raise gen.Return(row)

    @gen.coroutine
    def _execute_with_result_wrapper(self):
        ResultWrapper = self.get_result_wrapper()
        meta = (self._returning, {self.model_class: []})
        self._qr = ResultWrapper(self.model_class, (yield self._execute()), meta)
        raise gen.Return(self._qr)

    @gen.coroutine
    def _insert_with_loop(self):
        id_list = []
        last_id = None
        return_id_list = self._return_id_list
        for row in self._rows:
            last_id = (yield InsertQuery(self.model_class, row)
                       .upsert(self._upsert)
                       .execute())
            if return_id_list:
                id_list.append(last_id)

        if return_id_list:
            raise gen.Return(id_list)
        else:
            raise gen.Return(last_id)

    @gen.coroutine
    def execute(self):
        insert_with_loop = (
            self._is_multi_row_insert and
            self._query is None and
            self._returning is None and
            not self.database.insert_many)
        if insert_with_loop:
            result = yield self._insert_with_loop()
            raise gen.Return(result)

        if self._returning is not None and self._qr is None:
            result =  yield self._execute_with_result_wrapper()
            raise gen.Return(result)
        elif self._qr is not None:
            raise gen.Return(self._qr)
        else:
            cursor = yield self._execute()
            if not self._is_multi_row_insert:
                if self.database.insert_returning:
                    pk_row = cursor.fetchone()
                    meta = self.model_class._meta
                    clean_data = [
                        field.python_value(column)
                        for field, column
                        in zip(meta.get_primary_key_fields(), pk_row)]
                    if self.model_class._meta.composite_key:
                        raise gen.Return(clean_data)
                    raise gen.Return(clean_data[0])
                raise gen.Return(self.database.last_insert_id(cursor, self.model_class))
            elif self._return_id_list:
                raise gen.Return(map(operator.itemgetter(0), cursor.fetchall()))
            else:
                raise gen.Return(True)

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            if future._exc_info is not None:
                self.set_exc_info(future.exc_info())
            else:
                self.set_result(future.result())

        self._future.add_done_callback(on_done)
        super(InsertQuery, self).add_done_callback(fn)


class DeleteQuery(gen.Future, BaseDeleteQuery):
    def __init__(self, *args, **kwargs):
        BaseDeleteQuery.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self):
        sql, params = self.sql()
        result = yield self.database.execute_sql(sql, params, self.require_commit)
        raise gen.Return(result)

    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = yield self.tuples().first()
        else:
            row = (yield self._execute()).fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            raise gen.Return(row)

    @gen.coroutine
    def _execute_with_result_wrapper(self):
        ResultWrapper = self.get_result_wrapper()
        meta = (self._returning, {self.model_class: []})
        self._qr = ResultWrapper(self.model_class, (yield self._execute()), meta)
        raise gen.Return(self._qr)

    @gen.coroutine
    def execute(self):
        if self._returning is not None and self._qr is None:
            result = yield self._execute_with_result_wrapper()
        elif self._qr is not None:
            result = self._qr
        else:
            result = self.database.rows_affected((yield self._execute()))
        raise gen.Return(result)

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            if future._exc_info is not None:
                self.set_exc_info(future.exc_info())
            else:
                self.set_result(future.result())

        self._future.add_done_callback(on_done)
        super(DeleteQuery, self).add_done_callback(fn)


class RawQuery(gen.Future, BaseRawQuery):
    def __init__(self, *args, **kwargs):
        BaseRawQuery.__init__(self, *args, **kwargs)
        gen.Future.__init__(self)

        self._future = None

    @gen.coroutine
    def _execute(self):
        sql, params = self.sql()
        result = yield self.database.execute_sql(sql, params, self.require_commit)
        raise gen.Return(result)

    @gen.coroutine
    def scalar(self, as_tuple=False, convert=False):
        if convert:
            row = yield self.tuples().first()
        else:
            row = (yield self._execute()).fetchone()
        if row and not as_tuple:
            raise gen.Return(row[0])
        else:
            raise gen.Return(row)

    def execute(self):
        if self._qr is None:
            if self._tuples:
                QRW = self.database.get_result_wrapper(RESULTS_TUPLES)
            elif self._dicts:
                QRW = self.database.get_result_wrapper(RESULTS_DICTS)
            else:
                QRW = self.database.get_result_wrapper(RESULTS_NAIVE)
            self._qr = QRW(self.model_class, (yield self._execute()), None)
        raise gen.Return(self._qr)

    def add_done_callback(self, fn):
        if self._future is not None:
            raise QueryIsDoneError()

        self._future = self.execute()

        def on_done(future):
            if future._exc_info is not None:
                self.set_exc_info(future.exc_info())
            else:
                self.set_result(future.result())

        self._future.add_done_callback(on_done)
        super(RawQuery, self).add_done_callback(fn)

    def __iter__(self):
        raise NotImplementedError()

    def iterator(self):
        raise gen.Return(iter((yield self.execute())))