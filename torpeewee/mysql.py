# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from tornado import gen
from peewee import MySQLDatabase as BaseMySQLDatabase, IndexMetadata, ColumnMetadata, ForeignKeyMetadata, sort_models_topologically
from .transaction import Transaction as BaseTransaction, TransactionFuture as BaseTransactionFuture

try:
    import tormysql
except ImportError:
    tormysql = None

class AsyncMySQLDatabase(BaseMySQLDatabase):
    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def push_execution_context(self, transaction):
        raise NotImplementedError

    def pop_execution_context(self):
        raise NotImplementedError

    def execution_context_depth(self):
        raise NotImplementedError

    def execution_context(self, with_transaction=True, transaction_type=None):
        raise NotImplementedError

    def push_transaction(self, transaction):
        raise NotImplementedError

    def pop_transaction(self):
        raise NotImplementedError

    def transaction_depth(self):
        raise NotImplementedError

    @gen.coroutine
    def transaction(self, transaction_type=None):
        raise NotImplementedError

    def commit_on_success(self, func):
        raise NotImplementedError

    def savepoint(self, sid=None):
        raise NotImplementedError

    def atomic(self, transaction_type=None):
        raise NotImplementedError

    @gen.coroutine
    def get_tables(self, schema=None):
        rows = yield self.execute_sql('SHOW TABLES')
        raise gen.Return([row for row, in rows])

    @gen.coroutine
    def get_indexes(self, table, schema=None):
        cursor = yield self.execute_sql('SHOW INDEX FROM `%s`' % table)
        unique = set()
        indexes = {}
        for row in cursor.fetchall():
            if not row[1]:
                unique.add(row[2])
            indexes.setdefault(row[2], [])
            indexes[row[2]].append(row[4])
        raise gen.Return([IndexMetadata(name, None, indexes[name], name in unique, table)
                          for name in indexes])

    @gen.coroutine
    def get_columns(self, table, schema=None):
        sql = """
                SELECT column_name, is_nullable, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = DATABASE()"""
        cursor = yield self.execute_sql(sql, (table,))
        pks = set(self.get_primary_keys(table))
        raise gen.Return([ColumnMetadata(name, dt, null == 'YES', name in pks, table)
                          for name, null, dt in cursor.fetchall()])

    @gen.coroutine
    def get_primary_keys(self, table, schema=None):
        cursor = yield self.execute_sql('SHOW INDEX FROM `%s`' % table)
        raise gen.Return([row[4] for row in cursor.fetchall() if row[2] == 'PRIMARY'])

    @gen.coroutine
    def get_foreign_keys(self, table, schema=None):
        query = """
                SELECT column_name, referenced_table_name, referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_name = %s
                    AND table_schema = DATABASE()
                    AND referenced_table_name IS NOT NULL
                    AND referenced_column_name IS NOT NULL"""
        cursor = yield self.execute_sql(query, (table,))
        raise gen.Return([ForeignKeyMetadata(column, dest_table, dest_column, table)
                             for column, dest_table, dest_column in cursor.fetchall()])

    @gen.coroutine
    def create_table(self, model_class, safe=False):
        qc = self.compiler()
        cursor = yield self.execute_sql(*qc.create_table(model_class, safe))
        raise gen.Return(cursor)

    @gen.coroutine
    def create_tables(self, models, safe=False):
        for m in sort_models_topologically(models):
            yield m.create_table(fail_silently=safe)

    @gen.coroutine
    def create_index(self, model_class, fields, unique=False):
        qc = self.compiler()
        if not isinstance(fields, (list, tuple)):
            raise ValueError('Fields passed to "create_index" must be a list '
                             'or tuple: "%s"' % fields)
        fobjs = [
            model_class._meta.fields[f] if isinstance(f, basestring) else f
            for f in fields]
        cursor = yield self.execute_sql(*qc.create_index(model_class, fobjs, unique))
        raise gen.Return(cursor)

    @gen.coroutine
    def create_foreign_key(self, model_class, field, constraint=None):
        qc = self.compiler()
        cursor = yield self.execute_sql(*qc.create_foreign_key(
            model_class, field, constraint))
        raise gen.Return(cursor)

    @gen.coroutine
    def create_sequence(self, seq):
        if self.sequences:
            qc = self.compiler()
            cursor = yield self.execute_sql(*qc.create_sequence(seq))
            raise gen.Return(cursor)

    @gen.coroutine
    def drop_table(self, model_class, fail_silently=False, cascade=False):
        qc = self.compiler()
        cursor = yield self.execute_sql(*qc.drop_table(
            model_class, fail_silently, cascade))
        raise gen.Return(cursor)

    @gen.coroutine
    def drop_tables(self, models, safe=False, cascade=False):
        for m in reversed(sort_models_topologically(models)):
            yield m.drop_table(fail_silently=safe, cascade=cascade)

    @gen.coroutine
    def truncate_table(self, model_class, restart_identity=False,
                       cascade=False):
        qc = self.compiler()
        cursor = yield self.execute_sql(*qc.truncate_table(
            model_class, restart_identity, cascade))
        raise gen.Return(cursor)

    @gen.coroutine
    def truncate_tables(self, models, restart_identity=False, cascade=False):
        for model in reversed(sort_models_topologically(models)):
            yield model.truncate_table(restart_identity, cascade)

    @gen.coroutine
    def drop_sequence(self, seq):
        if self.sequences:
            qc = self.compiler()
            cursor = yield self.execute_sql(*qc.drop_sequence(seq))
            raise gen.Return(cursor)


class Transaction(BaseTransaction, AsyncMySQLDatabase):
    def __init__(self, database):
        AsyncMySQLDatabase.__init__(self, database.database)

        self.database = database
        self.connection = None

class TransactionFuture(BaseTransactionFuture):
    def __init__(self, database, args_name):
        super(TransactionFuture, self).__init__(args_name)

        self.transaction = Transaction(database)
        self._future = None


class MySQLDatabase(AsyncMySQLDatabase):
    def __init__(self, *args, **kwargs):
        kwargs["threadlocals"] = False

        super(MySQLDatabase, self).__init__(*args, **kwargs)

        self._closed = True
        self._conn_pool = None

    def _connect(self, database, **kwargs):
        conn_kwargs = {
            'charset': 'utf8',
            'use_unicode': True,
        }
        conn_kwargs.update(kwargs)
        if 'password' in conn_kwargs:
            conn_kwargs['passwd'] = conn_kwargs.pop('password')
        return tormysql.ConnectionPool(db=database, **conn_kwargs)

    def close(self):
        with self._conn_lock:
            if self.deferred:
                raise Exception('Error, database not properly initialized '
                                'before closing connection')
            with self.exception_wrapper:
                if not self._closed and self._conn_pool:
                    self._conn_pool.close()
                    self._closed = True

    @gen.coroutine
    def get_conn(self):
        if self._closed:
            with self.exception_wrapper:
                self._conn_pool = self._connect(self.database, **self.connect_kwargs)
                self._closed = False
                self.initialize_connection(self._conn_pool)
        conn = yield self._conn_pool.Connection()
        raise gen.Return(conn)

    @gen.coroutine
    def execute_sql(self, sql, params=None, require_commit=True):
        with self.exception_wrapper:
            conn = yield self.get_conn()
            try:
                cursor = conn.cursor()
                yield cursor.execute(sql, params or ())
                yield cursor.close()
            except Exception:
                if self.get_autocommit() and self.autorollback:
                    yield conn.rollback()
                raise
            else:
                if require_commit and self.get_autocommit():
                    yield conn.commit()
            finally:
                self._close(conn)
        raise gen.Return(cursor)

    def transaction(self, args_name = "transaction"):
        return TransactionFuture(self, args_name)

    def commit_on_success(self, func):
        return self.transaction()(func)