# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from tornado import gen
from peewee import MySQLDatabase as BaseMySQLDatabase, IndexMetadata, ColumnMetadata, ForeignKeyMetadata, sort_models, SENTINEL
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

    def batch_commit(self, it, n):
        raise NotImplementedError

    def in_transaction(self):
        raise NotImplementedError

    def push_transaction(self, transaction):
        raise NotImplementedError

    def pop_transaction(self):
        raise NotImplementedError

    def transaction_depth(self):
        return 0

    def top_transaction(self):
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
    def table_exists(self, table, schema=None):
        raise gen.Return(table.__name__ in (yield self.get_tables(schema=schema)))

    @gen.coroutine
    def get_tables(self, schema=None):
        raise gen.Return([row for row, in (yield self.execute_sql('SHOW TABLES'))])

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
        gen.Return([IndexMetadata(name, None, indexes[name], name in unique, table)
                for name in indexes])

    @gen.coroutine
    def get_columns(self, table, schema=None):
        sql = """
            SELECT column_name, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = DATABASE()"""
        cursor = yield self.execute_sql(sql, (table,))
        pks = set(self.get_primary_keys(table))
        gen.Return([ColumnMetadata(name, dt, null == 'YES', name in pks, table, df)
                for name, null, dt, df in cursor.fetchall()])

    @gen.coroutine
    def get_primary_keys(self, table, schema=None):
        cursor = yield self.execute_sql('SHOW INDEX FROM `%s`' % table)
        gen.Return([row[4] for row in
                filter(lambda row: row[2] == 'PRIMARY', cursor.fetchall())])

    @gen.coroutine
    def get_foreign_keys(self, table, schema=None):
        query = """
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s
                AND table_schema = DATABASE()
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL"""
        cursor = self.execute_sql(query, (table,))
        gen.Return([
            ForeignKeyMetadata(column, dest_table, dest_column, table)
            for column, dest_table, dest_column in cursor.fetchall()])

    @gen.coroutine
    def sequence_exists(self, seq):
        raise NotImplementedError

    @gen.coroutine
    def create_tables(self, models, **options):
        for model in sort_models(models):
            yield model.create_table(**options)

    @gen.coroutine
    def drop_tables(self, models, **kwargs):
        for model in reversed(sort_models(models)):
            yield model.drop_table(**kwargs)


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
    commit_select = True

    def __init__(self, *args, **kwargs):
        autocommit = kwargs.pop("autocommit") if "autocommit" in kwargs else None
        kwargs["thread_safe"] = False

        super(MySQLDatabase, self).__init__(*args, **kwargs)

        self._closed = True
        self._conn_pool = None

        self.autocommit = autocommit
        if self.autocommit:
            self.connect_params["autocommit"] = autocommit

    def _connect(self):
        conn_kwargs = {
            'charset': 'utf8',
            'use_unicode': True,
        }
        conn_kwargs.update(self.connect_params)
        if 'password' in conn_kwargs:
            conn_kwargs['passwd'] = conn_kwargs.pop('password')
        return tormysql.ConnectionPool(db=self.database, **conn_kwargs)

    def close(self):
        with self._lock:
            if self.deferred:
                raise Exception('Error, database must be initialized before '
                                'opening a connection.')

            if not self._closed and self._conn_pool:
                self._conn_pool.close()
                self._closed = True
                return True
            return False

    @gen.coroutine
    def connection(self):
        if self.is_closed():
            self.connect()
        conn = yield self._conn_pool.Connection()
        raise gen.Return(conn)

    def connect(self, reuse_if_open=False):
        with self._lock:
            if self.deferred:
                raise Exception('Error, database must be initialized before '
                                'opening a connection.')

            self._conn_pool = self._connect()
            self._initialize_connection(self._conn_pool)
        return True

    @gen.coroutine
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        if commit is SENTINEL:
            if self.commit_select:
                commit = True
            else:
                commit = not sql[:6].lower().startswith('select')

        conn = yield self.connection()
        try:
            cursor = conn.cursor()
            yield cursor.execute(sql, params or ())
            yield cursor.close()
        except Exception:
            if self.autorollback and not conn._connection.autocommit_mode:
                yield conn.rollback()
            raise
        else:
            if commit and not conn._connection.autocommit_mode:
                yield conn.commit()
        finally:
            self._close(conn)
        raise gen.Return(cursor)

    @gen.coroutine
    def cursor(self, commit=None):
        conn = yield self.connection()
        raise gen.Return(conn.cursor())

    def transaction(self, args_name = "transaction"):
        return TransactionFuture(self, args_name)

    def commit_on_success(self, func):
        return self.transaction()(func)