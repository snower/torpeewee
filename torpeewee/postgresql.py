# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

import re
from tornado import gen
from peewee import PostgresqlDatabase as BasePostgresqlDatabase, IndexMetadata, ColumnMetadata, ForeignKeyMetadata, sort_models, SENTINEL
from .transaction import Transaction as BaseTransaction, TransactionFuture as BaseTransactionFuture

try:
    import momoko
except ImportError:
    momoko = None


def _param_escape(s, re_escape=re.compile(r"([\\'])"), re_space=re.compile(r'\s')):
    if not s: return "''"

    s = re_escape.sub(r'\\\1', s)
    if re_space.search(s):
        s = "'" + s + "'"

    return s

class AsyncPostgresqlDatabase(BasePostgresqlDatabase):
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
        query = ('SELECT tablename FROM pg_catalog.pg_tables '
                 'WHERE schemaname = %s ORDER BY tablename')

        cursor = yield self.execute_sql(query, (schema or 'public',))
        raise gen.Return([table for table, in cursor.fetchall()])

    @gen.coroutine
    def get_indexes(self, table, schema=None):
        query = """
            SELECT
                i.relname, idxs.indexdef, idx.indisunique,
                array_to_string(array_agg(cols.attname), ',')
            FROM pg_catalog.pg_class AS t
            INNER JOIN pg_catalog.pg_index AS idx ON t.oid = idx.indrelid
            INNER JOIN pg_catalog.pg_class AS i ON idx.indexrelid = i.oid
            INNER JOIN pg_catalog.pg_indexes AS idxs ON
                (idxs.tablename = t.relname AND idxs.indexname = i.relname)
            LEFT OUTER JOIN pg_catalog.pg_attribute AS cols ON
                (cols.attrelid = t.oid AND cols.attnum = ANY(idx.indkey))
            WHERE t.relname = %s AND t.relkind = %s AND idxs.schemaname = %s
            GROUP BY i.relname, idxs.indexdef, idx.indisunique
            ORDER BY idx.indisunique DESC, i.relname;"""
        cursor = yield self.execute_sql(query, (table, 'r', schema or 'public'))
        raise gen.Return([IndexMetadata(row[0], row[1], row[3].split(','), row[2], table)
                for row in cursor.fetchall()])

    @gen.coroutine
    def get_columns(self, table, schema=None):
        query = """
            SELECT column_name, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position"""
        cursor = yield self.execute_sql(query, (table, schema or 'public'))
        pks = set(self.get_primary_keys(table, schema))
        raise gen.Return([ColumnMetadata(name, dt, null == 'YES', name in pks, table, df)
                for name, null, dt, df in cursor.fetchall()])

    @gen.coroutine
    def get_primary_keys(self, table, schema=None):
        query = """
            SELECT kc.column_name
            FROM information_schema.table_constraints AS tc
            INNER JOIN information_schema.key_column_usage AS kc ON (
                tc.table_name = kc.table_name AND
                tc.table_schema = kc.table_schema AND
                tc.constraint_name = kc.constraint_name)
            WHERE
                tc.constraint_type = %s AND
                tc.table_name = %s AND
                tc.table_schema = %s"""
        ctype = 'PRIMARY KEY'
        cursor = yield self.execute_sql(query, (ctype, table, schema or 'public'))
        raise gen.Return([pk for pk, in cursor.fetchall()])

    @gen.coroutine
    def get_foreign_keys(self, table, schema=None):
        sql = """
            SELECT
                kcu.column_name, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON (tc.constraint_name = kcu.constraint_name AND
                    tc.constraint_schema = kcu.constraint_schema)
            JOIN information_schema.constraint_column_usage AS ccu
                ON (ccu.constraint_name = tc.constraint_name AND
                    ccu.constraint_schema = tc.constraint_schema)
            WHERE
                tc.constraint_type = 'FOREIGN KEY' AND
                tc.table_name = %s AND
                tc.table_schema = %s"""
        cursor = yield self.execute_sql(sql, (table, schema or 'public'))
        raise gen.Return([ForeignKeyMetadata(row[0], row[1], row[2], table)
                for row in cursor.fetchall()])

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


class Transaction(BaseTransaction, AsyncPostgresqlDatabase):
    def __init__(self, database):
        AsyncPostgresqlDatabase.__init__(self, database.database)

        self.database = database
        self.connection = None

    @gen.coroutine
    def get_cursor(self):
        raise NotImplementedError()

    @gen.coroutine
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        if self.connection is None:
            yield self.begin()

        cursor = yield self.connection.execute(sql, params or ())
        raise gen.Return(cursor)

    @gen.coroutine
    def begin(self):
        self.connection = yield self.database.get_conn()
        yield self.connection.execute("BEGIN;")

    @gen.coroutine
    def commit(self):
        if self.connection:
            yield self.connection.execute("COMMIT;")
            self.close()

    @gen.coroutine
    def rollback(self):
        if self.connection:
            yield self.connection.execute("ROLLBACK;")
            self.close()

class TransactionFuture(BaseTransactionFuture):
    def __init__(self, database, args_name):
        super(TransactionFuture, self).__init__(args_name)

        self.transaction = Transaction(database)
        self._future = None

class PostgresqlDatabase(AsyncPostgresqlDatabase):
    def __init__(self, *args, **kwargs):
        autocommit = kwargs.pop("autocommit") if "autocommit" in kwargs else None
        kwargs["thread_safe"] = False

        super(PostgresqlDatabase, self).__init__(*args, **kwargs)

        self._closed = True
        self._conn_pool = None

        self.autocommit = autocommit
        if self.autocommit:
            self.connect_params["autocommit"] = autocommit

    def _connect(self):
        pool_kwargs = {}
        conn_kwargs = {"dbname": self.database}
        conn_kwargs.update(self.connect_params)

        for key in ['connection_factory', 'cursor_factory', 'size', 'max_size', 'ioloop', 'raise_connect_errors',
                    'reconnect_interval','setsession', 'auto_shrink', 'shrink_delay', 'shrink_period']:
            if key in conn_kwargs:
                pool_kwargs[key] = conn_kwargs.pop(key)

        if 'password' in conn_kwargs:
            conn_kwargs['passwd'] = conn_kwargs.pop('password')

        if "database" in conn_kwargs:
            conn_kwargs["dbname"] = conn_kwargs.pop("database")
        if "db" in conn_kwargs:
            conn_kwargs["dbname"] = conn_kwargs.pop("db")
        if "passwd" in conn_kwargs:
            conn_kwargs["password"] = conn_kwargs.pop("passwd")
        dsn = " ".join(["%s=%s" % (k, _param_escape(str(v)))
                                for (k, v) in conn_kwargs.items()])

        if "max_size" not in pool_kwargs:
            pool_kwargs["max_size"] = 32
        if "auto_shrink" not in pool_kwargs:
            pool_kwargs["auto_shrink"] = True
        return momoko.Pool(dsn = dsn, **pool_kwargs)

    def close(self):
        with self._lock:
            if self.deferred:
                raise Exception('Error, database not properly initialized '
                                'before closing connection')

            if not self._closed and self._conn_pool:
                self._conn_pool.close()
                self._closed = True
                return True
            return False

    def connect(self, reuse_if_open=False):
        with self._lock:
            if self.deferred:
                raise Exception('Error, database must be initialized before '
                                'opening a connection.')

            self._conn_pool = self._connect()
            self._initialize_connection(self._conn_pool)
        return True

    @gen.coroutine
    def connection(self):
        if self.is_closed():
            self.connect()
            yield self._conn_pool.connect()
        conn = yield self._conn_pool.getconn(False)
        raise gen.Return(conn)

    @gen.coroutine
    def execute_sql(self, sql, params=None, commit=SENTINEL):
        if commit is SENTINEL:
            if self.commit_select:
                commit = True
            else:
                commit = not sql[:6].lower().startswith('select')

        conn = yield self.connection()
        try:
            cursor = yield conn.execute(sql, params or ())
        except Exception:
            if self.autorollback and self.autocommit:
                yield conn.execute("ROLLBACK;")
            raise
        else:
            if commit and self.autocommit:
                yield conn.execute("COMMIT;")
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

    def _close(self, conn):
        self._conn_pool.putconn(conn)