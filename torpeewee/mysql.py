# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from peewee import MySQLDatabase as BaseMySQLDatabase, IndexMetadata, ColumnMetadata, ForeignKeyMetadata, sort_models, SENTINEL
from .transaction import Transaction as BaseTransaction

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

    async def transaction(self, transaction_type=None):
        raise NotImplementedError

    def commit_on_success(self, func):
        raise NotImplementedError

    def savepoint(self, sid=None):
        raise NotImplementedError

    def atomic(self, transaction_type=None):
        raise NotImplementedError

    async def table_exists(self, table, schema=None):
        return table.__name__ in (await self.get_tables(schema=schema))

    async def get_tables(self, schema=None):
        return [row for row, in (await self.execute_sql('SHOW TABLES'))]

    async def get_indexes(self, table, schema=None):
        cursor = await self.execute_sql('SHOW INDEX FROM `%s`' % table)
        unique = set()
        indexes = {}
        for row in cursor.fetchall():
            if not row[1]:
                unique.add(row[2])
            indexes.setdefault(row[2], [])
            indexes[row[2]].append(row[4])
        return [IndexMetadata(name, None, indexes[name], name in unique, table)
                for name in indexes]

    async def get_columns(self, table, schema=None):
        sql = """
            SELECT column_name, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = DATABASE()"""
        cursor = await self.execute_sql(sql, (table,))
        pks = set(self.get_primary_keys(table))
        return [ColumnMetadata(name, dt, null == 'YES', name in pks, table, df)
                for name, null, dt, df in cursor.fetchall()]

    async def get_primary_keys(self, table, schema=None):
        cursor = await self.execute_sql('SHOW INDEX FROM `%s`' % table)
        return [row[4] for row in
                filter(lambda row: row[2] == 'PRIMARY', cursor.fetchall())]

    async def get_foreign_keys(self, table, schema=None):
        query = """
            SELECT column_name, referenced_table_name, referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s
                AND table_schema = DATABASE()
                AND referenced_table_name IS NOT NULL
                AND referenced_column_name IS NOT NULL"""
        cursor = await self.execute_sql(query, (table,))
        return [
            ForeignKeyMetadata(column, dest_table, dest_column, table)
            for column, dest_table, dest_column in cursor.fetchall()]

    async def sequence_exists(self, seq):
        raise NotImplementedError

    async def create_tables(self, models, **options):
        for model in sort_models(models):
            await model.create_table(**options)

    async def drop_tables(self, models, **kwargs):
        for model in reversed(sort_models(models)):
            await model.drop_table(**kwargs)


class Transaction(BaseTransaction, AsyncMySQLDatabase):
    def __init__(self, database, args_name):
        AsyncMySQLDatabase.__init__(self, database.database)
        BaseTransaction.__init__(self, database, args_name)

        self.connection = None

class MySQLDatabase(AsyncMySQLDatabase):
    commit_select = True

    def __init__(self, *args, **kwargs):
        if tormysql is None or tormysql.version < '0.3.8':
            raise ImportError("use MySQL require install tormysql>=0.3.8")

        kwargs["thread_safe"] = False
        self._closed = True
        self._conn_pool = None

        super(MySQLDatabase, self).__init__(*args, **kwargs)

        self.connect_params["autocommit"] = False

    def is_closed(self):
        return self._closed

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

    async def connection(self):
        if self.is_closed():
            self.connect()
            self._closed = False
        conn = await self._conn_pool.Connection()
        return conn

    def connect(self, reuse_if_open=False):
        with self._lock:
            if self.deferred:
                raise Exception('Error, database must be initialized before '
                                'opening a connection.')

            self._conn_pool = self._connect()
            self._initialize_connection(self._conn_pool)
        return True

    async def execute_sql(self, sql, params=None, commit=SENTINEL):
        if commit is SENTINEL:
            if self.commit_select:
                commit = True
            else:
                commit = not sql[:6].lower().startswith('select')

        conn = await self.connection()
        try:
            cursor = conn.cursor()
            await cursor.execute(sql, params or ())
            await cursor.close()
        except Exception:
            if self.autorollback:
                await conn.rollback()
            raise
        else:
            if commit:
                await conn.commit()
        finally:
            await self._close(conn)
        return cursor

    async def cursor(self, commit=None):
        conn = await self.connection()
        return conn.cursor()

    def transaction(self, args_name = "transaction"):
        return Transaction(self, args_name)

    def commit_on_success(self, func):
        return self.transaction()(func)

    async def _close(self, conn):
        await conn.close()