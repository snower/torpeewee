# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

from peewee import PostgresqlDatabase as BasePostgresqlDatabase, IndexMetadata, ColumnMetadata, ForeignKeyMetadata, sort_models, SENTINEL
from .transaction import Transaction as BaseTransaction

try:
    import aiopg
except ImportError:
    aiopg = None

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
        query = ('SELECT tablename FROM pg_catalog.pg_tables '
                 'WHERE schemaname = %s ORDER BY tablename')

        cursor = await self.execute_sql(query, (schema or 'public',))
        return [table for table, in cursor.fetchall()]

    async def get_indexes(self, table, schema=None):
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
        cursor = await self.execute_sql(query, (table, 'r', schema or 'public'))
        return [IndexMetadata(row[0], row[1], row[3].split(','), row[2], table)
                for row in cursor.fetchall()]

    async def get_columns(self, table, schema=None):
        query = """
            SELECT column_name, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position"""
        cursor = await self.execute_sql(query, (table, schema or 'public'))
        pks = set(self.get_primary_keys(table, schema))
        return [ColumnMetadata(name, dt, null == 'YES', name in pks, table, df)
                for name, null, dt, df in cursor.fetchall()]

    async def get_primary_keys(self, table, schema=None):
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
        cursor = await self.execute_sql(query, (ctype, table, schema or 'public'))
        return [pk for pk, in cursor.fetchall()]

    async def get_foreign_keys(self, table, schema=None):
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
        cursor = await self.execute_sql(sql, (table, schema or 'public'))
        return [ForeignKeyMetadata(row[0], row[1], row[2], table)
                for row in cursor.fetchall()]

    async def sequence_exists(self, seq):
        raise NotImplementedError

    async def create_tables(self, models, **options):
        for model in sort_models(models):
            await model.create_table(**options)

    async def drop_tables(self, models, **kwargs):
        for model in reversed(sort_models(models)):
            await model.drop_table(**kwargs)

class Cursor(object):
    _cursor = None

    def __init__(self, cursor):
        self._cursor = cursor

    def fetchone(self):
        ret = self._cursor._impl.fetchone()
        assert not self._cursor._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    def fetchmany(self, size=None):
        if size is None:
            size = self._cursor._impl.arraysize
        ret = self._cursor._impl.fetchmany(size)
        assert not self._cursor._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    def fetchall(self):
        ret = self._cursor._impl.fetchall()
        assert not self._cursor._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    def scroll(self, value, mode="relative"):
        ret = self._cursor._impl.scroll(value, mode)
        assert not self._cursor._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    def close(self):
        pass

    def __getattr__(self, item):
        return getattr(self._cursor, item)

class Transaction(BaseTransaction, AsyncPostgresqlDatabase):
    def __init__(self, database, args_name):
        AsyncPostgresqlDatabase.__init__(self, database.database)
        BaseTransaction.__init__(self, database, args_name)

        self.connection = None
        self.cursor = None
        self.aiopg_transaction = None

    async def execute_sql(self, sql, params=None, commit=SENTINEL):
        if self.connection is None:
            await self.begin()

        cursor = await self.connection.cursor()
        await cursor.execute(sql, params or ())
        return Cursor(cursor)

    async def begin(self):
        self.connection = await self.database.connection()
        try:
            self.cursor = await self.connection.cursor()
            self.aiopg_transaction = await self.cursor.begin()
        except:
            await self.close()
            raise
        return self

    async def commit(self):
        if self.connection:
            await self.aiopg_transaction.commit()
            await self.close()

    async def rollback(self):
        if self.connection:
            await self.aiopg_transaction.rollback()
            await self.close()

    async def close(self):
        if self.connection:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                await self.database._close(self.connection)
            self.connection = None
            self.cursor = None
            self.aiopg_transaction = None

class PostgresqlDatabase(AsyncPostgresqlDatabase):
    def __init__(self, *args, **kwargs):
        if aiopg is None or aiopg.version < '0.14.0':
            raise ImportError("use Postgresql require install aiopg>=0.14.0")

        kwargs["thread_safe"] = False
        self._closed = True
        self._conn_pool = None

        super(PostgresqlDatabase, self).__init__(*args, **kwargs)

    def is_closed(self):
        return self._closed

    def _connect(self):
        conn_kwargs = {
            "database": self.database,
        }

        conn_kwargs.update(self.connect_params)
        if 'passwd' in conn_kwargs:
            conn_kwargs['password'] = conn_kwargs.pop('passwd')
        if "db" in conn_kwargs:
            conn_kwargs["database"] = conn_kwargs.pop("db")
        if "maxsize" not in conn_kwargs:
            conn_kwargs["maxsize"] = 32
        return aiopg.create_pool(None, **conn_kwargs)

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

    async def connection(self):
        if self.is_closed():
            self.connect()
            self._conn_pool = await self._conn_pool
            self._closed = False
        conn = await self._conn_pool.acquire()
        return conn

    async def execute_sql(self, sql, params=None, commit=SENTINEL):
        if commit is SENTINEL:
            if self.commit_select:
                commit = True
            else:
                commit = not sql[:6].lower().startswith('select')

        if self.autorollback or commit:
            conn = await self.connection()
            try:
                cursor = await conn.cursor()
                transaction = await cursor.begin()
                try:
                    cursor = await conn.cursor()
                    await cursor.execute(sql, params or ())
                except Exception:
                    if self.autorollback:
                        await transaction.rollback()
                    raise
                else:
                    if commit:
                        await transaction.commit()
            finally:
                await self._close(conn)
            return Cursor(cursor)

        conn = await self.connection()
        try:
            cursor = await conn.cursor()
            await cursor.execute(sql, params or ())
        finally:
            await self._close(conn)
        return Cursor(cursor)

    async def cursor(self, commit=None):
        conn = await self.connection()
        return conn.cursor()

    def transaction(self, args_name = "transaction"):
        return Transaction(self, args_name)

    def commit_on_success(self, func):
        return self.transaction()(func)

    async def _close(self, conn):
        await self._conn_pool.release(conn)