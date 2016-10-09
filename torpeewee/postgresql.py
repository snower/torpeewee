# -*- coding: utf-8 -*-
# 16/6/28
# create by: snower

import re
from tornado import gen
from peewee import PostgresqlDatabase as BasePostgresqlDatabase, IndexMetadata, ColumnMetadata, ForeignKeyMetadata, sort_models_topologically
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

    def push_execution_context(self, transaction):
        raise NotImplementedError

    def pop_execution_context(self):
        raise NotImplementedError

    def execution_context_depth(self):
        raise NotImplementedError

    def execution_context(self, with_transaction=True):
        raise NotImplementedError

    def push_transaction(self, transaction):
        raise NotImplementedError

    def pop_transaction(self):
        raise NotImplementedError

    def transaction_depth(self):
        raise NotImplementedError

    @gen.coroutine
    def transaction(self):
        raise NotImplementedError

    def commit_on_success(self, func):
        raise NotImplementedError

    def savepoint(self, sid=None):
        raise NotImplementedError

    def atomic(self):
        raise NotImplementedError

    def _get_pk_sequence(self, model):
        meta = model._meta
        if meta.primary_key is not False and meta.primary_key.sequence:
            return meta.primary_key.sequence
        elif meta.auto_increment:
            return '%s_%s_seq' % (meta.db_table, meta.primary_key.db_column)

    def last_insert_id(self, cursor, model):
        sequence = self._get_pk_sequence(model)
        if not sequence:
            return

        meta = model._meta
        if meta.schema:
            schema = '%s.' % meta.schema
        else:
            schema = ''

        cursor.execute("SELECT CURRVAL('%s\"%s\"')" % (schema, sequence))
        result = cursor.fetchone()[0]
        if self.get_autocommit():
            self.commit()
        return result

    @gen.coroutine
    def get_tables(self, schema='public'):
        query = ('SELECT tablename FROM pg_catalog.pg_tables '
                 'WHERE schemaname = %s ORDER BY tablename')
        raise gen.Return([r for r, in (yield self.execute_sql(query, (schema,))).fetchall()])

    @gen.coroutine
    def get_indexes(self, table, schema='public'):
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
        cursor = yield self.execute_sql(query, (table, 'r', schema))
        raise gen.Return([IndexMetadata(row[0], row[1], row[3].split(','), row[2], table)
                for row in cursor.fetchall()])

    @gen.coroutine
    def get_columns(self, table, schema='public'):
        query = """
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position"""
        cursor = yield self.execute_sql(query, (table, schema))
        pks = set((yield self.get_primary_keys(table, schema)))
        raise gen.Return([ColumnMetadata(name, dt, null == 'YES', name in pks, table)
                for name, null, dt in cursor.fetchall()])

    @gen.coroutine
    def get_primary_keys(self, table, schema='public'):
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
        cursor = yield self.execute_sql(query, ('PRIMARY KEY', table, schema))
        raise gen.Return([row for row, in cursor.fetchall()])

    @gen.coroutine
    def get_foreign_keys(self, table, schema='public'):
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
        cursor = yield self.execute_sql(sql, (table, schema))
        raise gen.Return([ForeignKeyMetadata(row[0], row[1], row[2], table)
                for row in cursor.fetchall()])

    @gen.coroutine
    def sequence_exists(self, sequence):
        res = yield self.execute_sql("""
            SELECT COUNT(*) FROM pg_class, pg_namespace
            WHERE relkind='S'
                AND pg_class.relnamespace = pg_namespace.oid
                AND relname=%s""", (sequence,))
        raise gen.Return(bool(res.fetchone()[0]))

    @gen.coroutine
    def set_search_path(self, *search_path):
        path_params = ','.join(['%s'] * len(search_path))
        yield self.execute_sql('SET search_path TO %s' % path_params, search_path)

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


class Transaction(BaseTransaction, AsyncPostgresqlDatabase):
    def __init__(self, database):
        AsyncPostgresqlDatabase.__init__(self, database.database)

        self.database = database
        self.connection = None

    @gen.coroutine
    def get_cursor(self):
        raise NotImplementedError()

    @gen.coroutine
    def execute_sql(self, sql, params=None, require_commit=True):
        if self.connection is None:
            yield self.begin()

        with self.database.exception_wrapper():
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
        kwargs["threadlocals"] = False

        super(PostgresqlDatabase, self).__init__(*args, **kwargs)

        self._closed = True
        self._conn_pool = None

    def _connect(self, database, **kwargs):
        pool_kwargs = {}
        for key in ['connection_factory', 'cursor_factory', 'size', 'max_size', 'ioloop', 'raise_connect_errors',
                    'reconnect_interval','setsession', 'auto_shrink', 'shrink_delay', 'shrink_period']:
            if key in kwargs:
                pool_kwargs[key] = kwargs.pop(key)

        conn_kwargs = {"dbname": database}
        conn_kwargs.update(kwargs)
        if 'password' in conn_kwargs:
            conn_kwargs['passwd'] = conn_kwargs.pop('password')

        if "database" in conn_kwargs:
            conn_kwargs["dbname"] = conn_kwargs.pop("database")
        if "db" in conn_kwargs:
            conn_kwargs["dbname"] = conn_kwargs.pop("db")
        if "passwd" in conn_kwargs:
            conn_kwargs["password"] = conn_kwargs.pop("passwd")
        dsn = " ".join(["%s=%s" % (k, _param_escape(str(v)))
                                for (k, v) in conn_kwargs.iteritems()])

        if "max_size" not in pool_kwargs:
            pool_kwargs["max_size"] = 32
        return momoko.Pool(dsn = dsn, **pool_kwargs)

    def close(self):
        with self._conn_lock:
            if self.deferred:
                raise Exception('Error, database not properly initialized '
                                'before closing connection')
            with self.exception_wrapper():
                if not self._closed and self._conn_pool:
                    self._conn_pool.close()
                    self._closed = True

    @gen.coroutine
    def get_conn(self):
        if self._closed:
            with self.exception_wrapper():
                self._conn_pool = self._connect(self.database, **self.connect_kwargs)
                self._closed = False
                self.initialize_connection(self._conn_pool)
            yield self._conn_pool.connect()
        conn = yield self._conn_pool.getconn(False)
        raise gen.Return(conn)

    @gen.coroutine
    def execute_sql(self, sql, params=None, require_commit=True):
        with self.exception_wrapper():
            conn = yield self.get_conn()
            try:
                cursor = yield conn.execute(sql, params or ())
            except Exception:
                if self.get_autocommit() and self.autorollback:
                    yield conn.execute("ROLLBACK;")
                raise
            else:
                if require_commit and self.get_autocommit():
                    yield conn.execute("COMMIT;")
            finally:
                self._close(conn)
        raise gen.Return(cursor)

    def transaction(self, args_name = "transaction"):
        return TransactionFuture(self, args_name)

    def commit_on_success(self, func):
        return self.transaction()(func)

    def _close(self, conn):
        self._conn_pool.putconn(conn)