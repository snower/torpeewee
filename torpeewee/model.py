# -*- coding: utf-8 -*-
# 16/6/29
# create by: snower

import sys
from tornado import gen
from peewee import Model as BaseModel, SchemaManager as BaseSchemaManager, ModelAlias, BaseQuery, IntegrityError, DoesNotExist, __deprecated__
from .query import QueryFuture, ModelSelectFuture as ModelSelect, NoopModelSelectFuture as NoopModelSelect, \
    ModelUpdateFuture as ModelUpdate, ModelInsertFuture as ModelInsert, \
    ModelDeleteFuture as ModelDelete, ModelRawFuture as ModelRaw
from .transaction import TransactionFuture

if sys.version_info[0] == 3:
    basestring = str

class SchemaManager(BaseSchemaManager):
    @gen.coroutine
    def create_table(self, safe=True, **options):
        yield self.database.execute(self._create_table(safe=safe, **options))

    @gen.coroutine
    def drop_table(self, safe=True, **options):
        yield self.database.execute(self._drop_table(safe=safe, **options))

    @gen.coroutine
    def create_indexes(self, safe=True):
        for query in self._create_indexes(safe=safe):
            yield self.database.execute(query)

    @gen.coroutine
    def drop_indexes(self, safe=True):
        for query in self._drop_indexes(safe=safe):
            yield self.database.execute(query)

    @gen.coroutine
    def create_sequence(self, field):
        seq_ctx = self._create_sequence(field)
        if seq_ctx is not None:
            yield self.database.execute(seq_ctx)

    @gen.coroutine
    def drop_sequence(self, field):
        seq_ctx = self._drop_sequence(field)
        if seq_ctx is not None:
            yield self.database.execute(seq_ctx)

    @gen.coroutine
    def create_foreign_key(self, field):
        yield self.database.execute(self._create_foreign_key(field))

    @gen.coroutine
    def create_sequences(self):
        if self.database.sequences:
            for field in self.model._meta.sorted_fields:
                if field.sequence:
                    yield self.create_sequence(field)

    @gen.coroutine
    def create_all(self, safe=True, **table_options):
        yield self.create_sequences()
        yield self.create_table(safe, **table_options)
        yield self.create_indexes(safe=safe)

    @gen.coroutine
    def drop_sequences(self):
        if self.database.sequences:
            for field in self.model._meta.sorted_fields:
                if field.sequence:
                    yield self.drop_sequence(field)

    @gen.coroutine
    def drop_all(self, safe=True, drop_sequences=True, **options):
        yield self.drop_table(safe, **options)
        if drop_sequences:
            yield self.drop_sequences()

class Model(BaseModel):
    class Meta:
        schema_manager_class = SchemaManager

    def __init__(self, *args, **kwargs):
        super(Model, self).__init__(*args, **kwargs)

        self._use_database = None

        def _(using):
            self._use_database = using
            return self

        self.use = _

    @classmethod
    def select(cls, *fields):
        is_default = not fields
        if not fields:
            fields = cls._meta.sorted_fields
        return ModelSelect(cls, fields, is_default=is_default)

    @classmethod
    def update(cls, __data=None, **update):
        return ModelUpdate(cls, cls._normalize_data(__data, update))

    @classmethod
    def insert(cls, __data=None, **insert):
        return ModelInsert(cls, cls._normalize_data(__data, insert))

    @classmethod
    def insert_many(cls, rows, fields=None):
        return ModelInsert(cls, insert=rows, columns=fields)

    @classmethod
    def insert_from(cls, query, fields):
        columns = [getattr(cls, field) if isinstance(field, basestring)
                   else field for field in fields]
        return ModelInsert(cls, insert=query, columns=columns)

    @classmethod
    def raw(cls, sql, *params):
        return ModelRaw(cls, sql, params)

    @classmethod
    def delete(cls):
        return ModelDelete(cls)

    @classmethod
    @gen.coroutine
    def create(cls, **query):
        inst = cls(**query)
        yield inst.save(force_insert=True)
        raise gen.Return(inst)

    @classmethod
    def noop(cls):
        return NoopModelSelect(cls, ())

    @classmethod
    @gen.coroutine
    def get_or_none(cls, *query, **filters):
        try:
            result = yield cls.get(*query, **filters)
        except DoesNotExist:
            result = None
        raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def get_or_create(cls, **kwargs):
        defaults = kwargs.pop('defaults', {})
        query = cls.select()
        for field, value in kwargs.items():
            query = query.where(getattr(cls, field) == value)

        try:
            result = yield query.get(), False
        except cls.DoesNotExist:
            try:
                if defaults:
                    kwargs.update(defaults)
                with cls._meta.database.atomic():
                    result = yield cls.create(**kwargs), True
            except IntegrityError as exc:
                try:
                    result = yield query.get(), False
                except cls.DoesNotExist:
                    raise exc
        raise gen.Return(result)

    @gen.coroutine
    def save(self, force_insert=False, only=None, using = None):
        use_database = using or self._use_database

        field_dict = self.__data__.copy()
        if self._meta.primary_key is not False:
            pk_field = self._meta.primary_key
            pk_value = self._pk
        else:
            pk_field = pk_value = None
        if only:
            field_dict = self._prune_fields(field_dict, only)
        elif self._meta.only_save_dirty and not force_insert:
            field_dict = self._prune_fields(field_dict, self.dirty_fields)
            if not field_dict:
                self._dirty.clear()
                raise gen.Return(False)

        self._populate_unsaved_relations(field_dict)
        if pk_value is not None and not force_insert:
            if self._meta.composite_key:
                for pk_part_name in pk_field.field_names:
                    field_dict.pop(pk_part_name, None)
            else:
                field_dict.pop(pk_field.name, None)
            if use_database:
                rows = yield self.__class__.use(use_database).update(**field_dict).where(self._pk_expr()).execute()
            else:
                rows = yield self.update(**field_dict).where(self._pk_expr()).execute()
        elif pk_field is None or not self._meta.auto_increment:
            if use_database:
                yield self.__class__.use(use_database).insert(**field_dict).execute()
            else:
                yield self.insert(**field_dict).execute()
            rows = 1
        else:
            if use_database:
                pk_from_cursor = yield self.__class__.use(use_database).insert(**field_dict).execute()
            else:
                pk_from_cursor = yield self.insert(**field_dict).execute()
            if pk_from_cursor is not None:
                pk_value = pk_from_cursor
            self._pk = pk_value
            rows = 1
        self._dirty.clear()
        raise gen.Return(rows)

    def dependencies(self, search_nullable=False, using = None):
        def _generator():
            data = yield None
            while data:
                if isinstance(data, Exception):
                    raise data
                data = yield data
            raise StopIteration()

        @gen.coroutine
        def _(self, search_nullable, using, generator):
            try:
                use_database = using or self._use_database

                model_class = type(self)
                if use_database:
                    query = yield self.__class__.use(use_database).select(self._meta.primary_key).where(self._pk_expr())
                else:
                    query = yield self.select(self._meta.primary_key).where(self._pk_expr())
                stack = [(type(self), query)]
                seen = set()

                while stack:
                    klass, query = stack.pop()
                    if klass in seen:
                        continue
                    seen.add(klass)
                    for fk, rel_model in klass._meta.backrefs.items():
                        if rel_model is model_class:
                            node = (fk == self.__data__[fk.rel_field.name])
                        else:
                            node = fk << query
                        if use_database:
                            subquery = (yield rel_model.use(use_database).select(rel_model._meta.primary_key)
                                        .where(node))
                        else:
                            subquery = (yield rel_model.select(rel_model._meta.primary_key)
                                        .where(node))
                        if not fk.null or search_nullable:
                            stack.append((rel_model, subquery))
                        generator.send((node, fk))

                generator.send(None)
            except Exception as e:
                generator.send(e)

        generator = next(_generator())
        _(self, search_nullable, using, generator)
        return generator

    @gen.coroutine
    def delete_instance(self, recursive=False, delete_nullable=False, using = None):
        use_database = using or self._use_database
        if recursive:
            dependencies = self.dependencies(delete_nullable, using=using)
            for query, fk in reversed(list(dependencies)):
                model = fk.model
                if fk.null and not delete_nullable:
                    if use_database:
                        yield model.use(use_database).update(**{fk.name: None}).where(query).execute()
                    else:
                        yield model.update(**{fk.name: None}).where(query).execute()
                else:
                    if use_database:
                        yield model.use(use_database).delete().where(query).execute()
                    else:
                        yield model.delete().where(query).execute()
        if use_database:
            raise gen.Return((yield self.__class__.use(use_database).delete().where(self._pk_expr()).execute()))
        raise gen.Return((yield self.delete().where(self._pk_expr()).execute()))

    @classmethod
    @gen.coroutine
    def create_table(cls, safe=True, **options):
        if 'fail_silently' in options:
            __deprecated__('"fail_silently" has been deprecated in favor of '
                           '"safe" for the create_table() method.')
            safe = options.pop('fail_silently')

        if safe and not cls._meta.database.safe_create_index \
                and (yield cls.table_exists()):
            raise gen.Return(None)
        yield cls._schema.create_all(safe, **options)

    @classmethod
    @gen.coroutine
    def drop_table(cls, safe=True, drop_sequences=True, **options):
        if safe and not cls._meta.database.safe_drop_index \
                and not (yield cls.table_exists()):
            raise gen.Return(None)
        yield cls._schema.drop_all(safe, drop_sequences, **options)

    @classmethod
    def use(cls, database):
        return Using(cls, database)


class Using(object):
    model_class = None
    database = None

    def __init__(self, model_class, database):
        self.model_class = model_class
        if isinstance(database, TransactionFuture):
            database = database.transaction
        self.database = database

    def __getattr__(self, key):
        attr = getattr(self.model_class, key)
        if callable(attr):
            def inner(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, QueryFuture):
                    result.bind(self.database)
                elif isinstance(result, BaseQuery):
                    result.bind(self.database)
                return result
            super(Using, self).__setattr__(key, inner)
            return inner
        return attr

    def __setattr__(self, key, value):
        if self.model_class is not None and not hasattr(self, key):
            return setattr(self.model_class, key, value)
        return super(Using, self).__setattr__(key, value)

    @gen.coroutine
    def create(self, **query):
        inst = self.model_class(**query)
        yield inst.save(force_insert=True, using=self.database)
        raise gen.Return(inst)

    @gen.coroutine
    def get_or_create(self, **kwargs):
        defaults = kwargs.pop('defaults', {})
        query = self.model_class.select().bind(self.database)
        for field, value in kwargs.items():
            query = query.where(getattr(self.model_class, field) == value)

        try:
            result = yield query.get(), False
        except self.model_class.DoesNotExist:
            try:
                if defaults:
                    kwargs.update(defaults)
                result = yield self.create(**kwargs), True
            except IntegrityError as exc:
                try:
                    result = yield query.get(), False
                except self.model_class.DoesNotExist:
                    raise exc
        raise gen.Return(result)

    def alias(self):
        model_alias = ModelAlias(self.model_class)
        return Using(model_alias, self.database)