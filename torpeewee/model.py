# -*- coding: utf-8 -*-
# 16/6/29
# create by: snower

from tornado import gen
from peewee import Model as BaseModel, ModelAlias, IntegrityError
from .query import SelectQuery, UpdateQuery, InsertQuery, DeleteQuery, RawQuery

class Model(BaseModel):
    @classmethod
    def select(cls, *selection):
        query = SelectQuery(cls, *selection)
        if cls._meta.order_by:
            query = query.order_by(*cls._meta.order_by)
        return query

    @classmethod
    def update(cls, __data=None, **update):
        fdict = __data or {}
        fdict.update([(cls._meta.fields[f], update[f]) for f in update])
        return UpdateQuery(cls, fdict)

    @classmethod
    def insert(cls, __data=None, **insert):
        fdict = __data or {}
        fdict.update([(cls._meta.fields[f], insert[f]) for f in insert])
        return InsertQuery(cls, fdict)

    @classmethod
    def insert_many(cls, rows, validate_fields=True):
        return InsertQuery(cls, rows=rows, validate_fields=True)

    @classmethod
    def insert_from(cls, fields, query):
        return InsertQuery(cls, fields=fields, query=query)

    @classmethod
    def delete(cls):
        return DeleteQuery(cls)

    @classmethod
    def raw(cls, sql, *params):
        return RawQuery(cls, sql, *params)

    @classmethod
    def use(cls, database):
        return Using(cls, database)

    @classmethod
    @gen.coroutine
    def create(cls, **query):
        inst = cls(**query)
        yield inst.save(force_insert=True)
        inst._prepare_instance()
        raise gen.Return(inst)

    @classmethod
    @gen.coroutine
    def get(cls, *query, **kwargs):
        sq = cls.select().naive()
        if query:
            sq = sq.where(*query)
        if kwargs:
            sq = sq.filter(**kwargs)
        result = yield sq.get()
        raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def get_or_create(cls, **kwargs):
        defaults = kwargs.pop('defaults', {})
        query = cls.select()
        for field, value in kwargs.items():
            if '__' in field:
                query = query.filter(**{field: value})
            else:
                query = query.where(getattr(cls, field) == value)

        try:
            result = (yield query.get()), False
        except cls.DoesNotExist:
            try:
                params = dict((k, v) for k, v in kwargs.items()
                              if '__' not in k)
                params.update(defaults)
                result = (yield cls.create(**params)), True
            except IntegrityError as exc:
                try:
                    result = (yield query.get()), False
                except cls.DoesNotExist:
                    raise exc
        raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def create_or_get(cls, **kwargs):
        try:
            result = (yield cls.create(**kwargs)), True
        except IntegrityError:
            query = []  # TODO: multi-column unique constraints.
            for field_name, value in kwargs.items():
                field = getattr(cls, field_name)
                if field.unique or field.primary_key:
                    query.append(field == value)
            result = (yield cls.get(*query)), False
        raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def table_exists(cls):
        kwargs = {}
        if cls._meta.schema:
            kwargs['schema'] = cls._meta.schema
        tables = yield cls._meta.database.get_tables(**kwargs)
        raise gen.Return(cls._meta.db_table in tables)

    @classmethod
    @gen.coroutine
    def create_table(cls, fail_silently=False):
        if fail_silently and (yield cls.table_exists()):
            return

        db = cls._meta.database
        pk = cls._meta.primary_key
        if db.sequences and pk is not False and pk.sequence:
            if not (yield db.sequence_exists(pk.sequence)):
                yield db.create_sequence(pk.sequence)

        yield db.create_table(cls)
        yield cls._create_indexes()

    @classmethod
    @gen.coroutine
    def _create_indexes(cls):
        db = cls._meta.database
        for field in cls._fields_to_index():
            yield db.create_index(cls, [field], field.unique)

        if cls._meta.indexes:
            for fields, unique in cls._meta.indexes:
                yield db.create_index(cls, fields, unique)

    @classmethod
    @gen.coroutine
    def drop_table(cls, fail_silently=False, cascade=False):
        yield cls._meta.database.drop_table(cls, fail_silently, cascade)

    @classmethod
    @gen.coroutine
    def truncate_table(cls, restart_identity=False, cascade=False):
        yield cls._meta.database.truncate_table(cls, restart_identity, cascade)

    @gen.coroutine
    def save(self, force_insert=False, only=None):
        field_dict = dict(self._data)
        if self._meta.primary_key is not False:
            pk_field = self._meta.primary_key
            pk_value = self._get_pk_value()
        else:
            pk_field = pk_value = None
        if only:
            field_dict = self._prune_fields(field_dict, only)
        elif self._meta.only_save_dirty and not force_insert:
            field_dict = self._prune_fields(
                field_dict,
                self.dirty_fields)
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
            rows = yield self.update(**field_dict).where(self._pk_expr()).execute()
        elif pk_field is None:
            yield self.insert(**field_dict).execute()
            rows = 1
        else:
            pk_from_cursor = yield self.insert(**field_dict).execute()
            if pk_from_cursor is not None:
                pk_value = pk_from_cursor
            self._set_pk_value(pk_value)
            rows = 1
        self._dirty.clear()
        raise gen.Return(rows)

    @gen.coroutine
    def delete_instance(self, recursive=False, delete_nullable=False):
        if recursive:
            dependencies = self.dependencies(delete_nullable)
            for query, fk in reversed(list(dependencies)):
                model = fk.model_class
                if fk.null and not delete_nullable:
                    yield model.update(**{fk.name: None}).where(query).execute()
                else:
                    yield model.delete().where(query).execute()
        result = yield self.delete().where(self._pk_expr()).execute()
        raise gen.Return(result)


class Using(object):
    model_class = None
    database = None

    def __init__(self, model_class, database):
        self.model_class = model_class
        self.database = database

    def __getattr__(self, key):
        attr = getattr(self.model_class, key)
        if not callable(attr):
            return attr

        def attr_proxy(*args, **kwargs):
            ori_database = self.model_class._meta.database
            self.model_class._meta.database = self.database
            try:
                return attr(*args, **kwargs)
            finally:
                self.model_class._meta.database = ori_database
        return attr_proxy

    def __setattr__(self, key, value):
        if self.model_class and self.database:
            return setattr(self.model_class, key, value)
        return super(Using, self).__setattr__(key, value)

    def alias(self):
        model_alias = ModelAlias(self.model_class)
        return Using(model_alias, self.database)