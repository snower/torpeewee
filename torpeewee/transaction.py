# -*- coding: utf-8 -*-
# 16/8/30
# create by: snower

import sys
from functools import wraps
import asyncio
from peewee import SENTINEL

class Transaction(object):
    def __init__(self, database, args_name):
        self.database = database
        self.args_name = args_name

    def _connect(self, database, **kwargs):
        raise NotImplementedError

    async def connection(self):
        if self.connection is None:
            await self.begin()

        return self.connection

    async def get_cursor(self):
        if self.connection is None:
            await self.begin()

        cursor = await self.connection.cursor()
        return cursor

    async def execute_sql(self, sql, params=None, commit=SENTINEL):
        if self.connection is None:
            await self.begin()

        cursor = self.connection.cursor()
        await cursor.execute(sql, params or ())
        await cursor.close()
        return cursor

    async def begin(self):
        self.connection = await self.database.connection()
        await self.connection.begin()
        return self

    async def commit(self):
        if self.connection:
            await self.connection.commit()
            await self.close()

    async def rollback(self):
        if self.connection:
            await self.connection.rollback()
            await self.close()

    def __call__(self, func):
        @wraps(func)
        async def _(*args, **kwargs):
            await self.begin()
            try:
                kwargs[self.args_name] = self
                result = await func(*args, **kwargs)
            except:
                exc_info = sys.exc_info()
                await self.rollback()
                raise exc_info[1].with_traceback(exc_info[2])
            else:
                await self.commit()
                return result
        return _

    def __await__(self):
        coroutine = self.begin()
        return coroutine.__await__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is None:
            return

        if exc_type:
            asyncio.ensure_future(self.rollback())
        else:
            try:
                asyncio.ensure_future(self.commit())
            except:
                exc_info = sys.exc_info()
                self.rollback()
                raise exc_info[1].with_traceback(exc_info[2])

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection is None:
            return

        if exc_type:
            await self.rollback()
        else:
            try:
                await self.commit()
            except:
                exc_info = sys.exc_info()
                await self.rollback()
                raise exc_info[1].with_traceback(exc_info[2])

    async def close(self):
        if self.connection:
            await self.database._close(self.connection)
            self.connection = None

    def __del__(self):
        asyncio.ensure_future(self.rollback())