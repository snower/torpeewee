# -*- coding: utf-8 -*-
# 18/5/22
# create by: snower

import datetime
from torpeewee import *
from aiohttp import web

db = MySQLDatabase("test", host="127.0.0.1", user="root", passwd="123456")

class BaseModel(Model):
    class Meta:
        database = db

class Test(BaseModel):
    id = IntegerField(primary_key= True)
    data = CharField(max_length=64, null=False)
    created_at = DateTimeField()

async def show_handle(request):
    datas = [t.data for t in await Test.select()]
    return web.Response(text = u"<br />".join(datas))

async def create_handle(request):
    data = await request.post()
    data = data["data"]
    await Test.create(data=data, created_at=datetime.datetime.now())
    return web.HTTPFound('/')

app = web.Application()
app.add_routes([
    web.get('/', show_handle),
    web.post('/', create_handle)
])

web.run_app(app)