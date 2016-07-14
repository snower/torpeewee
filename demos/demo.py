# -*- coding: utf-8 -*-
# 16/7/14
# create by: snower

import datetime
import tornado.ioloop
import tornado.web
import tornado.gen
from torpeewee import *

db = MySQLDatabase("test", host="127.0.0.1", user="root", passwd="123456")

class BaseModel(Model):
    class Meta:
        database = db

class Test(BaseModel):
    id = IntegerField(primary_key= True)
    data = CharField(max_length=64, null=False)
    created_at = DateTimeField()

class MainHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        datas = [t.data for t in (yield Test.select())]
        self.write(u"<br />".join(datas))

    @tornado.gen.coroutine
    def post(self):
        data = self.get_body_argument("data")
        yield Test.create(data = data, created_at = datetime.datetime.now())
        self.redirect("/")

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()