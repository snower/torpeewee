#!/usr/bin/env python
# encoding: utf-8

from tornado.testing import AsyncTestCase

class BaseTestCase(AsyncTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()

    def tearDown(self):
        super(BaseTestCase, self).tearDown()