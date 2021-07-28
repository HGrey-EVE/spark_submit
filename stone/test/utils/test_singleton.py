#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""

from whetstone.utils.singleton import SingletonClass, Singleton


def test_singleton_1():
    class A(metaclass=SingletonClass):
        pass

    a1 = A()
    a2 = A()
    a3 = A()
    assert a1 is a2
    assert a1 is a3
    assert a3 is a2


def test_singleton_2():
    @Singleton
    class A:
        pass

    a1 = A()
    a2 = A()
    a3 = A()
    assert a1 is a2
    assert a1 is a3
    assert a3 is a2
