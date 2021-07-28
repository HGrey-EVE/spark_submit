#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-12-20 16:51
"""
import threading


class SingletonClass(type):
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with SingletonClass._lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(SingletonClass, cls).__call__(*args, **kwargs)
        return cls._instance


class Singleton:
    _lock = threading.Lock()
    _instances = {}

    def __init__(self, _class):
        self._class = _class

    def __call__(self, *args, **kwargs):
        if self._class.__name__ not in Singleton._instances:
            with Singleton._lock:
                if self._class.__name__ not in Singleton._instances:
                    Singleton._instances[self._class.__name__] = self._class(*args, **kwargs)

        return Singleton._instances[self._class.__name__]
