#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: single_util
:Author: xufeng@bbdservice.com 
:Date: 2021-05-11 1:54 PM
:Version: v.1.0
:Description:
"""
import threading
import weakref


class Cached(type):
    _lock = threading.Lock()

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls.__cache = weakref.WeakKeyDictionary()

    def __call__(cls, *args, **kwargs):
        if args in cls.__cache:
            return cls.__cache[args]

        with Cached._lock:
            if args not in cls.__cache:
                obj = super().__call__(*args, **kwargs)
                cls.__cache[args] = obj
            return cls.__cache[args]


class SingletonClass(type):
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with SingletonClass._lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(SingletonClass, cls).__call__(*args, **kwargs)
        return cls._instance
