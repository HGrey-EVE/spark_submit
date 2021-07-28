#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: json_util
:Author: xufeng@bbdservice.com 
:Date: 2021-04-21 10:59 AM
:Version: v.1.0
:Description: from hainan-fta project
"""
import json
from datetime import datetime, date
import traceback


class DateEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


class JsonUtils:

    @staticmethod
    def to_object(obj_str):
        return json.loads(obj_str)

    @staticmethod
    def to_obj_ignore_ex(obj_str):
        """
        json转对象，忽略掉异常
        :return:
        """
        try:
            return json.loads(obj_str)
        except Exception:
            print(f'json loads error:{traceback.format_exc()}')
        return None

    @staticmethod
    def to_string(obj):
        """
        将 list ，map 转化为 json 字符串
        :param obj:
        :return:
        """
        return json.dumps(obj, ensure_ascii=False, cls=DateEncoder)
