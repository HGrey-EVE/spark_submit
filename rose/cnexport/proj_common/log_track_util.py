#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: json_util
:Author: weifuwan@bbdservice.com
:Date: 2021-04-22 10:59 AM
:Version: v.1.0
:Description: log track
"""


def log_track():
    def actrual_decorator(function):
        def wrapper(*args, **kwargs):
            flag = ""
            table = ""
            if "hbase" in function.__name__:
                flag = "hbase"
                table = str(args[0].table_name) + args[0].calc_date_str  + " --"
            elif "index" in function.__name__:
                flag = "index"
            elif "sql" in function.__name__:
                flag = "sql"
            args[0].logger.info(f" (°ο°)~~ START EXEC method {function.__name__} , -- {flag} -- {table} waiting ~~(°ο°)")
            result = function(*args, **kwargs)
            args[0].logger.info(f" O(∩_∩)O  ENG  EXEC method {function.__name__} , -- {flag} -- {table} sucess ~~*@ο@*~~")
            return result
        return wrapper
    return actrual_decorator

