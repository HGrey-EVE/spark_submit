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


class LogTrack:
    @staticmethod
    def log_track(function):
        def wrapper(*args, **kwargs):
            args[1].logger.info(f" ==== START EXEC method {function.__name__} ====")
            result = function(*args, **kwargs)
            args[1].logger.info(f" ====  ENG  EXEC method {function.__name__} ====")
            return result
        return wrapper

