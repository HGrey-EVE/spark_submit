#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: component_conf_check_util
:Author: xufeng@bbdservice.com 
:Date: 2021-05-12 8:00 PM
:Version: v.1.0
:Description:检查组件配置是否正确
"""


class ComponentCheckUtil:

    @staticmethod
    def check_mysql(ip, port, username, password, db_name) -> bool:
        return True

    @staticmethod
    def check_es() -> bool:
        return True

    @staticmethod
    def check_kafka() -> bool:
        return True
