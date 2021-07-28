#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: enums
:Author: xufeng@bbdservice.com 
:Date: 2021-05-11 3:31 PM
:Version: v.1.0
:Description:
"""


class MySqlSection:
    """
    四川外管局设计到抽取数据的mysql和最终保存数据的mysql
    两者是可以不一样的，这里将两者都加进来，注意和conf文件中的分段的名字(加[]的）对应
    """
    MYSQL_DESTINATION = 'mysql-destination'
    MYSQL_SOURCE = 'mysql-source'


class TableRowOperation:
    """
    增量表相关常量: 字段名及相关的操作
    """

    OPERATION_COLUMN = 'operation'
    ADD = 1
    UPDATE = 2
    DELETE = 3
