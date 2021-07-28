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
    0: 更新
    1: 删除
    2: 新增
    """
    OPERATION_COLUMN = 'operator'
    UPDATE = 0
    DELETE = 1
    ADD = 2


class MysqlDataChangeTableINfo:
    """
    -- ----------------------------
    --  Table structure for `data_change`
    -- ----------------------------
    DROP TABLE IF EXISTS `data_change`;
    CREATE TABLE `data_change` (
      `table_name` varchar(255) DEFAULT NULL COMMENT '原始数据对应数据导入的表',
      `uid` varchar(255) DEFAULT NULL COMMENT '原始数据的唯一ID',
      `properties` longtext COMMENT '要更新的属性json字符串: {"key1":"value1", "key2":"value2"}',
      `timestamp` datetime DEFAULT NULL COMMENT '数据更新时间',
      `operator` tinyint(4) DEFAULT NULL COMMENT '0表示更新，1表示删除,2表示新增'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """

    TABLE_NAME = 'data_change'
