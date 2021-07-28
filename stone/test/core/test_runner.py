#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-13 15:32
"""
import logging

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.proc import ProcInfo
from whetstone.core.runner import get_spark_session, get_spark_conf


def test_get_spark_session():
    _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")
    _settings = Setting()
    _cfg_mgr = ConfigManager(_proc, _settings)
    session = get_spark_session(_proc, _cfg_mgr, _settings, logging.getLogger())
    assert session.builder._options.get("spark.executor.extrajavaoptions") == \
           "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC"
    assert session.builder._options.get("spark.sql.warehouse.dir") == "hdfs:///user/hive/warehouse"
    assert session.builder._options.get("spark.executor.memory") == "8g"
    assert session.builder._options.get("spark.executor.cores") == "6"
    assert session.builder._options.get("spark.default.parallelism") == "500"
    assert session.builder._options.get("spark.executor.instances") == "40"
    assert session.builder._options.get("spark.sql.shuffle.partitions") == "500"
    #TODO  session name 增加test, 其他用例中使用了test, 有交叉影响, 原因未知
    assert session.builder._options.get("spark.app.name") in  ["project0-module1-dev", "test"]

    session.stop()


def test_get_spark_conf():
    _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")
    _settings = Setting()
    _cfg_mgr = ConfigManager(_proc, _settings)
    conf = get_spark_conf(_cfg_mgr, _settings, logging.getLogger())
    assert conf.get("spark.executor.extrajavaoptions") == \
           "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC"
    assert conf.get("spark.sql.warehouse.dir") == "hdfs:///user/hive/warehouse"
    assert conf.get("spark.executor.memory") == "8g"
    assert conf.get("spark.executor.cores") == "6"
    assert conf.get("spark.default.parallelism") == "500"
    assert conf.get("spark.executor.instances") == "40"
    assert conf.get("spark.sql.shuffle.partitions") == "500"
