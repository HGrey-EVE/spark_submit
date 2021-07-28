#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-18 14:15
"""
import datetime
import logging
import sys
from logging import Logger
from unittest import mock

from pyspark import StorageLevel, SparkConf
from pyspark.sql import SparkSession

from whetstone.common.settings import Setting
from whetstone.core import index
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.entry import Entry
from whetstone.core.index import Executor, Parameter, register, Index, SQL
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


def test_executor_register():
    @register(name="df2", desc="desc1", dependencies=["df1"], persistent="parquet", cache=True,
              cache_level=StorageLevel.MEMORY_ONLY)
    def df2(entry: Entry, param: Parameter, logger: Logger):
        logger.info("df2 executed")
        return entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                           ["id", "index", "value"])

    @register(name="df1", desc="desc1", dependencies=["df1"], persistent="parquet",
              persistent_param={"mode": "overwrite"})
    def df1(entry: Entry, param: Parameter, logger: Logger):
        logger.info("df1 executed")
        return entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                           ["id", "index", "value"])

    assert len(Executor._views) == 2
    assert Executor._views.get("df1")._persistent_param == {"mode": "overwrite"}

    assert Executor._views.get("df2").dependencies == ["df1"]
    assert Executor._views.get("df2")._cache is True
    assert Executor._views.get("df2")._cache_level is StorageLevel.MEMORY_ONLY
    assert Executor._views.get("df2")._persistent == "parquet"
    assert Executor._views.get("df2")._desc == "desc1"
    assert Executor._views.get("df2")._persistent_param == {}


def test_executor_execute_1():
    @register(name="df2", desc="desc1", dependencies=["df1"],
              persistent="parquet", persistent_param={"mode": "overwrite"},
              cache=True, cache_level=StorageLevel.MEMORY_ONLY)
    def index_df2(entry: Entry, param: Parameter, logger: Logger):
        logger.error("df2 executed")
        logger.error(param.get("data"))
        df2 = entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                          ["id", "index", "value"])
        df2.show()
        entry.spark.sql("select * from df1 where id = 1 and value= 2").show()
        return df2

    @register(name="df1", desc="desc1", dependencies=[])
    def index_df1(entry: Entry, param: Parameter, logger: Logger):
        logger.error("df1 executed")
        logger.error(param.get("data"))
        df1 = entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                          ["id", "index", "value"])
        df1.show()
        return df1

    index.get_index_hdfs_path = mock.Mock(return_value="./index/")

    _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")
    _settings = Setting()
    _path_mgr = PathManager()
    _cfg_mgr = ConfigManager(_proc, _settings)
    _logger = logging.getLogger()
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _logger.setLevel(logging.INFO)
    # session = get_spark_session(_proc, _cfg_mgr, _settings, logger)
    conf = SparkConf()
    conf.setAppName("test").setMaster("local[1]")
    session = SparkSession \
        .builder \
        .appName(f"test") \
        .config(conf=conf).getOrCreate()
    # .enableHiveSupport() \

    _entry = Entry() \
        .build_proc(_proc) \
        .build_spark(session) \
        .build_cfg_mgr(_cfg_mgr) \
        .build_logger(_logger) \
        .build_path_mgr(_path_mgr) \
        .build_settings(_settings)

    _param = Parameter()
    _param.update({"data": datetime.datetime.now().strftime("%Y-%m-%d")})

    Executor.execute(_entry, _param, _logger, "df1", "df2")
    session.stop()


def test_executor_execute_2():
    @register(name="df2", desc="desc1", dependencies=["df1"],
              cache=True, cache_level=StorageLevel.MEMORY_ONLY)
    def index_df2(entry: Entry, param: Parameter, logger: Logger):
        logger.error("df2 executed")
        logger.error(param.get("data"))
        df2 = entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                          ["id", "index", "value"])
        df2.show()
        return df2

    @register(name="df1", desc="desc1", dependencies=[])
    def index_df1(entry: Entry, param: Parameter, logger: Logger):
        logger.error("df1 executed")
        logger.error(param.get("data"))
        df1 = entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                          ["id", "index", "value"])
        df1.show()
        return df1

    index.check_index_hdfs_data_exists = mock.Mock(return_value=False)

    i1 = Index("df3", SQL("SELECT * FROM df1 WHERE id = 1", "df33"),
               SQL("SELECT * FROM df33 WHERE id = 1 and value = 1", ""))
    i2 = Index("df4", SQL("SELECT * FROM df2 WHERE id = 2 and value = 4", ""))
    i3 = Index("df6", SQL("SELECT * FROM df5 WHERE id = 2 and value = 4", ""))

    Executor.register(name=i1.name, df_func=i1.execute, dependencies=["df1"])
    Executor.register(name=i2.name, df_func=i2.execute, dependencies=["df2"])
    Executor.register(name=i3.name, df_func=i3.execute, dependencies=["df5"])

    _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True, script="module11")
    _settings = Setting()
    _path_mgr = PathManager()
    _cfg_mgr = ConfigManager(_proc, _settings)
    _logger = logging.getLogger()
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _logger.setLevel(logging.INFO)
    # session = get_spark_session(_proc, _cfg_mgr, _settings, logger)
    conf = SparkConf()
    conf.setAppName("test").setMaster("local[1]")
    session = SparkSession \
        .builder \
        .appName(f"test") \
        .config(conf=conf).getOrCreate()
    # .enableHiveSupport() \

    _entry = Entry() \
        .build_proc(_proc) \
        .build_spark(session) \
        .build_cfg_mgr(_cfg_mgr) \
        .build_logger(_logger) \
        .build_path_mgr(_path_mgr) \
        .build_settings(_settings)

    _param = Parameter()
    _param.update({"data": datetime.datetime.now().strftime("%Y-%m-%d")})

    df0 = _entry.spark.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                       ["id", "index", "value"])
    df0.createOrReplaceTempView("df0")
    df = _entry.spark.sql("SELECT * FROM df0 WHERE id = 2")
    Executor.register(name="df5", df_func=df)

    Executor.execute(_entry, _param, _logger, "df3", "df4", "df6")
    session.stop()


def test_pyspark_1():
    from pyspark import SparkConf
    conf = SparkConf().setAppName("wordcount").setMaster("local[1]")
    session = SparkSession \
        .builder \
        .appName(f"test") \
        .config(conf=conf).getOrCreate()
    df = session.createDataFrame([["1", "指标1", 1], ["1", "指标2", 2], ["2", "指标1", 3], ["2", "指标2", 4]],
                                 ["id", "index", "value"])
    df.show()
    session.stop()


def test_cycle_dependency():
    # def check_deps(original, current_index, depends_dict, cycle_cnt, current_cycle, depends_chain):
    #
    #     depends_index = depends_dict[current_index]
    #     if not depends_index:
    #         return False
    #
    #     if original in depends_index:
    #         print(f"find cycle depends: {depends_chain}")
    #         return True
    #
    #     if current_cycle >= cycle_cnt:
    #         return True
    #
    #     for idx in depends_index:
    #
    #         if idx in depends_chain:
    #             depends_chain.append(idx)
    #             print(f"find cycle depends: {depends_chain}")
    #             return True
    #
    #         depends_chain.append(idx)
    #         has_cycle = check_deps(original, idx, depends_dict, cycle_cnt, current_cycle + 1, depends_chain)
    #         if has_cycle:
    #             return has_cycle
    #     else:
    #         return False
    #
    # def check_cycle_dependency(get_dependency_lst):
    #     depends_dict = {item[0]: item[1] for item in get_dependency_lst}
    #
    #     cycle_cnt = len(depends_dict)
    #
    #     for index in depends_dict:
    #
    #         dependency_index = depends_dict[index]
    #         if not dependency_index:
    #             continue
    #
    #         for dep_index in dependency_index:
    #             has_cycle = check_deps(index, dep_index, depends_dict, cycle_cnt, 1, [index, dep_index])
    #             if has_cycle:
    #                 return True
    #     return False

    def check_depends_yield(depends_dict, depends_chain, current_idx):

        if current_idx not in depends_dict:
            raise Exception(f"index {current_idx} not in index dict")

        if current_idx[-1] in depends_chain[:-1]:
            yield depends_chain
            print(f"find one depends chain:{depends_chain}")
            return

        current_depends = depends_dict[current_idx]
        for idx in current_depends:
            yield from check_depends_yield(depends_dict, depends_chain + (idx,), idx)

    def check_cycle_dependency(get_dependency_lst):
        depends_dict = {item[0]: item[1] for item in get_dependency_lst}
        for index in depends_dict:
            ret = []
            for chain in check_depends_yield(depends_dict, (index,), index):
                ret.append(chain)
            if ret:
                print(ret)
                print('==================\n')

    target = [
        ("A", ["B", "C"]),
        ("C", ["B", "D", "X", "Y"]),
        ("D", ["E"]),
        ("E", ["F"]),
        ("X", ["B"]),
        ("Y", []),
        ("B", ["A"]),
        ("F", ["A"]),
    ]


    assert check_cycle_dependency(target)
