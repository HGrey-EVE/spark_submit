#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-06-08 10:52
"""
import logging
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.entry import Entry
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo
from whetstone.utils.index import index_join_merge


class TestIndices:

    def setup_class(self):
        _proc = ProcInfo(project="project0", module="module1", env="dev", version="20210501", debug=True)
        _settings = Setting()
        _path_mgr = PathManager()
        _cfg_mgr = ConfigManager(_proc, _settings)

        _logger = logging.getLogger()
        _logger.addHandler(logging.StreamHandler(sys.stdout))
        _logger.setLevel(logging.INFO)

        conf = SparkConf()
        conf.setAppName("test").setMaster("local[2]")
        session = SparkSession \
            .builder \
            .appName(f"test") \
            .config(conf=conf).getOrCreate()

        self.entry = Entry() \
            .build_proc(_proc) \
            .build_spark(session) \
            .build_cfg_mgr(_cfg_mgr) \
            .build_logger(_logger) \
            .build_path_mgr(_path_mgr) \
            .build_settings(_settings)

    def teardown_class(self):
        self.entry.spark.stop()

    def test_index_merge(self):
        target_company_df = self.entry.spark.read.csv("./new_company_list-20200714.csv", header=True, sep=",")
        index1 = self.entry.spark.createDataFrame([["5393225", 11],
                                                   ["8290591", 12],
                                                   ["21712617", 13]],
                                                  ["corp_code", "index1"])
        index2 = self.entry.spark.createDataFrame([["50050276", 21],
                                                   ["50050348", 22],
                                                   ["50050866", 23]],
                                                  ["corp_code", "index2"])
        index3 = self.entry.spark.createDataFrame([["50050997", 31]],
                                                  ["corp_code", "index3"])
        index4 = self.entry.spark.createDataFrame([["50051180", 41]],
                                                  ["corp_code", "index4"])
        rst = index_join_merge(self.entry.spark, target_company_df, target_columns=["corp_code", "company_name"],
                    index_df_list=[index1, index2, index3, index4],
                    index_columns=["index1", "index2", "index3", "index4"],
                    join_columns=["corp_code"],
                    hdfs_tmp_path="./",
                    slice_step_num=10)
        rst.show()