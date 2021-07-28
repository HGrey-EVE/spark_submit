#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-26 11:23
"""
import logging
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.entry import Entry
from whetstone.core.index import Executor
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


class TestSpark:

    def setup_class(self):
        _proc = ProcInfo(project="scwgj", module="index_compute", env="dev", version="20210501", debug=True)
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

        # target_company_df = self.entry.spark.read.csv("./new_company_list-20200714.csv", header=True, sep=",")
        # Executor.register(name=f"target_company_v", df_func=target_company_df, dependencies=[])

    def teardown_class(self):
        self.entry.spark.stop()

    def test_1(self):
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '121010'],
            ["8290591", 2000, '2021-05-04', '121020'],
            ["21712617", 3003, '2021-05-04', '121040'],
            ["50050276", 4004, '2020-05-04', '121080']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code"])
        pboc_corp_rcv_jn_df.createOrReplaceTempView("pboc_corp_rcv_jn")

        print(pboc_corp_rcv_jn_df.columns)

        pboc_corp_lcy_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, '2021-05-04'],
            ["8290591", 1000, '2021-05-04'],
            ["21712617", 3003, '2021-05-04'],
            ["50050276", 4004, '2020-05-04']],
            schema=["corp_code", "salefx_amt_usd", "deal_date"])
        pboc_corp_lcy_df.createOrReplaceTempView("pboc_corp_lcy")

        df = self.entry.spark.sql("""
        select a.*, b.* 
        from pboc_corp_rcv_jn a 
        left join pboc_corp_lcy b on a.corp_code=b.corp_code
        """)
        df.show()


    def test_2(self):
        pass
