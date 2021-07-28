#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-26 11:23
"""
import datetime
import logging
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.entry import Entry
from whetstone.core.index import Executor, Parameter
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


def has_same_element_list(list1, list2):
    flag = True
    if len(list1) != len(list2):
        return False
    for element in list1:
        if element not in list2:
            flag = False
            break
    return flag


def has_same_k_v_map(map1, map2):
    flag = True
    if len(map1) != len(map2):
        return False
    for k, v in map1.items():
        if map2.get(k) != v:
            flag = False
            break
    return flag


class TestIndices:

    def setup_class(self):
        _proc = ProcInfo(project="scwgj", module="index_compute", env="dev", version="20210501", debug=True)
        _settings = Setting()
        _path_mgr = PathManager()
        _cfg_mgr = ConfigManager(_proc, _settings)

        _logger = logging.getLogger()
        _logger.addHandler(logging.StreamHandler(sys.stdout))
        _logger.setLevel(logging.INFO)

        conf = SparkConf()
        conf.setAppName("test").setMaster("local[1]")
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

        target_company_df = self.entry.spark.read.csv("./new_company_list-20200714.csv", header=True, sep=",")
        Executor.register(name=f"target_company_v", df_func=target_company_df, dependencies=[])

    def teardown_class(self):
        self.entry.spark.stop()

    def test_corp_inc_settle_prop(self):
        from scwgj.index_compute.indices.corp_inc_settle_prop import corp_inc_settle_prop
        assert corp_inc_settle_prop
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '121010'],
            ["8290591", 2000, '2021-05-04', '121020'],
            ["21712617", 3003, '2021-05-04', '121040'],
            ["50050276", 4004, '2020-05-04', '121080']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])

        pboc_corp_lcy_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, '2021-05-04'],
            ["8290591", 1000, '2021-05-04'],
            ["21712617", 3003, '2021-05-04'],
            ["50050276", 4004, '2020-05-04']],
            schema=["corp_code", "salefx_amt_usd", "deal_date"])
        Executor.register(name=f"pboc_corp_lcy_v", df_func=pboc_corp_lcy_df, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_inc_settle_prop")

        self.entry.spark.sql("""
            select * 
            from corp_inc_settle_prop 
            where corp_code in ('5393225', '8290591', '21712617', '50050276')
        """).show()

    def test_import_exp_trade_prop_2y(self):
        from scwgj.index_compute.indices.import_exp_trade_prop_2y import import_exp_trade_prop_2y
        assert import_exp_trade_prop_2y

        # give the test data: pboc_corp_pay_jn_v
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, 1, "121010", "2021-05-01"],
            ["8290591", 600, 2, "121020", "2021-04-01"],
            ["21712617", 700, 3, "121030", "2021-03-01"],
            ["50050276", 800, 4, "121080", "2021-02-01"],
            ["50050348", 900, 5, "121090", "2021-01-01"],
            ["50050866", 1000, 6, "121990", "2021-05-15"],
            ["50050997", 1100, 7, "120010", "2020-12-01"],
            ["50051180", None, 7, "120010", "2020-12-01"],
            ["50051842", None, 7, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "pay_date"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        # give the test data: pboc_imp_custom_rpt_v
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-05-01"],
            ["8290591", 10, "2021-04-01"],
            ["21712617", 10, "2021-03-01"],
            ["50050276", 10, "2021-02-01"],
            ["50050348", 10, "2021-01-01"],
            ["50050997", 10, "2020-12-01"],
            ["50051180", 10, "2020-12-01"],
            ["50051842", None, "2020-12-01"]],
            schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])

        # the params of executing
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        # executing the target index func
        Executor.execute(self.entry, param, self.entry.logger, "import_exp_trade_prop_2y")
        # retrieve the executing result from entry
        result_df = self.entry.spark.sql("""select * 
                                            from import_exp_trade_prop_2y
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_df.show()
        result_row_list = result_df.collect()

        # 期望的 corp_code list
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180', '50051842']
        # 期望的 import_exp_trade_prop_2y 的值
        expected_corp_code_index_map = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 0.1,
            "50050866": 1000.0,
            "50050276": 80.0,
            "50050348": 90.0,
            "50051180": 0.1,
            "50051842": None,
            "50050997": 0.1
        }

        # 取出实际业务计算相关的数据
        actual_result_corp_code_list = [row["corp_code"] for row in result_row_list]
        actual_result_corp_code_index_map = {row["corp_code"]: row["import_exp_trade_prop_2y"]
                                             for row in result_row_list}

        assert has_same_element_list(expected_corp_code_list, actual_result_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_index_map, actual_result_corp_code_index_map)

    def test_import_exp_trade_prop(self):
        from scwgj.index_compute.indices.import_exp_trade_prop import import_exp_trade_prop
        assert import_exp_trade_prop

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "121010", "2021-05-01"],
            ["8290591", 600, "121020", "2021-04-01"],
            ["21712617", 700, "121030", "2021-03-01"],
            ["50050276", 800, "121080", "2021-02-01"],
            ["50050348", 900, "121090", "2021-01-01"],
            ["50050866", 1000, "121990", "2021-05-15"],
            ["50050997", 1100, "120010", "2020-12-01"],
            ["50051180", None, "120010", "2020-12-01"],
            ["50051842", None, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "tx_code", "pay_date"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-05-01"],
            ["8290591", 10, "2021-04-01"],
            ["21712617", 10, "2021-03-01"],
            ["50050276", 10, "2021-02-01"],
            ["50050348", 10, "2021-01-01"],
            ["50050997", 10, "2020-12-01"],
            ["50051180", 10, "2020-12-01"],
            ["50051842", None, "2020-12-01"]],
            schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "import_exp_trade_prop")
        result_df = self.entry.spark.sql("""select * from import_exp_trade_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_exp_trade_prop"]
                                    for row in result_rows_list}

        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 0.1,
            "50050866": 1000.0,
            "50050276": 80.0,
            "50050348": 90.0,
            "50051180": 0.1,
            "50051842": None,
            "50050997": 0.1
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_import_trade_prepay_prop(self):
        from scwgj.index_compute.indices.import_trade_prepay_prop import import_trade_prepay_prop
        assert import_trade_prepay_prop

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "A"],
            ["8290591", 600, "A"],
            ["21712617", 700, "B"],
            ["50050276", 800, "C"],
            ["50050348", 900, "A"],
            ["50050866", 1000, "A"],
            ["50050997", 1100, "A"],
            ["50051180", None, "A"],
            ["50051842", None, "A"]],
            schema=["corp_code", "tx_amt_usd", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10],
            ["8290591", 10],
            ["21712617", 10],
            ["50050276", 10],
            ["50050348", 10],
            ["50050997", 10],
            ["50051180", 10],
            ["50051842", None]],
            schema=["corp_code", "deal_amt_usd"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "import_trade_prepay_prop")
        result_df = self.entry.spark.sql("""select * from import_trade_prepay_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_trade_prepay_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 0.1,
            "50050276": 0.1,
            "50050348": 90.0,
            "50050866": 1000.0,
            "50050997": 110.0,
            "50051180": 0.1,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_import_trade_prepay_prop_1y(self):
        from scwgj.index_compute.indices.import_trade_prepay_prop_1y import import_trade_prepay_prop_1y
        assert import_trade_prepay_prop_1y

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "A", "2021-06-01"],
            ["8290591", 600, "A", "2021-05-01"],
            ["21712617", 700, "B", "2021-04-01"],
            ["50050276", 800, "C", "2021-04-15"],
            ["50050348", 900, "A", "2021-03-01"],
            ["50050866", 1000, "A", "2021-02-01"],
            ["50050997", 1100, "A", "2021-01-01"],
            ["50051180", None, "A", "2021-01-15"],
            ["50051842", None, "A", "2021-05-01"]],
            schema=["corp_code", "tx_amt_usd", "rcvpay_attr_code", "pay_date"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-06-01"],
            ["8290591", 10, "2021-05-01"],
            ["21712617", 10, "2021-04-01"],
            ["50050276", 10, "2021-04-15"],
            ["50050348", 10, "2021-03-01"],
            ["50050997", 10, "2021-01-01"],
            ["50051180", 10, "2021-01-15"],
            ["50051842", None, "2021-05-01"]],
            schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "import_trade_prepay_prop_1y")
        result_df = self.entry.spark.sql("""select * from import_trade_prepay_prop_1y 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_trade_prepay_prop_1y"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 0.1,
            "50050276": 0.1,
            "50050348": 90.0,
            "50050866": 1000.0,
            "50050997": 110.0,
            "50051180": 0.1,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_export_inc_trade_prop(self):
        from scwgj.index_compute.indices.export_inc_trade_prop import export_inc_trade_prop
        assert export_inc_trade_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "A", "2021-06-01", 1],
            ["8290591", 600, "A", "2021-05-01", 2],
            ["21712617", 700, "B", "2021-04-01", 3],
            ["50050276", 800, "C", "2021-04-15", 4],
            ["50050348", 900, "A", "2021-03-01", 5],
            ["50050866", 1000, "A", "2021-02-01", 6],
            ["50050997", 1100, "A", "2021-01-01", 7],
            ["50051180", None, "A", "2021-01-15", 8],
            ["50051842", None, "A", "2021-05-01", 9]],
            schema=["corp_code", "tx_amt_usd", "rcvpay_attr_code", "rcv_date", "rptno"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-06-01"],
            ["8290591", 10, "2021-05-01"],
            ["21712617", 10, "2021-04-01"],
            ["50050276", 10, "2021-04-15"],
            ["50050348", 10, "2021-03-01"],
            ["50050997", 10, "2021-01-01"],
            ["50051180", 10, "2021-01-15"],
            ["50051842", None, "2021-05-01"]],
            schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "export_inc_trade_prop")
        result_df = self.entry.spark.sql("""select * from export_inc_trade_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["export_inc_trade_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 0.1,
            "50050276": 0.1,
            "50050348": 90.0,
            "50050866": 1000.0,
            "50050997": 110.0,
            "50051180": 0.1,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_export_txamt_dealamt_prop(self):
        from scwgj.index_compute.indices.export_txamt_dealamt_prop import export_txamt_dealamt_prop
        assert export_txamt_dealamt_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, 1, "121010", "2021-05-01"],
            ["8290591", 600, 2, "121020", "2021-04-01"],
            ["21712617", 700, 3, "121030", "2021-03-01"],
            ["50050276", 800, 4, "121080", "2021-02-01"],
            ["50050348", 900, 5, "121090", "2021-01-01"],
            ["50050866", 1000, 6, "121990", "2021-05-15"],
            ["50050997", 1100, 7, "120010", "2020-12-01"],
            ["50051180", None, 7, "120010", "2020-12-01"],
            ["50051842", None, 7, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-05-01"],
            ["8290591", 10, "2021-04-01"],
            ["21712617", 10, "2021-03-01"],
            ["50050276", 10, "2021-02-01"],
            ["50050348", 10, "2021-01-01"],
            ["50050997", 10, "2020-12-01"],
            ["50051180", 10, "2020-12-01"],
            ["50051842", None, "2020-12-01"]],
            schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "export_txamt_dealamt_prop")
        result_df = self.entry.spark.sql("""select * from export_txamt_dealamt_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["export_txamt_dealamt_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 0.1,
            "50050276": 80.0,
            "50050348": 90.0,
            "50050866": 1000.0,
            "50050997": 0.1,
            "50051180": 0.1,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_servpay_genrcv_prop(self):
        from scwgj.index_compute.indices.corp_servpay_genrcv_prop import corp_servpay_genrcv_prop
        assert corp_servpay_genrcv_prop

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2001", "2021-05-01"],
            ["8290591", 600, "2002", "2021-04-01"],
            ["21712617", 700, "2003", "2021-03-01"],
            ["50050276", 800, "2004", "2021-02-01"],
            ["50050348", 900, "1998", "2021-01-01"],
            ["50050866", 1000, "1997", "2021-05-15"],
            ["50050997", 1100, "2010", "2020-12-01"],
            ["50051180", None, "1879", "2020-12-01"],
            ["50051842", None, "2010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "tx_code", "pay_date"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01"],
            ["8290591", 10, 2, "121020", "2021-04-01"],
            ["21712617", 10, 3, "121030", "2021-03-01"],
            ["50050276", 10, 4, "121080", "2021-02-01"],
            ["50050348", 10, 5, "121090", "2021-01-01"],
            ["50050866", 10, 6, "121990", "2021-05-15"],
            ["50050997", 10, 7, "120010", "2020-12-01"],
            ["50051180", None, 7, "120010", "2020-12-01"],
            ["50051842", None, 7, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_servpay_genrcv_prop")
        result_df = self.entry.spark.sql("""select * from corp_servpay_genrcv_prop 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_servpay_genrcv_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": 700.0,
            "50050276": 80.0,
            "50050348": 0.1,
            "50050866": 0.1,
            "50050997": 1100.0,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_exp_inc_prop(self):
        from scwgj.index_compute.indices.corp_exp_inc_prop import corp_exp_inc_prop
        assert corp_exp_inc_prop

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "121010", "2021-05-01"],
            ["8290591", 600, "121020", "2021-04-01"],
            ["21712617", 700, "121030", "2021-03-01"],
            ["50050276", 800, "121080", "2021-02-01"],
            ["50050348", 900, "121090", "2021-01-01"],
            ["50050866", 1000, "121990", "2021-05-15"],
            ["50050997", 1100, "120010", "2020-12-01"],
            ["50051180", None, "120010", "2020-12-01"],
            ["50051842", None, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "tx_code", "pay_date"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01"],
            ["8290591", 10, 2, "121020", "2021-04-01"],
            ["21712617", 10, 3, "121030", "2021-03-01"],
            ["50050276", 10, 4, "121080", "2021-02-01"],
            ["50050348", 10, 5, "121090", "2021-01-01"],
            ["50050866", 10, 6, "121990", "2021-05-15"],
            ["50050997", 10, 7, "120010", "2020-12-01"],
            ["50051180", None, 7, "120010", "2020-12-01"],
            ["50051842", None, 7, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_exp_inc_prop")
        result_df = self.entry.spark.sql("""select * from corp_exp_inc_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_exp_inc_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": None,
            "50050276": 80.0,
            "50050348": 90.0,
            "50050866": 100.0,
            "50050997": None,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_rmbpay_genrcv_prop(self):
        from scwgj.index_compute.indices.corp_rmbpay_genrcv_prop import corp_rmbpay_genrcv_prop
        assert corp_rmbpay_genrcv_prop

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "CNY", "2021-05-01"],
            ["8290591", 600, "CNY", "2021-04-01"],
            ["21712617", 700, "USD", "2021-03-01"],
            ["50050276", 800, "CNY", "2021-02-01"],
            ["50050348", 900, "CNY", "2021-01-01"],
            ["50050866", 1000, "FRI", "2021-05-15"],
            ["50050997", 1100, "CNY", "2020-12-01"],
            ["50051180", None, "CNY", "2020-12-01"],
            ["50051842", None, "HKD", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "tx_ccy_code", "pay_date"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01"],
            ["8290591", 10, 2, "121020", "2021-04-01"],
            ["21712617", 10, 3, "121030", "2021-03-01"],
            ["50050276", 10, 4, "121080", "2021-02-01"],
            ["50050348", 10, 5, "121090", "2021-01-01"],
            ["50050866", 10, 6, "121990", "2021-05-15"],
            ["50050997", 10, 7, "120010", "2020-12-01"],
            ["50051180", None, 7, "120010", "2020-12-01"],
            ["50051842", None, 7, "120010", "2020-12-01"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_rmbpay_genrcv_prop")
        result_df = self.entry.spark.sql("""select * from corp_rmbpay_genrcv_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_rmbpay_genrcv_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 50.0,
            "8290591": 60.0,
            "21712617": None,
            "50050276": 80.0,
            "50050348": 90.0,
            "50050866": 0.1,
            "50050997": 1100.0,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_yf_prepay_ct_prop(self):
        from scwgj.index_compute.indices.yf_prepay_ct_prop import yf_prepay_ct_prop
        assert yf_prepay_ct_prop

        # step1: giving the test data
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["21712617", 700, "2021-03-01", 1, "A", "121030"],
            ["21712617", 700, "2021-03-01", 1, "B", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 900, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "B", "121990"],
            ["50050997", 1100, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "B", "120010"],
            ["50051842", None, "2020-12-01", 1, "A", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "yf_prepay_ct_prop")
        result_df = self.entry.spark.sql("""select * from yf_prepay_ct_prop 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["yf_prepay_ct_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 1.0,
            "8290591": 1.0,
            "21712617": 1.0,
            "50050276": 1.0,
            "50050348": 90.0,
            "50050866": 0.1,
            "50050997": 1.0,
            "50051180": None,
            "50051842": 1.0
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_ys_preinc_ct_prop(self):
        from scwgj.index_compute.indices.ys_preinc_ct_prop import ys_preinc_ct_prop
        assert ys_preinc_ct_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A"],
            ["8290591", 10, 2, "121020", "2021-04-01", "B"],
            ["21712617", 10, 3, "121030", "2021-03-01", "C"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B"],
            ["50050997", 10, 7, "120010", "2020-12-01", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A"],
            ["50051842", None, 7, "120010", "2020-12-01", "A"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "ys_preinc_ct_prop")
        result_df = self.entry.spark.sql("""select * from ys_preinc_ct_prop 
                                                            where corp_code in ('5393225', '8290591', 
                                                                                '21712617', '50050276', 
                                                                                '50050348', '50050866', 
                                                                                '50050997', '50051180',
                                                                                '50051842')
                                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["ys_preinc_ct_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 1.0,
            "8290591": 1.0,
            "21712617": None,
            "50050276": 1.0,
            "50050348": 90.0,
            "50050866": 0.1,
            "50050997": None,
            "50051180": 1.0,
            "50051842": 1.0
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_ys_preinc_amt_prop(self):
        from scwgj.index_compute.indices.ys_preinc_amt_prop import ys_preinc_amt_prop
        assert ys_preinc_amt_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A"],
            ["8290591", 10, 2, "121020", "2021-04-01", "B"],
            ["21712617", 10, 3, "121030", "2021-03-01", "C"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B"],
            ["50050997", 10, 7, "120010", "2020-12-01", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A"],
            ["50051842", None, 7, "120010", "2020-12-01", "A"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "ys_preinc_amt_prop")
        result_df = self.entry.spark.sql("""select * from ys_preinc_amt_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["ys_preinc_amt_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 1.0,
            "8290591": 0.1,
            "21712617": None,
            "50050276": 1.0,
            "50050348": 1.0,
            "50050866": 0.1,
            "50050997": None,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_signamt_txamt_entreport_prop(self):
        from scwgj.index_compute.indices.corp_signamt_txamt_entreport_prop import corp_signamt_txamt_entreport_prop
        assert corp_signamt_txamt_entreport_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A"],
            ["8290591", 10, 2, "121020", "2021-04-01", "B"],
            ["21712617", 10, 3, "121030", "2021-03-01", "C"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A"],
            ["50051842", None, 7, "120010", "2020-12-01", "A"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["21712617", 700, "2021-03-01", 1, "A", "121030"],
            ["21712617", 700, "2021-03-01", 1, "B", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 900, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "B", "121990"],
            ["50050997", 1100, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "B", "120010"],
            ["50051842", None, "2020-12-01", 1, "A", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_dfxloan_sign_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01"],
            ["8290591", 25, "2021-04-01"],
            ["21712617", 100, "2021-03-01"],
            ["50050276", 200, "2021-02-01"],
            ["50050348", 50, "2021-01-01"],
            ["50050866", 25, "2021-05-15"],
            ["50050997", 400, "2020-12-01"],
            ["50051180", None, "2020-12-01"],
            ["50051842", None, "2020-12-01"]
        ], schema=["corp_code", "sign_amt_usd", "interest_start_date"])
        Executor.register(name="pboc_dfxloan_sign_v", df_func=pboc_dfxloan_sign_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_signamt_txamt_entreport_prop")
        result_df = self.entry.spark.sql("""select * from corp_signamt_txamt_entreport_prop 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_signamt_txamt_entreport_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 0.02,
            "8290591": 0.04,
            "21712617": 14.0,
            "50050276": 0.005,
            "50050348": 0.02,
            "50050866": 0.04,
            "50050997": 0.0025,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_pay_inc_prop(self):
        from scwgj.index_compute.indices.corp_entreport_pay_inc_prop import corp_entreport_pay_inc_prop
        assert corp_entreport_pay_inc_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A"],
            ["8290591", 10, 2, "121020", "2021-04-01", "B"],
            ["21712617", 10, 3, "121030", "2021-03-01", "C"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A"],
            ["50051842", None, 7, "120010", "2020-12-01", "A"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["21712617", 200, "2021-03-01", 1, "A", "121030"],
            ["21712617", 200, "2021-03-01", 1, "B", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 900, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "B", "121990"],
            ["50050997", 1100, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "B", "120010"],
            ["50051842", None, "2020-12-01", 1, "A", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_pay_inc_prop")
        result_df = self.entry.spark.sql("""select * from corp_entreport_pay_inc_prop 
                                                            where corp_code in ('5393225', '8290591', 
                                                                                '21712617', '50050276', 
                                                                                '50050348', '50050866', 
                                                                                '50050997', '50051180',
                                                                                '50051842')
                                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_pay_inc_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": None,
            "8290591": None,
            "21712617": 0.025,
            "50050276": None,
            "50050348": None,
            "50050866": None,
            "50050997": None,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_rel_prepay_capinc_prop(self):
        from scwgj.index_compute.indices.corp_rel_prepay_capinc_prop import corp_rel_prepay_capinc_prop
        assert corp_rel_prepay_capinc_prop

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "622011", "2021-05-01", "A"],
            ["8290591", 10, 2, "622012", "2021-04-01", "B"],
            ["21712617", 10, 3, "622013", "2021-03-01", "C"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A"],
            ["50051842", None, 7, "120010", "2020-12-01", "A"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["21712617", 200, "2021-03-01", 1, "A", "121030"],
            ["21712617", 200, "2021-03-01", 1, "B", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 900, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "B", "121990"],
            ["50050997", 1100, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "B", "120010"],
            ["50051842", None, "2020-12-01", 1, "A", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        all_offline_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_offline_relations_v", df_func=all_offline_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_rel_prepay_capinc_prop")
        result_df = self.entry.spark.sql("""select * from corp_rel_prepay_capinc_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_rel_prepay_capinc_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 150,
            "8290591": 120.0,
            "21712617": 20.0,
            "50050276": None,
            "50050348": 900.0,
            "50050866": None,
            "50050997": 1100.0,
            "50051180": None,
            "50051842": None
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_pattern_abnormal(self):
        from scwgj.index_compute.indices.corp_pattern_abnormal import corp_pattern_abnormal
        assert corp_pattern_abnormal

        # step1: giving the test data
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C", "T"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code", "settle_method_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_dfxloan_sign_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "12"],
            ["8290591", 25, "2021-04-01", "13"],
            ["21712617", 100, "2021-03-01", "14"],
            ["50050276", 200, "2021-02-01", "15"],
            ["50050348", 50, "2021-01-01", "16"],
            ["50050866", 25, "2021-05-15", "17"],
            ["50050997", 400, "2020-12-01", "18"],
            ["50051180", None, "2020-12-01", "19"],
            ["50051842", None, "2020-12-01", "20"]
        ], schema=["corp_code", "sign_amt_usd", "interest_start_date", "dfxloan_no"])
        Executor.register(name="pboc_dfxloan_sign_v", df_func=pboc_dfxloan_sign_v, dependencies=[])
        pboc_exp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01"],
            ["8290591", 25, "2021-04-01"],
            ["21712617", 100, "2021-03-01"],
            ["50050276", 200, "2021-02-01"],
            ["50050348", 50, "2021-01-01"],
            ["50050866", 25, "2021-05-15"],
            ["50050997", 400, "2020-12-01"],
            ["50051180", None, "2020-12-01"],
            ["50051842", None, "2020-12-01"]
        ], schema=["corp_code", "deal_amt_usd", "exp_date"])
        Executor.register(name="pboc_exp_custom_rpt_v", df_func=pboc_exp_custom_rpt_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01"],
            ["8290591", 25, "2021-04-01"],
            ["21712617", 100, "2021-03-01"],
            ["50050276", 200, "2021-02-01"],
            ["50050348", 50, "2021-01-01"],
            ["50050866", 25, "2021-05-15"],
            ["50050997", 400, "2020-12-01"],
            ["50051180", None, "2020-12-01"],
            ["50051842", None, "2020-12-01"]
        ], schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        # step2: executing the target function and retrieving the result
        Executor.execute(self.entry, param, self.entry.logger, "corp_pattern_abnormal")
        result_df = self.entry.spark.sql("""select * from corp_pattern_abnormal 
                                                                    where corp_code in ('5393225', '8290591', 
                                                                                        '21712617', '50050276', 
                                                                                        '50050348', '50050866', 
                                                                                        '50050997', '50051180',
                                                                                        '50051842')
                                                                 """)
        result_rows_list = result_df.collect()

        # step3: retrieving the actual result and defining the expected data
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_pattern_abnormal"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 0,
            "8290591": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50050997": 0,
            "50051180": 0,
            "50051842": 0
        }

        # step4: assert the expected_data and actual_data
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_prepay_capinc_prop(self):
        from scwgj.index_compute.indices.corp_prepay_capinc_prop import corp_prepay_capinc_prop
        assert corp_prepay_capinc_prop

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["8290591", 600, "2021-04-01", 1, "A", "121020"],
            ["21712617", 200, "2021-03-01", 1, "A", "121030"],
            ["21712617", 200, "2021-03-01", 1, "B", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 900, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "B", "121990"],
            ["50050997", 1100, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "B", "120010"],
            ["50051842", None, "2020-12-01", 1, "A", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C", "T"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_prepay_capinc_prop")
        result_df = self.entry.spark.sql("""select * from corp_prepay_capinc_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_prepay_capinc_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 1500.0,
            "8290591": 1200.0,
            "21712617": 200.0,
            "50050276": None,
            "50050348": 900.0,
            "50050866": None,
            "50050997": 1100.0,
            "50051180": None,
            "50051842": None
        }

        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_yf_rptamt_prop(self):
        from scwgj.index_compute.indices.yf_rptamt_prop import yf_rptamt_prop
        assert yf_rptamt_prop

        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-05-01"],
            ["8290591", 10, "2021-04-01"],
            ["21712617", 10, "2021-03-01"],
            ["50050276", 10, "2021-02-01"],
            ["50050348", 10, "2021-01-01"],
            ["50050997", 10, "2020-12-01"],
            ["50051180", 10, "2020-12-01"],
            ["50051842", None, "2020-12-01"]],
            schema=["corp_code", "deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        pboc_adv_pay_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-05-01"],
            ["8290591", 10, "2021-04-01"],
            ["21712617", 10, "2021-03-01"],
            ["50050276", 10, "2021-02-01"],
            ["50050348", 10, "2021-01-01"],
            ["50050997", 10, "2020-12-01"],
            ["50051180", 10, "2020-12-01"],
            ["50051842", None, "2020-12-01"]],
            schema=["corp_code", "rpt_amt_usd", "expt_imp_date"])
        Executor.register(name="pboc_adv_pay_v", df_func=pboc_adv_pay_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "P", "121010"],
            ["5393225", 500, "2021-05-01", 1, "P", "121010"],
            ["5393225", 500, "2021-05-01", 1, "P", "121010"],
            ["8290591", 200, "2021-04-01", 1, "P", "121020"],
            ["8290591", 200, "2021-04-01", 1, "O", "121020"],
            ["21712617", 400, "2021-03-01", 1, "O", "121030"],
            ["21712617", 400, "2021-03-01", 1, "O", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "P", "121990"],
            ["50050997", 2000, "2020-12-01", 1, "O", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "120010"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_delay_pay_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, "2021-05-01"],
            ["8290591", 10, "2021-04-01"],
            ["21712617", 10, "2021-03-01"],
            ["50050276", 10, "2021-02-01"],
            ["50050348", 10, "2021-01-01"],
            ["50050997", 10, "2020-12-01"],
            ["50051180", 10, "2020-12-01"],
            ["50051842", None, "2020-12-01"]],
            schema=["corp_code", "rpt_amt_usd", "expt_pay_date"])
        Executor.register(name="pboc_delay_pay_v", df_func=pboc_delay_pay_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "yf_rptamt_prop")
        result_df = self.entry.spark.sql("""select * from yf_rptamt_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["yf_rptamt_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": -0.006666666666666667,
            "8290591": -0.025,
            "21712617": -0.0125,
            "50050276": 10.0,
            "50050348": 10.0,
            "50050866": -0.001,
            "50050997": -0.005,
            "50051180": 10.0,
            "50051842": None
        }

        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_settle_prepay_prop(self):
        from scwgj.index_compute.indices.corp_settle_prepay_prop import corp_settle_prepay_prop
        assert corp_settle_prepay_prop

        pboc_corp_lcy_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, '2021-05-04', "622011"],
            ["8290591", 1000, '2021-05-04', "622012"],
            ["21712617", 3003, '2021-05-04', "622013"],
            ["50050276", 4004, '2020-05-04', "622014"]],
            schema=["corp_code", "salefx_amt_usd", "deal_date", "tx_code"])
        Executor.register(name=f"pboc_corp_lcy_v", df_func=pboc_corp_lcy_df, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "B", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["21712617", 400, "2021-03-01", 1, "B", "121030"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "A", "121990"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "120010"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "622011", "2021-05-01", "A", "T"],
            ["8290591", 10, 2, "622012", "2021-04-01", "A", "T"],
            ["21712617", 10, 3, "622013", "2021-03-01", "A", "T"],
            ["50050276", 10, 4, "622014", "2021-02-01", "A", "T"],
            ["50050348", 10, 5, "622015", "2021-01-01", "A", "S"],
            ["50050866", 10, 6, "622016", "2021-05-15", "B", "T"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C", "T"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_settle_prepay_prop")
        result_df = self.entry.spark.sql("""select * from corp_settle_prepay_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_settle_prepay_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 0.006666666666666667,
            "8290591": 0.0,
            "21712617": 0.0,
            "50050276": None,
            "50050348": None,
            "50050866": 0.0,
            "50050997": None,
            "50051180": 0.0,
            "50051842": None
        }

        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_multi_income(self):
        from scwgj.index_compute.indices.corp_multi_income import corp_multi_income
        assert corp_multi_income

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "622011", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "622012", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "622013", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "622014", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "622015", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "622016", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "82220", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_multi_income")
        result_df = self.entry.spark.sql("""select * from corp_multi_income 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_multi_income"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50050997', '50051180',
                                   '50051842']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50050997": 1,
            "50051180": 1,
            "50051842": 1
        }

        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_basic_regcap(self):
        from scwgj.index_compute.indices.basic_regcap import basic_regcap
        assert basic_regcap

        qyxx_basic_v = self.entry.spark.createDataFrame(data=[
            ["3c1bad94869f4bfeb4cd4507e4f192e7", "成都商厦太平洋百货有限公司", "1000"],
            ["34badb6a0c5f4ea7ba4bd9e7eb36b1f1", "自贡市邮政局", "1000"],
            ["bb9b2c4f3a514c0e96c4f82ade780bde", "四川龙城保龄球俱乐部有限公司", "2000"],
            ["264dd82001484255b43e0942a9d24b1d", "成都中电建海赋房地产开发有限公司", "3000"],
            ["a90aeb5f7ee74bbfba30d5899c44e783", "成都市盈富教育咨询有限公司", "4000"],
            ["80d45a71d41c4b25addda2fe2818f3a8", "成都华美国际旅行社有限公司", "5000"],
        ], schema=["bbd_qyxx_id", "company_name", "regcap_amount"])
        Executor.register(name="qyxx_basic_v", df_func=qyxx_basic_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "basic_regcap")
        result_df = self.entry.spark.sql("""select * from basic_regcap 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["basic_regcap"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866']
        expected_corp_code_indexes = {
            "5393225": "1000",
            "8290591": "1000",
            "21712617": "2000",
            "50050276": "3000",
            "50050348": "4000",
            "50050866": "5000"
        }

        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_basic_esage(self):
        from scwgj.index_compute.indices.basic_esage import basic_esage
        assert basic_esage

        qyxx_basic_v = self.entry.spark.createDataFrame(data=[
            ["3c1bad94869f4bfeb4cd4507e4f192e7", "成都商厦太平洋百货有限公司", "1000", "2020-06-01"],
            ["34badb6a0c5f4ea7ba4bd9e7eb36b1f1", "自贡市邮政局", "1000", "2010-06-01"],
            ["bb9b2c4f3a514c0e96c4f82ade780bde", "四川龙城保龄球俱乐部有限公司", "2000", "2012-05-01"],
            ["264dd82001484255b43e0942a9d24b1d", "成都中电建海赋房地产开发有限公司", "3000", "2013-05-01"],
            ["a90aeb5f7ee74bbfba30d5899c44e783", "成都市盈富教育咨询有限公司", "4000", "2014-07-01"],
            ["80d45a71d41c4b25addda2fe2818f3a8", "成都华美国际旅行社有限公司", "5000", "2016-07-01"],
        ], schema=["bbd_qyxx_id", "company_name", "regcap_amount", "esdate"])
        Executor.register(name="qyxx_basic_v", df_func=qyxx_basic_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        def override_current_date():
            return "2021-06-01"
        # 覆盖spark中的自有current_date()函数
        self.entry.spark.udf.register("current_date", override_current_date)

        Executor.execute(self.entry, param, self.entry.logger, "basic_esage")
        result_df = self.entry.spark.sql("""select * from basic_esage 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["basic_esage"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 11,
            "21712617": 9,
            "50050276": 8,
            "50050348": 6,
            "50050866": 4
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_export_ct_varicoef(self):
        from scwgj.index_compute.indices.export_ct_varicoef import export_ct_varicoef
        assert export_ct_varicoef

        pboc_exp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "123"],
            ["8290591", 25, "2021-04-01", "124"],
            ["21712617", 100, "2021-03-01", "125"],
            ["50050276", 200, "2021-02-01", "126"],
            ["50050348", 50, "2021-01-01", "23"],
            ["50050866", 25, "2021-05-15", "34"],
            ["50050997", 400, "2020-12-01", "4444"],
            ["50051180", None, "2020-12-01", "4444"],
            ["50051842", None, "2020-12-01", "121"]
        ], schema=["corp_code", "deal_amt_usd", "exp_date", "custom_rpt_no"])
        Executor.register(name="pboc_exp_custom_rpt_v", df_func=pboc_exp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "export_ct_varicoef")
        result_df = self.entry.spark.sql("""select * from export_ct_varicoef 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["export_ct_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866']
        expected_corp_code_indexes = {
            "5393225": 0.0,
            "8290591": 0.0,
            "21712617": 0.0,
            "50050276": 0.0,
            "50050348": 0.0,
            "50050866": 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_inc_ct_varicoef(self):
        from scwgj.index_compute.indices.corp_inc_ct_varicoef import corp_inc_ct_varicoef
        assert corp_inc_ct_varicoef

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_inc_ct_varicoef")
        result_df = self.entry.spark.sql("""select * from corp_inc_ct_varicoef 
                                                            where corp_code in ('5393225', '8290591', 
                                                                                '21712617', '50050276', 
                                                                                '50050348', '50050866', 
                                                                                '50050997', '50051180',
                                                                                '50051842')
                                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_inc_ct_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', "50051842", "50051180", "50050997"]
        expected_corp_code_indexes = {
            "5393225": 0.0,
            "8290591": 0.0,
            "21712617": 0.0,
            "50050276": 0.0,
            "50050348": 0.0,
            "50050866": 0.0,
            "50051842":None,
            "50051180": None,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_hk_conc_income(self):
        from scwgj.index_compute.indices.corp_hk_conc_income import corp_hk_conc_income
        assert corp_hk_conc_income

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "HKG"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "MAC"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "SGP"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "HKG"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "MAC"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "SGP"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "HKG"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "HKG"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "HKG"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_hk_conc_income")
        result_df = self.entry.spark.sql("""select * from corp_hk_conc_income 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_hk_conc_income"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', "50051842", "50051180", "50050997"]
        expected_corp_code_indexes = {
            "5393225": 0,
            "8290591": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 0,
            "50051180": 0,
            "50050997": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_yf_prepay_ct_varicoef(self):
        from scwgj.index_compute.indices.yf_prepay_ct_varicoef import yf_prepay_ct_varicoef
        assert yf_prepay_ct_varicoef

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "B", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["21712617", 400, "2021-03-01", 1, "B", "121030"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "A", "121990"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "120010"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "yf_prepay_ct_varicoef")
        result_df = self.entry.spark.sql("""select * from yf_prepay_ct_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["yf_prepay_ct_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866','50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0.0,
            "8290591": 0.0,
            "21712617": None,
            "50050276": None,
            "50050348": 0.0,
            "50050866": 0.0,
            "50051842": None,
            "50051180": None,
            "50050997": 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_yf_prepay_amt_varicoef(self):
        from scwgj.index_compute.indices.yf_prepay_amt_varicoef import yf_prepay_amt_varicoef
        assert yf_prepay_amt_varicoef

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "B", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["21712617", 400, "2021-03-01", 1, "B", "121030"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "A", "121990"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "120010"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "yf_prepay_amt_varicoef")
        result_df = self.entry.spark.sql("""select * from yf_prepay_amt_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["yf_prepay_amt_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0.0,
            "8290591": 0.0,
            "21712617": None,
            "50050276": None,
            "50050348": 0.0,
            "50050866": 0.0,
            "50051842": None,
            "50051180": None,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_ys_preinc_ct_varicoef(self):
        from scwgj.index_compute.indices.ys_preinc_ct_varicoef import ys_preinc_ct_varicoef
        assert ys_preinc_ct_varicoef

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "ys_preinc_ct_varicoef")
        result_df = self.entry.spark.sql("""select * from ys_preinc_ct_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["ys_preinc_ct_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0.0,
            "8290591": 0.0,
            "21712617": 0.0,
            "50050276": 0.0,
            "50050348": 0.0,
            "50050866": None,
            "50051842": 0.0,
            "50051180": 0.0,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_ys_preinc_amt_varicoef(self):
        from scwgj.index_compute.indices.ys_preinc_amt_varicoef import ys_preinc_amt_varicoef
        assert ys_preinc_amt_varicoef

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "ys_preinc_amt_varicoef")
        result_df = self.entry.spark.sql("""select * from ys_preinc_amt_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["ys_preinc_amt_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0.0,
            "8290591": 0.0,
            "21712617": 0.0,
            "50050276": 0.0,
            "50050348": 0.0,
            "50050866": None,
            "50051842": None,
            "50051180": None,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_exp_amt_varicoef(self):
        from scwgj.index_compute.indices.corp_entreport_exp_amt_varicoef import corp_entreport_exp_amt_varicoef
        assert corp_entreport_exp_amt_varicoef

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "B", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["21712617", 400, "2021-03-01", 1, "B", "121030"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "A", "121990"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "120010"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_exp_amt_varicoef")
        result_df = self.entry.spark.sql("""select * from corp_entreport_exp_amt_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_exp_amt_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": None,
            "8290591": None,
            "21712617": 0.0,
            "50050276": None,
            "50050348": None,
            "50050866": None,
            "50051842": None,
            "50051180": None,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_inc_amt_varicoef(self):
        from scwgj.index_compute.indices.corp_entreport_inc_amt_varicoef import corp_entreport_inc_amt_varicoef
        assert corp_entreport_inc_amt_varicoef

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_inc_amt_varicoef")
        result_df = self.entry.spark.sql("""select * from corp_entreport_inc_amt_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_inc_amt_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": None,
            "8290591": None,
            "21712617": None,
            "50050276": None,
            "50050348": None,
            "50050866": None,
            "50051842": None,
            "50051180": None,
            "50050997": 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_expinc_varicoef(self):
        from scwgj.index_compute.indices.corp_entreport_expinc_varicoef import corp_entreport_expinc_varicoef
        assert corp_entreport_expinc_varicoef

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["5393225", 500, "2021-05-01", 1, "B", "121010"],
            ["5393225", 500, "2021-05-01", 1, "A", "121010"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["21712617", 400, "2021-03-01", 1, "B", "121030"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "121080"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "A", "121990"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "120010"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_expinc_varicoef")
        result_df = self.entry.spark.sql("""select * from corp_entreport_expinc_varicoef 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_expinc_varicoef"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": None,
            "8290591": None,
            "21712617": 0.0,
            "50050276": None,
            "50050348": None,
            "50050866": None,
            "50051842": None,
            "50051180": None,
            "50050997": 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_exp_cp_rel_prop(self):
        from scwgj.index_compute.indices.rmb_exp_cp_rel_prop import rmb_exp_cp_rel_prop
        assert rmb_exp_cp_rel_prop

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010"],
            ["50051180", None, "2020-12-01", 1, "P", "929070"],
            ["50051842", None, "2020-12-01", 1, "P", "120010"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_exp_cp_rel_prop")
        result_df = self.entry.spark.sql("""select * from rmb_exp_cp_rel_prop 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_exp_cp_rel_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1.0,
            "8290591": 1.0,
            "21712617": 1.0,
            "50050276": 1.0,
            "50050348": None,
            "50050866": 1.0,
            "50051842": None,
            "50051180": None,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_preinc_same_foreign_cp(self):
        from scwgj.index_compute.indices.corp_preinc_same_foreign_cp import corp_preinc_same_foreign_cp
        assert corp_preinc_same_foreign_cp

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_preinc_same_foreign_cp")
        result_df = self.entry.spark.sql("""select * from corp_preinc_same_foreign_cp 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_preinc_same_foreign_cp"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0,
            "8290591": 1,
            "21712617": 1,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 1,
            "50051180": 1,
            "50050997": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_prepay_same_foreign_cp(self):
        from scwgj.index_compute.indices.corp_prepay_same_foreign_cp import corp_prepay_same_foreign_cp
        assert corp_prepay_same_foreign_cp

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "zhangsan"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "lisi"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "lisi"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "wangwu"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "wangwu"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "zhaoliu"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "qianqi"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "zhangsan"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "lisi"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "wangwu"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "zhaoliu"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_prepay_same_foreign_cp")
        result_df = self.entry.spark.sql("""select * from corp_prepay_same_foreign_cp 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_prepay_same_foreign_cp"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 1,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 1,
            "50051842": 0,
            "50051180": 0,
            "50050997": 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_exp_same_cp(self):
        from scwgj.index_compute.indices.rmb_exp_same_cp import rmb_exp_same_cp
        assert rmb_exp_same_cp

        pboc_cncc_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司", "zhangsan"],
            ["四川龙城保龄球俱乐部有限公司", "zhangsan"],
            ["成都中电建海赋房地产开发有限公司", "zhangsan"],
            ["成都市盈富教育咨询有限公司", "lisi"],
            ["成都华美国际旅行社有限公司", "wangwu"],
            ["绿地集团成都蜀峰房地产开发有限公司", "wangwu"],
            ["成都希彤进出口贸易有限公司", "zhaoliu"],
            ["四川格蓝环保工程有限公司", "zhaoliu"],
            ["成都玖锦科技有限公司", "zhaoliu"],
            ["成都索成易半导体有限公司", "zhaoliu"],
            ["成都浣花黉台酒店有限公司", "zhaoliu"],
            ["上海亚致力物流有限公司成都分公司", "qianqi"],
            ["成都市刘氏红杉贸易有限公司", "qianqi"],
            ["成都芯盟微科技有限公司", "zhangsan"],
        ], schema=["DEBTORNAME", "CRDTORNAME"])
        Executor.register(name="pboc_cncc_v", df_func=pboc_cncc_v,dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_exp_same_cp")
        result_df = self.entry.spark.sql("""select * from rmb_exp_same_cp 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_exp_same_cp"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50051842": 1,
            "50051180": 1,
            "50050997": 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_multi_location(self):
        from scwgj.index_compute.indices.corp_entreport_multi_location import corp_entreport_multi_location
        assert corp_entreport_multi_location

        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C", "1234567"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C", "2345678"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J", "3456789"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A", "4567890"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K", "5678901"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C", "6789012"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C", "7890123"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C", "8901234"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J", "9012345"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code", "bank_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_multi_location")
        result_df = self.entry.spark.sql("""select * from corp_entreport_multi_location 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_multi_location"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['50050997']
        expected_corp_code_indexes = {
            "50050997": 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_exp_proper_use(self):
        from scwgj.index_compute.indices.corp_exp_proper_use import corp_exp_proper_use
        assert corp_exp_proper_use

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_exp_proper_use")
        result_df = self.entry.spark.sql("""select * from corp_exp_proper_use 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_exp_proper_use"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0,
            "8290591": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 0,
            "50051180": 0,
            "50050997": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_inc_ct(self):
        from scwgj.index_compute.indices.rmb_inc_ct import rmb_inc_ct
        assert rmb_inc_ct

        pboc_cncc_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司", "成都商厦太平洋百货有限公司"],
            ["四川龙城保龄球俱乐部有限公司", "四川龙城保龄球俱乐部有限公司"],
            ["成都中电建海赋房地产开发有限公司", "成都中电建海赋房地产开发有限公司"],
            ["成都市盈富教育咨询有限公司", "成都市盈富教育咨询有限公司"],
            ["成都华美国际旅行社有限公司", "成都华美国际旅行社有限公司"],
            ["绿地集团成都蜀峰房地产开发有限公司", "绿地集团成都蜀峰房地产开发有限公司"],
            ["成都希彤进出口贸易有限公司", "成都希彤进出口贸易有限公司"],
            ["四川格蓝环保工程有限公司", "四川格蓝环保工程有限公司"],
            ["成都玖锦科技有限公司", "成都玖锦科技有限公司"],
            ["成都索成易半导体有限公司", "成都索成易半导体有限公司"],
            ["成都浣花黉台酒店有限公司", "成都浣花黉台酒店有限公司"],
            ["上海亚致力物流有限公司成都分公司", "上海亚致力物流有限公司成都分公司"],
            ["成都市刘氏红杉贸易有限公司", "成都市刘氏红杉贸易有限公司"],
            ["成都芯盟微科技有限公司", "成都芯盟微科技有限公司"],
        ], schema=["DEBTORNAME", "CRDTORNAME"])
        Executor.register(name="pboc_cncc_v", df_func=pboc_cncc_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "rmb_inc_ct")
        result_df = self.entry.spark.sql("""select * from rmb_inc_ct 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_inc_ct"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50051842": 1,
            "50051180": 1,
            "50050997": 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_black_rel_prop(self):
        from scwgj.index_compute.indices.black_rel_prop import black_rel_prop
        assert black_rel_prop

        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        negative_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="negative_list_v", df_func=negative_list_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "black_rel_prop")
        result_df = self.entry.spark.sql("""select * from black_rel_prop 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["black_rel_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1.0,
            "8290591": 1.0,
            "21712617": 1.0,
            "50050276": 1.0,
            "50050348": 1.0,
            "50050866": 1.0,
            "50051842": 1.0,
            "50051180": 1.0,
            "50050997": None
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_black_isneg(self):
        from scwgj.index_compute.indices.black_isneg import black_isneg
        assert black_isneg

        negative_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="negative_list_v", df_func=negative_list_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "black_isneg")
        result_df = self.entry.spark.sql("""select * from black_isneg 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["black_isneg"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50051842": 1,
            "50051180": 1,
            "50050997": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_black_suspect_taxfraud_rel(self):
        from scwgj.index_compute.indices.black_suspect_taxfraud_rel import black_suspect_taxfraud_rel
        assert black_suspect_taxfraud_rel

        negative_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="negative_list_v", df_func=negative_list_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C", "1234567"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C", "2345678"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J", "3456789"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A", "4567890"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K", "5678901"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C", "6789012"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C", "7890123"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C", "8901234"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J", "9012345"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code", "bank_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        black_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225"],
            ["8290591"],
            ["21712617"],
            ["50050276"],
            ["50050348"],
            ["50050866"],
            ["50051180"],
            ["50051842"],
            ["50052714"]
        ], schema=["corp_code"])
        Executor.register(name="black_list_v", df_func=black_list_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })

        Executor.execute(self.entry, param, self.entry.logger, "black_suspect_taxfraud_rel")
        result_df = self.entry.spark.sql("""select * from black_suspect_taxfraud_rel 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["black_suspect_taxfraud_rel"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50051842": 1,
            "50051180": 1,
            "50050997": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_black_isneg_rel(self):
        from scwgj.index_compute.indices.black_isneg_rel import black_isneg_rel
        assert black_isneg_rel

        negative_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="negative_list_v", df_func=negative_list_v, dependencies=[])
        black_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225"],
            ["8290591"],
            ["21712617"],
            ["50050276"],
            ["50050348"],
            ["50050866"],
            ["50051180"],
            ["50051842"],
            ["50052714"]
        ], schema=["corp_code"])
        Executor.register(name="black_list_v", df_func=black_list_v, dependencies=[])
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '121010', '金融1'],
            ["8290591", 2000, '2021-05-04', '121020', '证券1'],
            ["21712617", 3003, '2021-05-04', '121040', '电子1'],
            ["50050276", 4004, '2020-05-04', '121080', '金融1']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code", "cp_payer_name"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "black_isneg_rel")
        result_df = self.entry.spark.sql("""select * from black_isneg_rel 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["black_isneg_rel"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "8290591": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50051842": 1,
            "50051180": 1,
            "50050997":1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_inc_person_ct(self):
        from scwgj.index_compute.indices.rmb_inc_person_ct import rmb_inc_person_ct
        assert rmb_inc_person_ct

        pboc_cncc_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司", "CHN"],
            ["四川龙城保龄球俱乐部有限公司", "CHN"],
            ["成都中电建海赋房地产开发有限公司", "CHN"],
            ["成都市盈富教育咨询有限公司", "CHN"],
            ["成都华美国际旅行社有限公司", "USA"],
            ["绿地集团成都蜀峰房地产开发有限公司", "CHN"],
            ["成都希彤进出口贸易有限公司", "USA"],
            ["四川格蓝环保工程有限公司", "CHN"],
            ["成都玖锦科技有限公司", "ZW"],
            ["成都索成易半导体有限公司", "ENGLAND"],
            ["成都浣花黉台酒店有限公司", "CHN"],
            ["上海亚致力物流有限公司成都分公司", "ZW"],
            ["成都市刘氏红杉贸易有限公司", "FRANCE"],
            ["成都芯盟微科技有限公司", "ZW"],
        ], schema=["CRDTORNAME","DEBTORNAME"])
        Executor.register(name="pboc_cncc_v", df_func=pboc_cncc_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_inc_person_ct")
        result_df = self.entry.spark.sql("""select * from rmb_inc_person_ct 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_inc_person_ct"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 1,
            "21712617": 1,
            "50050276": 1,
            "50050348": 1,
            "50050866": 1,
            "50051842": 1,
            "50051180": 1,
            "50050997": 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_exp_person_ct(self):
        from scwgj.index_compute.indices.rmb_exp_person_ct import rmb_exp_person_ct
        assert rmb_exp_person_ct

        pboc_cncc_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司", "CHN"],
            ["四川龙城保龄球俱乐部有限公司", "CHN"],
            ["成都中电建海赋房地产开发有限公司", "CHN"],
            ["成都市盈富教育咨询有限公司", "CHN"],
            ["成都华美国际旅行社有限公司", "USA"],
            ["绿地集团成都蜀峰房地产开发有限公司", "CHN"],
            ["成都希彤进出口贸易有限公司", "USA"],
            ["四川格蓝环保工程有限公司", "CHN"],
            ["成都玖锦科技有限公司", "ZW"],
            ["成都索成易半导体有限公司", "ENGLAND"],
            ["成都浣花黉台酒店有限公司", "CHN"],
            ["上海亚致力物流有限公司成都分公司", "ZW"],
            ["成都市刘氏红杉贸易有限公司", "FRANCE"],
            ["成都芯盟微科技有限公司", "ZW"],
        ], schema=["DEBTORNAME", "CRDTORNAME"])
        Executor.register(name="pboc_cncc_v", df_func=pboc_cncc_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_exp_person_ct")
        result_df = self.entry.spark.sql("""select * from rmb_exp_person_ct 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_exp_person_ct"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051180': 1,
            '5393225': 1,
            '8290591': 1,
            '50050866': 1,
            '21712617': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_same_payrcv_cp(self):
        from scwgj.index_compute.indices.corp_same_payrcv_cp import corp_same_payrcv_cp
        assert corp_same_payrcv_cp

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 10, 1, "121010", "2021-05-01", "A", "T", "zhangsan", "1", "A", "C", "1234567"],
            ["8290591", 10, 2, "121020", "2021-04-01", "A", "T", "lisi", "2", "B", "C", "2345678"],
            ["21712617", 10, 3, "121040", "2021-03-01", "A", "T", "wangwu", "3", "C", "J", "3456789"],
            ["50050276", 10, 4, "121080", "2021-02-01", "A", "T", "zhaoliu", "4", "D", "A", "4567890"],
            ["50050348", 10, 5, "121090", "2021-01-01", "A", "S", "qianqi", "5", "E", "K", "5678901"],
            ["50050866", 10, 6, "121990", "2021-05-15", "B", "T", "zhangsan", "1", "A", "C", "6789012"],
            ["50050997", 10, 7, "121030", "2020-12-01", "C", "T", "zhangsan", "1", "A", "C", "7890123"],
            ["50051180", None, 7, "120010", "2020-12-01", "A", "S", "lisi", "2", "B", "C", "8901234"],
            ["50051842", None, 7, "120010", "2020-12-01", "A", "T", "wangwu", "3", "C", "J", "9012345"]],
            schema=["corp_code", "tx_amt_usd", "rptno",
                    "tx_code", "rcv_date", "rcvpay_attr_code",
                    "settle_method_code", "cp_payer_name", "safecode",
                    "indu_attr_code", "payer_country_code", "bank_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_same_payrcv_cp")
        result_df = self.entry.spark.sql("""select * from corp_same_payrcv_cp 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_same_payrcv_cp"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 0,
            "50051180": 0,
            "50050997": 0,
            "8290591": 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_prepay_capinc_same_cp(self):
        from scwgj.index_compute.indices.corp_prepay_capinc_same_cp import corp_prepay_capinc_same_cp
        assert corp_prepay_capinc_same_cp

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "A", "金融1"],
            ["5393225", "2021-05-01", "B", "证券1"],
            ["5393225", "2021-05-01", "A", "证券1"],
            ["8290591", "2021-04-01", "A", "证券1"],
            ["8290591", "2021-04-01", "A", "证券1"],
            ["21712617", "2021-03-01", "B",  "金融1"],
            ["21712617", "2021-03-01", "C", "证券1"],
            ["50050276", "2021-02-01", "B", "金融1"],
            ["50050348", "2021-01-01", "A", "证券1"],
            ["50050866", "2021-05-15", "A", "金融1"],
            ["50050997", "2020-12-01", "A", "电子1"],
            ["50051180", "2020-12-01", "P", "金融1"],
            ["50051842", "2020-12-01", "P", "金融1"]],
            schema=["corp_code", "pay_date", "rcvpay_attr_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "121010", "2021-05-01", "zhangsan"],
            ["8290591", "121020", "2021-04-01", "lisi"],
            ["21712617", "121040", "2021-03-01", "wangwu"],
            ["50050276", "121080", "2021-02-01",  "zhaoliu"],
            ["50050348", "121090", "2021-01-01", "qianqi"],
            ["50050866", "121990", "2021-05-15", "zhangsan"],
            ["50050997", "121030", "2020-12-01", "zhangsan"],
            ["50051180", "120010", "2020-12-01", "lisi"],
            ["50051842", "120010", "2020-12-01", "wangwu"]],
            schema=["corp_code", "tx_code", "rcv_date", "cp_payer_name",])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_prepay_capinc_same_cp")
        result_df = self.entry.spark.sql("""select * from corp_prepay_capinc_same_cp 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_prepay_capinc_same_cp"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 0,
            "50051180": 0,
            "50050997": 0,
            "8290591": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_cp_rel_consistent(self):
        from scwgj.index_compute.indices.corp_cp_rel_consistent import corp_cp_rel_consistent
        assert corp_cp_rel_consistent

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "A", "金融1"],
            ["5393225", "2021-05-01", "B", "证券1"],
            ["5393225", "2021-05-01", "A", "证券1"],
            ["8290591", "2021-04-01", "A", "证券1"],
            ["8290591", "2021-04-01", "A", "证券1"],
            ["21712617", "2021-03-01", "B", "金融1"],
            ["21712617", "2021-03-01", "C", "证券1"],
            ["50050276", "2021-02-01", "B", "金融1"],
            ["50050348", "2021-01-01", "A", "证券1"],
            ["50050866", "2021-05-15", "A", "金融1"],
            ["50050997", "2020-12-01", "A", "电子1"],
            ["50051180", "2020-12-01", "P", "金融1"],
            ["50051842", "2020-12-01", "P", "金融1"]],
            schema=["corp_code", "pay_date", "rcvpay_attr_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "121010", "2021-05-01", "zhangsan"],
            ["8290591", "121020", "2021-04-01", "lisi"],
            ["21712617", "121040", "2021-03-01", "wangwu"],
            ["50050276", "121080", "2021-02-01", "zhaoliu"],
            ["50050348", "121090", "2021-01-01", "qianqi"],
            ["50050866", "121990", "2021-05-15", "zhangsan"],
            ["50050997", "121030", "2020-12-01", "zhangsan"],
            ["50051180", "120010", "2020-12-01", "lisi"],
            ["50051842", "120010", "2020-12-01", "wangwu"]],
            schema=["corp_code", "tx_code", "rcv_date", "cp_payer_name", ])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_cp_rel_consistent")
        result_df = self.entry.spark.sql("""select * from corp_cp_rel_consistent 
                                                    where corp_code in ('5393225', '8290591', 
                                                                        '21712617', '50050276', 
                                                                        '50050348', '50050866', 
                                                                        '50050997', '50051180',
                                                                        '50051842')
                                                 """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_cp_rel_consistent"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 0,
            "50051180": 0,
            "50050997": 0,
            "8290591": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_cp_guar_consistent(self):
        from scwgj.index_compute.indices.corp_cp_guar_consistent import corp_cp_guar_consistent
        assert corp_cp_guar_consistent

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "A", "金融1"],
            ["5393225", "2021-05-01", "B", "证券1"],
            ["5393225", "2021-05-01", "A", "证券1"],
            ["8290591", "2021-04-01", "A", "证券1"],
            ["8290591", "2021-04-01", "A", "证券1"],
            ["21712617", "2021-03-01", "B", "金融1"],
            ["21712617", "2021-03-01", "C", "证券1"],
            ["50050276", "2021-02-01", "B", "金融1"],
            ["50050348", "2021-01-01", "A", "证券1"],
            ["50050866", "2021-05-15", "A", "金融1"],
            ["50050997", "2020-12-01", "A", "电子1"],
            ["50051180", "2020-12-01", "P", "金融1"],
            ["50051842", "2020-12-01", "P", "金融1"]],
            schema=["corp_code", "pay_date", "rcvpay_attr_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_con_exguaran_new_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "金融1", "2021-05-01"],
            ["8290591", "证券1", "2021-05-01"],
            ["21712617", "金融1", "2021-05-01"],
            ["50050276", "金融1", "2021-05-01"],
            ["50050348", "金融1", "2021-05-01"],
            ["50050866", "证券1", "2021-05-01"],
            ["50050997", "电子1", "2021-05-01"],
            ["50051180", "金融1", "2021-05-01"],
            ["50051842", "金融1", "2021-05-01"],
        ], schema=["corp_code", "guednameen0", "contractdate"])
        Executor.register(name="pboc_con_exguaran_new_v", df_func=pboc_con_exguaran_new_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_cp_guar_consistent")
        result_df = self.entry.spark.sql("""select * from corp_cp_guar_consistent 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_cp_guar_consistent"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            "5393225": 0,
            "21712617": 0,
            "50050276": 0,
            "50050348": 0,
            "50050866": 0,
            "50051842": 0,
            "50051180": 0,
            "50050997": 0,
            "8290591": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_exp_cp_ct(self):
        from scwgj.index_compute.indices.corp_entreport_exp_cp_ct import corp_entreport_exp_cp_ct
        assert corp_entreport_exp_cp_ct

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "122010", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "121030", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "121030", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "122010", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "122010", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "122010", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "121030", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_exp_cp_ct")
        result_df = self.entry.spark.sql("""select * from corp_entreport_exp_cp_ct 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276', 
                                                                '50050348', '50050866', 
                                                                '50050997', '50051180',
                                                                '50051842')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_exp_cp_ct"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['50051180', '5393225', '8290591', '50050866', '21712617']
        expected_corp_code_indexes = {
            '50051180': 1,
            '5393225': 1,
            '8290591': 1,
            '50050866': 1,
            '21712617': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_capinc_multi_domcp(self):
        from scwgj.index_compute.indices.corp_capinc_multi_domcp import corp_capinc_multi_domcp
        assert corp_capinc_multi_domcp

        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '121010', '金融1'],
            ["8290591", 2000, '2021-05-04', '121020', '证券1'],
            ["21712617", 3003, '2021-05-04', '121040', '电子1'],
            ["50050276", 4004, '2020-05-04', '121080', '金融1']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code", "cp_payer_name"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_capinc_multi_domcp")
        result_df = self.entry.spark.sql("""select * from corp_capinc_multi_domcp 
                                            where corp_code in ('5393225', '8290591', 
                                                                '21712617', '50050276')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_capinc_multi_domcp"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276']
        expected_corp_code_indexes = {
            "5393225": 0,
            "21712617": 0,
            "50050276": 0,
            "8290591": 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_bank_multi_legal(self):
        from scwgj.index_compute.indices.bank_multi_legal import bank_multi_legal
        assert bank_multi_legal

        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["zhangsan", "5393225"],
            ["zhangsan", "21712617"],
            ["wangwu", "50050276"],
            ["zhaoliu", "50050348"],
            ["zhangsan", "50050866"],
            ["wangwu", "50051842"],
            ["zhangsan", "50051180"],
            ["wangwu", "50050997"],
            ["zhangsan", "8290591"],
        ], schema=["frname", "corp_code"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "bank_multi_legal")
        result_df = self.entry.spark.sql("""select * from bank_multi_legal 
                                                    where corp_code in ('5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997')
                                                 """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["bank_multi_legal"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 1,
            '21712617': 1,
            '50051180': 1,
            '50050348': 0,
            '50050276': 1,
            '50050997': 1,
            '8290591': 1,
            '50050866': 1,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_basic_multi_phone(self):
        from scwgj.index_compute.indices.basic_multi_phone import basic_multi_phone
        assert basic_multi_phone
        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["18341567231", "5393225"],
            ["18341567231", "21712617"],
            ["18341567232", "50050276"],
            ["18341567233", "50050348"],
            ["18341567231", "50050866"],
            ["18341567232", "50051842"],
            ["18341567231", "50051180"],
            ["18341567232", "50050997"],
            ["18341567231", "8290591"],
        ], schema=["tel", "corp_code"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "basic_multi_phone")
        result_df = self.entry.spark.sql("""select * from basic_multi_phone 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276', '50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["basic_multi_phone"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617', '50050276',
                                   '50050348', '50050866', '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 1,
            '21712617': 1,
            '50051180': 1,
            '50050348': 0,
            '50050276': 1,
            '50050997': 1,
            '8290591': 1,
            '50050866': 1,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_prepay_guaramt_prop(self):
        from scwgj.index_compute.indices.corp_prepay_guaramt_prop import corp_prepay_guaramt_prop
        assert corp_prepay_guaramt_prop

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "122010", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "121030", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "121030", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "122010", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "122010", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "122010", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "121030", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_con_exguaran_new_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "1500", "2021-04-30"],
            ["8290591", "400", "2021-03-30"],
            ["21712617", "400", "2021-02-30"],
            ["50050276", "800", "2021-01-30"],
            ["50050348", "200", "2020-12-30"],
            ["50050866", "20", "2021-04-30"],
            ["50050997", "40", "2021-04-01"],
            ["50051180", "80", "2021-04-01"],
            ["50051842", "100", "2021-04-01"],
        ], schema=["corp_code", "usdguaranamount", "maturity"])
        Executor.register(name="pboc_con_exguaran_new_v", df_func=pboc_con_exguaran_new_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_prepay_guaramt_prop")
        result_df = self.entry.spark.sql("""select * from corp_prepay_guaramt_prop 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_prepay_guaramt_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['50050348', '8290591']
        expected_corp_code_indexes = {
            '50050348': 1,
            '8290591': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_rel_prepay_guar_prop(self):
        from scwgj.index_compute.indices.corp_rel_prepay_guar_prop import corp_rel_prepay_guar_prop
        assert corp_rel_prepay_guar_prop

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "122010", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "121030", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "121030", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "122010", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "122010", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "122010", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "121030", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        pboc_con_exguaran_new_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "1500", "2021-04-30"],
            ["8290591", "400", "2021-03-30"],
            ["21712617", "400", "2021-02-30"],
            ["50050276", "800", "2021-01-30"],
            ["50050348", "200", "2020-12-30"],
            ["50050866", "20", "2021-04-30"],
            ["50050997", "40", "2021-04-01"],
            ["50051180", "80", "2021-04-01"],
            ["50051842", "100", "2021-04-01"],
        ], schema=["corp_code", "usdguaranamount", "maturity"])
        Executor.register(name="pboc_con_exguaran_new_v", df_func=pboc_con_exguaran_new_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_rel_prepay_guar_prop")
        result_df = self.entry.spark.sql("""select * from corp_rel_prepay_guar_prop 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_rel_prepay_guar_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['50050348', '8290591']
        expected_corp_code_indexes = {
            '50050348': 1,
            '8290591': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_rel_prepay_guar_gap(self):
        from scwgj.index_compute.indices.corp_rel_prepay_guar_gap import corp_rel_prepay_guar_gap
        assert corp_rel_prepay_guar_gap

        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        pboc_con_exguaran_new_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "金融1", "2021-05-01"],
            ["8290591", "证券1", "2021-05-01"],
            ["21712617", "金融1", "2021-05-01"],
            ["50050276", "金融1", "2021-05-01"],
            ["50050348", "金融1", "2021-05-01"],
            ["50050866", "证券1", "2021-05-01"],
            ["50050997", "电子1", "2021-05-01"],
            ["50051180", "金融1", "2021-05-01"],
            ["50051842", "金融1", "2021-05-01"],
        ], schema=["corp_code", "guednameen0", "contractdate"])
        Executor.register(name="pboc_con_exguaran_new_v", df_func=pboc_con_exguaran_new_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "122010", "金融1"],
            ["5393225", 500, "2021-05-01", 1, "B", "121030", "证券1"],
            ["5393225", 500, "2021-05-01", 1, "A", "121030", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "122010", "证券1"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1"],
            ["21712617", 400, "2021-03-01", 1, "B", "122010", "金融1"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1"],
            ["50050866", 1000, "2021-05-15", 1, "A", "122010", "金融1"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1"],
            ["50051180", None, "2020-12-01", 1, "P", "121030", "金融1"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_rel_prepay_guar_gap")
        result_df = self.entry.spark.sql("""select * from corp_rel_prepay_guar_gap 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_rel_prepay_guar_gap"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '50050866', '50050348']
        expected_corp_code_indexes = {
            '5393225': 0,
            '8290591': 30,
            '50050866': 14,
            '50050348': 120
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_rmbpay_genrcv_gap(self):
        from scwgj.index_compute.indices.corp_rmbpay_genrcv_gap import corp_rmbpay_genrcv_gap
        assert corp_rmbpay_genrcv_gap

        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '121010'],
            ["8290591", 2000, '2021-05-04', '121020'],
            ["21712617", 3003, '2021-05-04', '121040'],
            ["50050276", 4004, '2020-05-04', '121080']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "122010", "金融1", 'CNY'],
            ["5393225", 500, "2021-05-01", 1, "B", "121030", "证券1", 'CNY'],
            ["5393225", 500, "2021-05-01", 1, "A", "121030", "证券1", 'CNY'],
            ["8290591", 200, "2021-04-01", 1, "A", "122010", "证券1", 'USD'],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "证券1", 'CNY'],
            ["21712617", 400, "2021-03-01", 1, "B", "122010", "金融1", 'EUR'],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "证券1", 'USD'],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "金融1", 'CNY'],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "证券1", 'CNY'],
            ["50050866", 1000, "2021-05-15", 1, "A", "122010", "金融1", 'USD'],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "电子1", 'EUR'],
            ["50051180", None, "2020-12-01", 1, "P", "121030", "金融1", 'CNY'],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "金融1", 'CNY']],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name", "tx_ccy_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_rmbpay_genrcv_gap")
        result_df = self.entry.spark.sql("""select * from corp_rmbpay_genrcv_gap 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_rmbpay_genrcv_gap"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591']
        expected_corp_code_indexes = {
            '5393225': 3.0,
            '8290591': 33.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_black_suspect_shell(self):
        from scwgj.index_compute.indices.black_suspect_shell import black_suspect_shell
        assert black_suspect_shell
        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["zhangsan", "5393225", "zhangsan"],
            ["zhangsan", "21712617", "zhangsan"],
            ["wangwu", "50050276", "wangwu"],
            ["zhaoliu", "50050348", "zhaoliu"],
            ["zhangsan", "50050866", "zhangsan"],
            ["wangwu", "50051842", "zhangsan"],
            ["zhangsan", "50051180", "zhangsan"],
            ["wangwu", "50050997", "zhangsan"],
            ["zhangsan", "8290591", "zhangsan"],
        ], schema=["frname", "corp_code", "contact_name"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "black_suspect_shell")
        result_df = self.entry.spark.sql("""select * from black_suspect_shell 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["black_suspect_shell"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 1,
            '50051180': 1,
            '50050348': 1,
            '50050276': 1,
            '50050997': 0,
            '8290591': 1,
            '50050866': 1,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_black_abnormal(self):
        from scwgj.index_compute.indices.black_abnormal import black_abnormal
        assert black_abnormal

        legal_adjudicative_documents_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司;自贡市邮政局"],
            ["成都中电建海赋房地产开发有限公司;成都华美国际旅行社有限公司"],
            ["绿地集团成都蜀峰房地产开发有限公司;成都希彤进出口贸易有限公司;四川格蓝环保工程有限公司"],
            ["成都玖锦科技有限公司"]
        ], schema=["defendant"])
        Executor.register(name="legal_adjudicative_documents_v", df_func=legal_adjudicative_documents_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "black_abnormal")
        result_df = self.entry.spark.sql("""select * from black_abnormal 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["black_abnormal"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50050997': 1,
            '50050276': 1,
            '50051842': 1,
            '5393225': 1,
            '50051180': 1,
            '21712617': 0,
            '50050866': 1,
            '50050348': 0,
            '8290591': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_exp_cp_isneg_mark(self):
        from scwgj.index_compute.indices.rmb_exp_cp_isneg_mark import rmb_exp_cp_isneg_mark
        assert rmb_exp_cp_isneg_mark

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan", "信托"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "zhangsan", "证券"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "lisi", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "lisi", "证券"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "wangwu", "信托"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "wangwu", "股票"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "zhaoliu", "信托"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "qianqi", "股票"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "zhangsan", "信托"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "lisi", "信托"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "wangwu", "股票"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "zhaoliu", "信托"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno", "rcvpay_attr_code", "tx_code", "cp_rcver_name", "tx_rem"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_cncc_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司", "zhangsan"],
            ["四川龙城保龄球俱乐部有限公司", "zhangsan"],
            ["成都中电建海赋房地产开发有限公司", "zhangsan"],
            ["成都市盈富教育咨询有限公司", "lisi"],
            ["成都华美国际旅行社有限公司", "wangwu"],
            ["绿地集团成都蜀峰房地产开发有限公司", "wangwu"],
            ["成都希彤进出口贸易有限公司", "zhaoliu"],
            ["四川格蓝环保工程有限公司", "zhaoliu"],
            ["成都玖锦科技有限公司", "zhaoliu"],
            ["成都索成易半导体有限公司", "zhaoliu"],
            ["成都浣花黉台酒店有限公司", "zhaoliu"],
            ["上海亚致力物流有限公司成都分公司", "qianqi"],
            ["成都市刘氏红杉贸易有限公司", "qianqi"],
            ["成都芯盟微科技有限公司", "zhangsan"],
        ], schema=["DEBTORNAME", "CRDTORNAME"])
        Executor.register(name="pboc_cncc_v", df_func=pboc_cncc_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_exp_cp_isneg_mark")
        result_df = self.entry.spark.sql("""select * from rmb_exp_cp_isneg_mark 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_exp_cp_isneg_mark"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 1,
            '50051180': 1,
            '50050348': 0,
            '50050276': 1,
            '50050997': 0,
            '8290591': 1,
            '50050866': 1,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_rel_entreport(self):
        from scwgj.index_compute.indices.corp_rel_entreport import corp_rel_entreport
        assert corp_rel_entreport

        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '121010', "2123432423"],
            ["8290591", 2000, '2021-05-04', '121020', "2123432424"],
            ["21712617", 3003, '2021-05-04', '121030', "2123432425"],
            ["50050276", 4004, '2020-05-04', '121080', "2123432426"]],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code", "rpt_tel"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, 1, "121010", "2021-05-01", "2123432423"],
            ["8290591", 600, 2, "121020", "2021-04-01", "2123432424"],
            ["21712617", 700, 3, "121030", "2021-03-01", "2123432425"],
            ["50050276", 800, 4, "121080", "2021-02-01", "2123432426"],
            ["50050348", 900, 5, "121090", "2021-01-01", "2123432427"],
            ["50050866", 1000, 6, "121990", "2021-05-15", "2123432428"],
            ["50050997", 1100, 7, "120010", "2020-12-01", "2123432429"],
            ["50051180", None, 7, "120010", "2020-12-01", "2123432430"],
            ["50051842", None, 7, "120010", "2020-12-01", "2123432431"]],
            schema=["corp_code", "tx_amt_usd", "rptno", "tx_code", "pay_date", "rpt_tel"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_rel_entreport")
        result_df = self.entry.spark.sql("""select * from corp_rel_entreport 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_rel_entreport"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 1,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 0,
            '8290591': 0,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_entreport_cp_isneg(self):
        from scwgj.index_compute.indices.corp_entreport_cp_isneg import corp_entreport_cp_isneg
        assert corp_entreport_cp_isneg

        black_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225"],
            ["8290591"],
            ["21712617"],
            ["50050276"],
            ["50050348"],
            ["50050866"]
        ], schema=["corp_code"])
        Executor.register(name="black_list_v", df_func=black_list_v, dependencies=[])
        negative_list_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="negative_list_v", df_func=negative_list_v, dependencies=[])
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '622011', '金融1'],
            ["8290591", 2000, '2021-05-04', '622012', '证券1'],
            ["21712617", 3003, '2021-05-04', '622013', '电子1'],
            ["50050276", 4004, '2020-05-04', '622011', '金融1']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code", "cp_payer_name"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan", "信托"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "zhangsan", "证券"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "lisi", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "lisi", "证券"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "wangwu", "信托"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "wangwu", "股票"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "zhaoliu", "信托"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "qianqi", "股票"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "zhangsan", "信托"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "lisi", "信托"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "wangwu", "股票"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "zhaoliu", "信托"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno",
                    "rcvpay_attr_code", "tx_code", "cp_rcver_name", "tx_rem"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_entreport_cp_isneg")
        result_df = self.entry.spark.sql("""select * from corp_entreport_cp_isneg 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_entreport_cp_isneg"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 1,
            '21712617': 1,
            '50051180': 1,
            '50050348': 1,
            '50050276': 1,
            '50050997': 1,
            '8290591': 1,
            '50050866': 1,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_capinc_multi_domcp_rel(self):
        from scwgj.index_compute.indices.corp_capinc_multi_domcp_rel import corp_capinc_multi_domcp_rel
        assert corp_capinc_multi_domcp_rel

        all_off_line_relations_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_rel_degree_1"])
        Executor.register(name="all_off_line_relations_v", df_func=all_off_line_relations_v, dependencies=[])
        all_company_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "成都商厦太平洋百货有限公司"],
            ["8290591", "自贡市邮政局"],
            ["21712617", "四川龙城保龄球俱乐部有限公司"],
            ["50050276", "成都中电建海赋房地产开发有限公司"],
            ["50050348", "成都市盈富教育咨询有限公司"],
            ["50050866", "绿地集团成都蜀峰房地产开发有限公司"],
            ["50051180", "成都希彤进出口贸易有限公司"],
            ["50051842", "四川格蓝环保工程有限公司"],
            ["50052714", "成都玖锦科技有限公司"]
        ], schema=["corp_code", "company_name"])
        Executor.register(name="all_company_v", df_func=all_company_v, dependencies=[])
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 1000, '2021-05-04', '622011', '金融1'],
            ["8290591", 2000, '2021-05-04', '622012', '证券1'],
            ["21712617", 3003, '2021-05-04', '622013', '电子1'],
            ["50050276", 4004, '2020-05-04', '622011', '金融1']],
            schema=["corp_code", "tx_amt_usd", "rcv_date", "tx_code", "cp_payer_name"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_capinc_multi_domcp_rel")
        result_df = self.entry.spark.sql("""select * from corp_capinc_multi_domcp_rel 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_capinc_multi_domcp_rel"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 0,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 0,
            '8290591': 0,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_use_isneg(self):
        from scwgj.index_compute.indices.rmb_use_isneg import rmb_use_isneg
        assert rmb_use_isneg

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan", "信托"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "zhangsan", "证券"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "zhangsan", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "lisi", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "lisi", "证券"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "wangwu", "信托"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "wangwu", "股票"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "zhaoliu", "信托"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "qianqi", "股票"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "zhangsan", "信托"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "lisi", "信托"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "wangwu", "股票"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "zhaoliu", "信托"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno",
                    "rcvpay_attr_code", "tx_code", "cp_rcver_name", "tx_rem"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_use_isneg")
        result_df = self.entry.spark.sql("""select * from rmb_use_isneg 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_use_isneg"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['50051180', '5393225', '8290591', '50050866', '21712617', '50050276']
        expected_corp_code_indexes = {
            '50051180': 1,
            '5393225': 1,
            '8290591': 1,
            '50050866': 1,
            '21712617': 1,
            '50050276': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rmb_exp_pay_circle(self):
        """
        todo: execution failed
        """
        from scwgj.index_compute.indices.rmb_exp_pay_circle import rmb_exp_pay_circle
        assert rmb_exp_pay_circle

        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "CHN-200", "信托"],
            ["5393225", 500, "2021-05-01", 1, "B", "929070", "USD-400", "证券"],
            ["5393225", 500, "2021-05-01", 1, "A", "929070", "EUR-200", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "929070", "CHN-200", "信托"],
            ["8290591", 200, "2021-04-01", 1, "A", "121020", "CHN-800", "证券"],
            ["21712617", 400, "2021-03-01", 1, "B", "929070", "CHN-200", "信托"],
            ["21712617", 400, "2021-03-01", 1, "C", "121030", "EUR-200", "股票"],
            ["50050276", 800, "2021-02-01", 1, "B", "929070", "USD-1000", "信托"],
            ["50050348", 200, "2021-01-01", 1, "A", "121090", "CHN-200", "股票"],
            ["50050866", 1000, "2021-05-15", 1, "A", "929070", "EUR-200", "信托"],
            ["50050997", 2000, "2020-12-01", 1, "A", "120010", "CHN-200", "信托"],
            ["50051180", None, "2020-12-01", 1, "P", "929070", "USD-200", "股票"],
            ["50051842", None, "2020-12-01", 1, "P", "120010", "CHN-200", "信托"]],
            schema=["corp_code", "tx_amt_usd", "pay_date", "rptno",
                    "rcvpay_attr_code", "tx_code", "cp_rcver_name", "tx_rem"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_cncc_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司", "zhangsan", 200],
            ["四川龙城保龄球俱乐部有限公司", "zhangsan", 100],
            ["成都中电建海赋房地产开发有限公司", "zhangsan", 100],
            ["成都市盈富教育咨询有限公司", "lisi", 200],
            ["成都华美国际旅行社有限公司", "wangwu", 100],
            ["绿地集团成都蜀峰房地产开发有限公司", "wangwu", 100],
            ["成都希彤进出口贸易有限公司", "zhaoliu", 200],
            ["四川格蓝环保工程有限公司", "zhaoliu", 100],
            ["成都玖锦科技有限公司", "zhaoliu", 100],
            ["成都索成易半导体有限公司", "zhaoliu", 100],
            ["成都浣花黉台酒店有限公司", "zhaoliu", 200],
            ["上海亚致力物流有限公司成都分公司", "qianqi", 100],
            ["成都市刘氏红杉贸易有限公司", "qianqi", 100],
            ["成都芯盟微科技有限公司", "zhangsan", 100],
        ], schema=["DEBTORNAME", "CRDTORNAME", "AMT"])
        Executor.register(name="pboc_cncc_v", df_func=pboc_cncc_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "rmb_use_isneg")
        result_df = self.entry.spark.sql("""select * from rmb_use_isneg 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["rmb_use_isneg"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['50051180', '5393225', '8290591', '50050866', '21712617', '50050276']
        expected_corp_code_indexes = {
            '50051180': 1,
            '5393225': 1,
            '8290591': 1,
            '50050866': 1,
            '21712617': 1,
            '50050276': 1
        }
        print(actual_corp_code_indexes)
        # assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        # assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_import_higher_decleared(self):
        from scwgj.index_compute.indices.import_higher_decleared import import_higher_decleared
        assert import_higher_decleared

        pboc_impf_v = self.entry.spark.createDataFrame(data=[
            ["50051842", "101", "201", 100, "2021-05-01"],
            ["21712617", "102", "202", 200, "2021-05-01"],
            ["50051180", "103", "203", 400, "2021-05-01"],
            ["50050348", "104", "204", 800, "2021-05-01"],
            ["50050276", "105", "205", 1000, "2021-05-01"],
            ["50050997", "106", "206", 100, "2021-05-01"],
            ["8290591", "107", "207", 100, "2021-05-01"],
        ], schema=["corp_code", "merch_code", "unit_code", "prod_deal_amt_usd", "imp_date"])
        Executor.register(name="pboc_impf_v", df_func=pboc_impf_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "import_higher_decleared")
        result_df = self.entry.spark.sql("""select * from import_higher_decleared 
                                                    where corp_code in ('5393225', '8290591', '21712617', 
                                                                        '50050276','50050348', '50050866', 
                                                                        '50051842', '50051180', '50050997')
                                                 """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_higher_decleared"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 0,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 0,
            '8290591': 0,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_export_higher_decleared(self):
        from scwgj.index_compute.indices.export_higher_decleared import export_higher_decleared
        assert export_higher_decleared
        pboc_expf_v = self.entry.spark.createDataFrame(data=[
            ["50051842", "101", "201", 100, "2021-05-01"],
            ["21712617", "102", "202", 200, "2021-05-01"],
            ["50051180", "103", "203", 400, "2021-05-01"],
            ["50050348", "104", "204", 800, "2021-05-01"],
            ["50050276", "105", "205", 1000, "2021-05-01"],
            ["50050997", "106", "206", 100, "2021-05-01"],
            ["8290591", "107", "207", 100, "2021-05-01"],
        ], schema=["corp_code", "merch_code", "unit_code", "prod_deal_amt_usd", "exp_date"])
        Executor.register(name="pboc_expf_v", df_func=pboc_expf_v, dependencies=[])
        param = Parameter(**{
            "one_year_ago": "2020-06-01",
            "current_date": "2021-06-01"
        })
        Executor.execute(self.entry, param, self.entry.logger, "export_higher_decleared")
        result_df = self.entry.spark.sql("""select * from export_higher_decleared 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["export_higher_decleared"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 0,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 0,
            '8290591': 0,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_special_pay_income_ratio(self):
        from scwgj.index_compute.indices.special_pay_income_ratio import special_pay_income_ratio
        assert special_pay_income_ratio

        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["5393225",  0, "5100"],
            ["21712617", 0, "5100"],
            ["50050276", 0, "5101"],
            ["50050348",  1, "5100"],
            ["50050866",  0, "5101"],
            ["50051842",  0, "5102"],
            ["50051180", 1, "5100"],
            ["50050997", 1, "5100"],
            ["8290591",  0, "5100"]
        ], schema=["corp_code", "is_taxfree", "safecode"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2020-05-01", "121030"],
            ["5393225", 500, "2020-05-01", "121040"],
            ["5393225", 500, "2020-05-01", "121030"],
            ["8290591", 200, "2020-04-01", "121030"],
            ["8290591", 200, "2020-04-01", "121030"],
            ["21712617", 400, "2020-03-01", "121030"],
            ["21712617", 400, "2020-03-01", "121030"],
            ["50050276", 800, "2020-02-01", "121040"],
            ["50050348", 200, "2020-01-01", "121030"],
            ["50050866", 1000, "2021-05-15", "121030"],
            ["50050997", 2000, "2020-12-01", "121030"],
            ["50051180", None, "2020-12-01", "121040"],
            ["50051842", None, "2020-12-01", "121030"]],
            schema=["corp_code", "pay_tamt_usd", "pay_date", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2020-05-01", "121030"],
            ["5393225", 500, "2020-05-01", "121030"],
            ["5393225", 500, "2020-05-01", "121030"],
            ["5393225", 500, "2020-05-01", "121030"],
            ["8290591", 200, "2020-04-01", "121030"],
            ["8290591", 200, "2020-04-01", "121030"],
            ["21712617", 400, "2020-03-01", "121030"],
            ["21712617", 400, "2020-03-01", "121030"],
            ["50050276", 800, "2020-02-01", "121040"],
            ["50050348", 200, "2020-01-01", "121030"],
            ["50050866", 1000, "2021-05-15", "121030"],
            ["50050997", 2000, "2020-12-01", "121030"],
            ["50051180", None, "2020-12-01", "121040"],
            ["50051842", None, "2020-12-01", "121030"]],
            schema=["corp_code", "rcv_tamt_usd", "rcv_date", "tx_code"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_pay_jn": "2021-04-05",
                "pboc_corp_rcv_jn": "2021-03-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "special_pay_income_ratio")
        result_df = self.entry.spark.sql("""select * from special_pay_income_ratio 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["special_pay_income_ratio"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0.0,
            '21712617': 0.0,
            '50051180': 0.0,
            '50050348': 0.0,
            '50050276': 0.0,
            '50050997': 1.0,
            '8290591': 0.0,
            '50050866': 0.0,
            '5393225': 0.5
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_special_export_ratio(self):
        from scwgj.index_compute.indices.special_export_ratio import special_export_ratio
        assert special_export_ratio

        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 0],
            ["21712617", 0],
            ["50050276", 0],
            ["50050348", 1],
            ["50050866", 0],
            ["50051842", 0],
            ["50051180", 1],
            ["50050997", 1],
            ["8290591", 0]
        ], schema=["corp_code", "is_taxfree"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        pboc_exp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "1233"],
            ["8290591", 25, "2021-04-01", "1234"],
            ["21712617", 100, "2021-03-01", "1233"],
            ["50050276", 200, "2021-02-01", "1233"],
            ["50050348", 50, "2021-01-01", "1234"],
            ["50050866", 25, "2021-05-15", "12356"],
            ["50050997", 400, "2020-12-01", "3424333"],
            ["50051180", None, "2020-12-01", "33435343"],
            ["50051842", None, "2020-12-01", "12134"]
        ], schema=["corp_code", "deal_amt", "exp_date", "custom_trade_mode_code"])
        Executor.register(name="pboc_exp_custom_rpt_v", df_func=pboc_exp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_exp_custom_rpt": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "special_export_ratio")
        result_df = self.entry.spark.sql("""select * from special_export_ratio 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["special_export_ratio"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0.0,
            '21712617': 1.0,
            '50051180': 0.0,
            '50050348': 0.0,
            '50050276': 1.0,
            '50050997': 0.0,
            '8290591': 1.0,
            '50050866': 0.0,
            '5393225': 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_special_import_ratio(self):
        from scwgj.index_compute.indices.special_import_ratio import special_import_ratio
        assert special_import_ratio

        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 0],
            ["21712617", 0],
            ["50050276", 0],
            ["50050348", 1],
            ["50050866", 0],
            ["50051842", 0],
            ["50051180", 1],
            ["50050997", 1],
            ["8290591", 0]
        ], schema=["corp_code", "is_taxfree"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "1233"],
            ["8290591", 25, "2021-04-01", "1234"],
            ["21712617", 100, "2021-03-01", "1233"],
            ["50050276", 200, "2021-02-01", "1233"],
            ["50050348", 50, "2021-01-01", "1234"],
            ["50050866", 25, "2021-05-15", "12356"],
            ["50050997", 400, "2020-12-01", "3424333"],
            ["50051180", None, "2020-12-01", "33435343"],
            ["50051842", None, "2020-12-01", "12134"]
        ], schema=["corp_code", "deal_amt", "imp_date", "custom_trade_mode_code"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_imp_custom_rpt": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "special_import_ratio")
        result_df = self.entry.spark.sql("""select * from special_import_ratio 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["special_import_ratio"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0.0,
            '21712617': 1.0,
            '50051180': 0.0,
            '50050348': 0.0,
            '50050276': 1.0,
            '50050997': 0.0,
            '8290591': 1.0,
            '50050866': 0.0,
            '5393225': 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_special_import_export_ratio(self):
        from scwgj.index_compute.indices.special_import_export_ratio import special_import_export_ratio
        assert special_import_export_ratio

        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 0],
            ["21712617", 0],
            ["50050276", 0],
            ["50050348", 1],
            ["50050866", 0],
            ["50051842", 0],
            ["50051180", 1],
            ["50050997", 1],
            ["8290591", 0]
        ], schema=["corp_code", "is_taxfree"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        pboc_exp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "1233"],
            ["8290591", 25, "2021-04-01", "1234"],
            ["21712617", 100, "2021-03-01", "1233"],
            ["50050276", 200, "2021-02-01", "1233"],
            ["50050348", 50, "2021-01-01", "1234"],
            ["50050866", 25, "2021-05-15", "12356"],
            ["50050997", 400, "2020-12-01", "3424333"],
            ["50051180", None, "2020-12-01", "33435343"],
            ["50051842", None, "2020-12-01", "12134"]
        ], schema=["corp_code", "deal_amt", "exp_date", "custom_trade_mode_code"])
        Executor.register(name="pboc_exp_custom_rpt_v", df_func=pboc_exp_custom_rpt_v, dependencies=[])
        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "1233"],
            ["8290591", 25, "2021-04-01", "1234"],
            ["21712617", 100, "2021-03-01", "1233"],
            ["50050276", 200, "2021-02-01", "1233"],
            ["50050348", 50, "2021-01-01", "1234"],
            ["50050866", 25, "2021-05-15", "12356"],
            ["50050997", 400, "2020-12-01", "3424333"],
            ["50051180", None, "2020-12-01", "33435343"],
            ["50051842", None, "2020-12-01", "12134"]
        ], schema=["corp_code", "deal_amt", "imp_date", "custom_trade_mode_code"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_exp_custom_rpt": "2021-04-05",
                "pboc_imp_custom_rpt": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "special_import_export_ratio")
        result_df = self.entry.spark.sql("""select * from special_import_export_ratio 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["special_import_export_ratio"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0.0,
            '21712617': 1.0,
            '21712617': 1.0,
            '50051180': 0.0,
            '50050348': 0.0,
            '50050276': 1.0,
            '50050997': 0.0,
            '8290591': 1.0,
            '50050866': 0.0,
            '5393225': 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_rel_abnormal_ratio(self):
        """
        todo: test
        """
        from scwgj.index_compute.indices.rel_abnormal_ratio import rel_abnormal_ratio
        assert rel_abnormal_ratio

    def test_settle_financing_attr(self):
        from scwgj.index_compute.indices.settle_financing_attr import settle_financing_attr
        assert settle_financing_attr

        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "121010", "T", 100],
            ["5393225", "2021-05-01", "121010", "T", 100],
            ["5393225", "2021-05-01", "121010", "T", 100],
            ["5393225", "2021-05-01", "121010", "T", 100],
            ["8290591", "2021-04-01", "121010", "T", 100],
            ["8290591", "2021-04-01", "121010", "T", 100],
            ["21712617", "2021-03-01", "121010", "A", 100],
            ["21712617", "2021-03-01", "121010", "T", 100],
            ["50050276", "2021-02-01", "121010", "T", 100],
            ["50050348", "2021-01-01", "121010", "A", 100],
            ["50050866", "2021-05-15", "121010", "T", 100],
            ["50050997", "2020-12-01", "121010", "T", 100],
            ["50051180", "2020-12-01", "121010", "T", 100],
            ["50051842", "2020-12-01", "121010", "T", 100]],
            schema=["corp_code", "rcv_date", "tx_code", "settle_method_code", "rcv_tamt"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        pboc_dfxloan_sign_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "1102", "2020-05-01", 200],
            ["5393225", "1102", "2020-05-01", 200],
            ["5393225", "1103", "2020-05-01", 200],
            ["5393225", "1102", "2020-05-01", None],
            ["8290591", "1102", "2020-04-01", 200],
            ["8290591", "1102", "2020-04-01", 200],
            ["21712617", "1104", "2020-03-01", 200],
            ["21712617", "1102", "2020-03-01", 200],
            ["50050276", "1102", "2020-02-01", 200],
            ["50050348", "1105", "2020-01-01", 200],
            ["50050866", "1102", "2021-05-15", 200],
            ["50050997", "1102", "2020-12-01", 200],
            ["50051180", None, "2020-12-01", 200],
            ["50051842", None, "2020-12-01", 200]
        ], schema=["corp_code", "domfx_loan_type_code", "interest_start_date", "sign_amt"])
        Executor.register(name=f"pboc_dfxloan_sign_v", df_func=pboc_dfxloan_sign_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-06-05",
                "pboc_dfxloan_sign": "2021-03-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "settle_financing_attr")
        result_df = self.entry.spark.sql("""select * from settle_financing_attr 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_settle_financing_attr_value_1 = {row["corp_code"]: row["settle_financing_attr_value_1"]
                                                for row in result_rows_list}
        actual_settle_financing_attr_value_2 = {row["corp_code"]: row["settle_financing_attr_value_2"]
                                                for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_settle_financing_attr_value_1 = {
            '50051842': 1.0,
            '21712617': 0.5,
            '50051180': 1.0,
            '50050348': 0.0,
            '50050276': 1.0,
            '50050997': 1.0,
            '8290591': 1.0,
            '50050866': 1.0,
            '5393225': 1.0
        }
        expected_settle_financing_attr_value_2 = {
            '50051842': 0,
            '21712617': 0,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 1,
            '8290591': 2,
            '50050866': 0,
            '5393225': 2
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_settle_financing_attr_value_1, actual_settle_financing_attr_value_1)
        assert has_same_k_v_map(expected_settle_financing_attr_value_2, actual_settle_financing_attr_value_2)

    def test_import_category_ct(self):
        from scwgj.index_compute.indices.import_category_ct import import_category_ct
        assert import_category_ct

        pboc_impf_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "121010"],
            ["5393225", "2021-05-01", "121010"],
            ["5393225", "2021-05-01", "131010"],
            ["5393225", "2021-05-01", "141010"],
            ["8290591", "2021-04-01", "121010"],
            ["8290591", "2021-04-01", "151010"],
            ["21712617", "2021-03-01", "121010"],
            ["21712617", "2021-03-01", "121010"],
            ["50050276", "2021-02-01", "121010"],
            ["50050348", "2021-01-01", "131010"],
            ["50050866", "2021-05-15", "141010"],
            ["50050997", "2020-12-01", "151010"],
            ["50051180", "2020-12-01", "161010"],
            ["50051842", "2020-12-01", "171010"]
        ], schema=["corp_code", "imp_date", "merch_code"])
        Executor.register(name=f"pboc_impf_v", df_func=pboc_impf_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_impf": "2021-06-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "import_category_ct")
        result_df = self.entry.spark.sql("""select * from import_category_ct 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_category_ct"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 1,
            '21712617': 1,
            '50051180': 1,
            '50050348': 1,
            '50050276': 1,
            '50050997': 1,
            '8290591': 2,
            '50050866': 1,
            '5393225': 3
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_import_actual_use(self):
        from scwgj.index_compute.indices.import_actual_use import import_actual_use
        assert import_actual_use

        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "公司A"],
            ["5393225", "2021-05-01", "公司B"],
            ["5393225", "2021-05-01", "公司C"],
            ["8290591", "2021-04-01", "公司D"],
            ["8290591", "2021-04-01", "公司A"],
            ["21712617", "2021-03-01", "公司A"],
            ["21712617", "2021-03-01", "公司A"],
            ["21712617", "2021-03-01", "公司B"],
            ["21712617", "2021-03-01", "公司A"],
            ["50050276", "2021-02-01", "公司C"],
            ["50050348", "2021-01-01", "公司A"],
            ["50050866", "2021-05-15", "公司A"],
            ["50050997", "2020-12-01", "公司A"],
            ["50051180", "2020-12-01", "公司A"],
            ["50051842", "2020-12-01", "公司A"]
        ], schema=["corp_code","imp_date", "rcv_unit"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_imp_custom_rpt": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "import_actual_use")
        result_df = self.entry.spark.sql("""select * from import_actual_use 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_actual_use"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 1,
            '21712617': 2,
            '50051180': 1,
            '50050348': 1,
            '50050276': 1,
            '50050997': 1,
            '8290591': 2,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_import_fxpay_ratio(self):
        from scwgj.index_compute.indices.import_fxpay_ratio import import_fxpay_ratio
        assert import_fxpay_ratio

        pboc_imp_custom_rpt_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 50, "2021-05-01", "1233"],
            ["8290591", 25, "2021-04-01", "1234"],
            ["21712617", 100, "2021-03-01", "1233"],
            ["50050276", 200, "2021-02-01", "1233"],
            ["50050348", 50, "2021-01-01", "1234"],
            ["50050866", 25, "2021-05-15", "12356"],
            ["50050997", 400, "2020-12-01", "3424333"],
            ["50051180", None, "2020-12-01", "33435343"],
            ["50051842", None, "2020-12-01", "12134"]
        ], schema=["corp_code", "deal_amt", "imp_date", "custom_trade_mode_code"])
        Executor.register(name="pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2020-05-01", "121030"],
            ["5393225", 500, "2020-05-01", "121040"],
            ["5393225", 500, "2020-05-01", "121030"],
            ["8290591", 200, "2020-04-01", "121030"],
            ["8290591", 200, "2020-04-01", "121030"],
            ["21712617", 400, "2020-03-01", "121030"],
            ["21712617", 400, "2020-03-01", "121030"],
            ["50050276", 800, "2020-02-01", "121040"],
            ["50050348", 200, "2020-01-01", "121030"],
            ["50050866", 1000, "2021-05-15", "121030"],
            ["50050997", 2000, "2020-12-01", "121030"],
            ["50051180", None, "2020-12-01", "121040"],
            ["50051842", None, "2020-12-01", "121030"]],
            schema=["corp_code", "pay_tamt_usd", "pay_date", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_imp_custom_rpt": "2021-04-05",
                "pboc_corp_pay_jn": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "import_fxpay_ratio")
        result_df = self.entry.spark.sql("""select * from import_fxpay_ratio 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["import_fxpay_ratio"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0.0,
            '21712617': 100.0,
            '50051180': 0.0,
            '50050348': 50.0,
            '50050276': 200.0,
            '50050997': 400.0,
            '8290591': 25.0,
            '50050866': 0.0,
            '5393225': 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_basic_multi_phone(self):
        from scwgj.index_compute.indices.basic_multi_phone import basic_multi_phone
        assert basic_multi_phone

        pboc_corp_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "13982650079"],
            ["21712617", "13982650078"],
            ["50050276", "13982650077"],
            ["50050348", "2813456"],
            ["50050866", "13982650075"],
            ["50051842", "13982650074"],
            ["50051180", "40004444"],
            ["50050997", "13982650072"],
            ["8290591", "13982650071"]
        ], schema=["corp_code", "tel"])
        Executor.register(name="pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "13982650071"],
            ["21712617", "13982650072"],
            ["50050276", "13982650073"],
            ["50050348", "2813497"],
            ["50050866", "13982650075"],
            ["50051842", "13982650076"],
            ["50051180", "13982650077"],
            ["50050997", "13982650078"],
            ["8290591", "13982650079"]
        ], schema=["corp_code", "rpt_tel"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "13982650079"],
            ["21712617", "13982650079"],
            ["50050276", "13982650079"],
            ["50050348", "2867365"],
            ["50050866", "13982650079"],
            ["50051842", "13982650079"],
            ["50051180", "13982650079"],
            ["50050997", "13982650079"],
            ["8290591", "08262345865"]
        ], schema=["corp_code", "rpt_tel"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_imp_custom_rpt": "2021-04-05",
                "pboc_corp_pay_jn": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "basic_multi_phone")
        result_df = self.entry.spark.sql("""select * from basic_multi_phone 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["basic_multi_phone"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 1,
            '21712617': 1,
            '50051180': 1,
            '50050348': 0,
            '50050276': 1,
            '50050997': 1,
            '8290591': 1,
            '50050866': 1,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_suspect_illegal_rel(self):
        from scwgj.index_compute.indices.suspect_illegal_rel import suspect_illegal_rel
        assert suspect_illegal_rel

        legal_adjudicative_documents_v = self.entry.spark.createDataFrame(data=[
            ["成都商厦太平洋百货有限公司;自贡市邮政局", "违规交易"],
            ["成都中电建海赋房地产开发有限公司;成都华美国际旅行社有限公司", "地下钱庄"],
            ["绿地集团成都蜀峰房地产开发有限公司;成都希彤进出口贸易有限公司;四川格蓝环保工程有限公司", "地下钱庄"],
            ["成都玖锦科技有限公司", "地下钱庄"]
        ], schema=["defendant", "action_cause"])
        Executor.register(name="legal_adjudicative_documents_v", df_func=legal_adjudicative_documents_v,
                          dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "2020-05-01", "121030", "四川金机进出口贸易有限公司"],
            ["5393225", 500, "2020-05-01", "121040", "成都悠鹿国际旅行社有限责任公司"],
            ["5393225", 500, "2020-05-01", "121030", "四川金机进出口贸易有限公司"],
            ["8290591", 200, "2020-04-01", "121030", "成都悠鹿国际旅行社有限责任公司"],
            ["8290591", 200, "2020-04-01", "121030", "成都海腾精益汽车零部件有限公司"],
            ["21712617", 400, "2020-03-01", "121030", "四川金机进出口贸易有限公司"],
            ["21712617", 400, "2020-03-01", "121030", "成都悠鹿国际旅行社有限责任公司"],
            ["50050276", 800, "2020-02-01", "121040", "四川金机进出口贸易有限公司"],
            ["50050348", 200, "2020-01-01", "121030", "成都诺新贸易有限公司"],
            ["50050866", 1000, "2021-05-15", "121030", "四川金机进出口贸易有限公司"],
            ["50050997", 2000, "2020-12-01", "121030", "四川金机进出口贸易有限公司"],
            ["50051180", None, "2020-12-01", "121040", "成都海腾精益汽车零部件有限公司"],
            ["50051842", None, "2020-12-01", "121030", "四川金机进出口贸易有限公司"]],
            schema=["corp_code", "pay_tamt_usd", "pay_date", "tx_code", "counter_party"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_df = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "121010", "T", 100, "四川金机进出口贸易有限公司"],
            ["5393225", "2021-05-01", "121010", "T", 100, "成都海腾精益汽车零部件有限公司"],
            ["5393225", "2021-05-01", "121010", "T", 100, "四川金机进出口贸易有限公司"],
            ["5393225", "2021-05-01", "121010", "T", 100, "成都悠鹿国际旅行社有限责任公司"],
            ["8290591", "2021-04-01", "121010", "T", 100, "成都海腾精益汽车零部件有限公司"],
            ["8290591", "2021-04-01", "121010", "T", 100, "四川金机进出口贸易有限公司"],
            ["21712617", "2021-03-01", "121010", "A", 100, "成都诺新贸易有限公司"],
            ["21712617", "2021-03-01", "121010", "T", 100, "四川金机进出口贸易有限公司"],
            ["50050276", "2021-02-01", "121010", "T", 100, "成都悠鹿国际旅行社有限责任公司"],
            ["50050348", "2021-01-01", "121010", "A", 100, "成都诺新贸易有限公司"],
            ["50050866", "2021-05-15", "121010", "T", 100, "四川金机进出口贸易有限公司"],
            ["50050997", "2020-12-01", "121010", "T", 100, "成都海腾精益汽车零部件有限公司"],
            ["50051180", "2020-12-01", "121010", "T", 100, "四川金机进出口贸易有限公司"],
            ["50051842", "2020-12-01", "121010", "T", 100, "成都诺新贸易有限公司"]],
            schema=["corp_code", "rcv_date", "tx_code", "settle_method_code", "rcv_tamt", "counter_party"])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_df, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-04-05",
                "pboc_corp_pay_jn": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "suspect_illegal_rel")
        result_df = self.entry.spark.sql("""select * from suspect_illegal_rel 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()

        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["suspect_illegal_rel"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 0,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 0,
            '8290591': 0,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_suspect_shell(self):
        """
        todo: test
        """
        from scwgj.index_compute.indices.suspect_shell import suspect_shell
        assert suspect_shell

    def test_entreport_no_income(self):
        from scwgj.index_compute.indices.entreport_no_income import entreport_no_income
        assert entreport_no_income
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500000, "CNY", "2021-05-01", "122020"],
            ["8290591", 600000, "CNY", "2021-04-01", "121030"],
            ["21712617", 700000, "USD", "2021-03-01", "122020"],
            ["50050276", 800000, "CNY", "2021-02-01", "122020"],
            ["50050348", 9000, "CNY", "2021-01-01", "121030"],
            ["50050866", 1000000, "FRI", "2021-05-15", "122050"],
            ["50050997", 110000, "CNY", "2020-12-01", "122020"],
            ["50051180", None, "CNY", "2020-12-01", "121030"],
            ["50051842", None, "HKD", "2020-12-01", "122020"]],
            schema=["corp_code", "tx_amt_usd", "tx_ccy_code", "pay_date", "tx_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "CNY", "2021-05-01", "122020"],
            ["8290591", 600, "CNY", "2021-04-01", "121030"],
            ["21712617", 700, "USD", "2021-03-01", "122020"],
            ["50050276", 800, "CNY", "2021-02-01", "122020"],
            ["50050348", 900, "CNY", "2021-01-01", "121030"],
            ["50050866", 1000, "FRI", "2021-05-15", "122050"],
            ["50050997", 1100, "CNY", "2020-12-01", "122020"],
            ["50051180", None, "CNY", "2020-12-01", "121030"],
            ["50051842", None, "HKD", "2020-12-01", "122020"]],
            schema=["corp_code", "tx_amt_usd", "tx_ccy_code", "rcv_date", "tx_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-04-05",
                "pboc_corp_pay_jn": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "entreport_no_income")
        result_df = self.entry.spark.sql("""select * from entreport_no_income 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["entreport_no_income"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 1,
            '50051180': 0,
            '50050348': 0,
            '50050276': 0,
            '50050997': 0,
            '8290591': 0,
            '50050866': 0,
            '5393225': 0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_corp_capinc_prepay_prop(self):
        from scwgj.index_compute.indices.corp_capinc_prepay_prop import corp_capinc_prepay_prop
        assert corp_capinc_prepay_prop
        pboc_corp_rcv_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "CNY", "2021-05-01", "622011"],
            ["8290591", 600, "CNY", "2021-04-01", "622012"],
            ["21712617", 700, "USD", "2021-03-01", "622013"],
            ["50050276", 800, "CNY", "2021-02-01", "622011"],
            ["50050348", 900, "CNY", "2021-01-01", "622011"],
            ["50050866", 1000, "FRI", "2021-05-15", "622012"],
            ["50050997", 1100, "CNY", "2020-12-01", "622011"],
            ["50051180", None, "CNY", "2020-12-01", "622014"],
            ["50051842", None, "HKD", "2020-12-01", "622011"]],
            schema=["corp_code", "tx_amt_usd", "tx_ccy_code", "rcv_date", "tx_code"])
        Executor.register(name="pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "CNY", "2021-05-01", "A"],
            ["8290591", 600, "CNY", "2021-04-01", "A"],
            ["21712617", 700, "USD", "2021-03-01", "T"],
            ["50050276", 800, "CNY", "2021-02-01", "A"],
            ["50050348", 900, "CNY", "2021-01-01", "A"],
            ["50050866", 1000, "FRI", "2021-05-15", "U"],
            ["50050997", 1100, "CNY", "2020-12-01", "A"],
            ["50051180", None, "CNY", "2020-12-01", "A"],
            ["50051842", None, "HKD", "2020-12-01", "A"]],
            schema=["corp_code", "tx_amt_usd", "tx_ccy_code", "pay_date", "rcvpay_attr_code"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-04-05",
                "pboc_corp_pay_jn": "2021-04-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "corp_capinc_prepay_prop")
        result_df = self.entry.spark.sql("""select * from corp_capinc_prepay_prop 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["corp_capinc_prepay_prop"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0.0,
            '21712617': 700.0,
            '50051180': 0.0,
            '50050348': 1.0,
            '50050276': 1.0,
            '50050997': 1.0,
            '8290591': 1.0,
            '50050866': 0.0,
            '5393225': 0.0
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)

    def test_cap_over_thr(self):
        from scwgj.index_compute.indices.cap_over_thr import cap_over_thr
        assert cap_over_thr
        pboc_corp_pay_jn_v = self.entry.spark.createDataFrame(data=[
            ["5393225", 500, "929070", "2021-05-01", "1823456565"],
            ["8290591", 600, "929070", "2021-04-01", "43244231843254"],
            ["21712617", 700, "929070", "2021-03-01", "1823456566"],
            ["50050276", 800, "929070", "2021-02-01", "1923456567"],
            ["50050348", 900, "929070", "2021-01-01", "1823456568"],
            ["50050866", 1000, "929070", "2021-05-15", "43244231843254"],
            ["50050997", 1100, "929070", "2020-12-01", "43244231843255"],
            ["50051180", None, "929070", "2020-12-01", "43244231843256"],
            ["50051842", None, "929070", "2020-12-01", "43244231943254"]],
            schema=["corp_code", "pay_tmat_usd", "tx_code", "pay_date", "invoice_no"])
        Executor.register(name="pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        pboc_corp_lcy_v = self.entry.spark.createDataFrame(data=[
            ["5393225", "2021-05-01", "018", "24343", 500],
            ["8290591", "2021-05-01", "018", "24343", 600],
            ["21712617", "2021-05-01", "018", "24344", 700],
            ["50050276", "2021-05-01", "019", "24343", 800],
            ["50050348", "2021-05-01", "018", "24345", 900],
            ["50050866", "2021-05-01", "0189", "24343", 1000],
            ["50050997", "2021-05-01", "018", "24343", 1100],
            ["50051180", "2021-05-01", "0189", "24343", None],
            ["50051842", "2021-05-01", "018", "24343", None]
        ], schema=["corp_code", "deal_date", "salefx_use_code", "acct_attr_code", "salefx_amt_usd"])
        Executor.register(name="pboc_corp_lcy_v", df_func=pboc_corp_lcy_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-06-01",
                "pobc_corp_lcy": "2021-06-05"
            }
        })
        Executor.execute(self.entry, param, self.entry.logger, "cap_over_thr")
        result_df = self.entry.spark.sql("""select * from cap_over_thr 
                                            where corp_code in ('5393225', '8290591', '21712617', 
                                                                '50050276','50050348', '50050866', 
                                                                '50051842', '50051180', '50050997')
                                         """)
        result_rows_list = result_df.collect()
        actual_corp_code_list = [row["corp_code"] for row in result_rows_list]
        actual_corp_code_indexes = {row["corp_code"]: row["cap_over_thr"]
                                    for row in result_rows_list}
        expected_corp_code_list = ['5393225', '8290591', '21712617',
                                   '50050276', '50050348', '50050866',
                                   '50051842', '50051180', '50050997']
        expected_corp_code_indexes = {
            '50051842': 0,
            '21712617': 1,
            '50051180': 0,
            '50050348': 1,
            '50050276': 0,
            '50050997': 1,
            '8290591': 1,
            '50050866': 0,
            '5393225': 1
        }
        assert has_same_element_list(expected_corp_code_list, actual_corp_code_list)
        assert has_same_k_v_map(expected_corp_code_indexes, actual_corp_code_indexes)
