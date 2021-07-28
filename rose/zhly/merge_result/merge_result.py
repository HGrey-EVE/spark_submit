#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 智慧楼宇企业名录
:Author: zhouchao@bbdservice.com
:Date: 2021-05-06 16:08
"""

import json
import os
import re

from whetstone.core.entry import Entry
from pyspark.sql import functions as fun, SparkSession, DataFrame
from zhly.proj_common.common_util import save_text_to_hdfs


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    spark: SparkSession = entry.spark

    # 项目共有配置
    hive_table = entry.cfg_mgr.get("hive", "database")
    index_pool_path = entry.cfg_mgr.hdfs.get_result_path("zhly", "index_pool_hdfs_path")
    index_path = entry.cfg_mgr.hdfs.get_result_path("zhly", "index_hdfs_path")
    out_path = entry.cfg_mgr.hdfs.get_result_path("zhly", "result_hdfs_path")
    company_path = entry.cfg_mgr.hdfs.get_input_path("zhly", "ly_company_path")
    temp_index_path = entry.cfg_mgr.hdfs.get_tmp_path("zhly", "temp_index_merge")
    entry.logger.info(f"配置信息hive_db:{hive_table}")
    # 获取楼宇企业名录
    ly_company = spark.read.parquet(company_path)
    ly_company.createOrReplaceTempView('basic_tmp')

    # 获取指标池指标数据
    entry.logger.info("获取指标池中相关指标")
    get_index_pool_data(spark, hive_table, index_pool_path)
    entry.logger.info("获取指标池中相关指标完成,开始合并指标")
    # 合并指标数据
    merge_data(spark, index_path, index_pool_path, out_path, ly_company, temp_index_path)
    entry.logger.info("指标合并完成,项目计算完毕")
def get_index_pool_data(spark, hive_table, index_pool_path):
    # 指标池指标
    index_pool_list = [
        "qy_cpws_num_1year",
        "qy_bgcpws_num_1year",
        "qy_yjfxcpws_num_1year",
        "qy_cpws_rate_3monthhbzzl",
        "qy_bgktgg_num_1year",
        "qy_yjfxktgg_num_1year",
        "qy_sx_num_1year",
        "qy_sx_rate_3monthhbzzl",
        "qy_sxbzxr_num_1year",
        "qy_jyyc_num_1year",
        "qy_qs_num",
        "qy_gqcz_num",
        "qy_zl_num",
        "qy_zl_rate_3monthhbzzl",
        "qy_zl_num_1year",
        "qy_rz_num",
        "qy_rz_rate_3monthhbzzl",
        "qy_rz_num_1year",
        "qy_sb_num",
        "qy_sb_num_1year",
        "qy_dxzp_num",
        "qy_ssjyszp_num",
        "qy_zprs_num",
        "qy_jyycts_num",
        "qy_jyyc_num",
        "qy_jyycYZCD_num",
        "qy_deg1_jyyc_num"
    ]
    index_pool_dims = ','.join(index_pool_list)
    index_value = spark.sql(f"""
            select a.bbd_qyxx_id, {index_pool_dims}, 
              b.qy_bgktgg_num_1year+b.qy_ygktgg_num_1year as qy_ktgg_num_1year,
              b.qy_dxzp_num/b.qy_zprs_num as qy_zpbk_zb,
              b.qy_ssjyszp_num/b.qy_zprs_num as qy_zpyjsjys_zb
            from basic_tmp a
            join {hive_table}.index_pool b
            on a.bbd_qyxx_id=b.bbd_qyxx_id
        """)
    save_text_to_hdfs(df=index_value, path=index_pool_path, repartition_number=10, force_repartition=True)


def merge_data(spark, index_path, index_pool_path, out_path, ly_company, temp_index_path):
    from time import sleep
    partitions = os.popen(f"hadoop fs -ls {index_path}").readlines()
    index_list = []
    for partition in partitions:
        if index_path in partition:
            index_list.append(re.findall(r"{index_path}.*".format(index_path=index_path), partition)[0])

    company = ly_company.drop('company_name')

    tmp_df = company
    index_tmp_list = []
    for i in range(0, len(index_list)):
        index_df_path = index_list[i]
        _part = spark.read.parquet(index_df_path)
        tmp_df = tmp_df.join(_part, 'bbd_qyxx_id','left').fillna(0)
        if (i != 0 and i % 3 == 0) or i == len(index_list) - 1:
            merge_tmp_path = os.path.join(temp_index_path, 'part_{}'.format(str(int(i / 3) if i % 3 == 0 else int(i / 3 + 1))))
            tmp_df.repartition(1).write.csv(merge_tmp_path, header=True, sep='\t', mode='overwrite')
            tmp_df = company
            index_tmp_list.append(merge_tmp_path)
    final_df = company
    for part_path in index_tmp_list:
        merge_df = spark.read.csv(part_path, header=True, sep='\t')
        merge_df.drop('bbd_table', 'bbd_type', 'bbd_uptime', 'bbd_dotime')
        final_df = final_df.join(merge_df, 'bbd_qyxx_id', 'left').fillna(0)

    index_pools = spark.read.parquet(index_pool_path)
    merge_result = final_df.join(index_pools, 'bbd_qyxx_id', 'left').fillna(0)
    merge_result.drop_duplicates(['bbd_qyxx_id']).repartition(10).write.json(out_path, mode='overwrite')


def post_check(entry: Entry):
    return True
