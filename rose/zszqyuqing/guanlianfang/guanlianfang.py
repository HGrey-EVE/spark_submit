# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/7/21 10:12
"""

import os
import re
from typing import Text

# 由于MD5模块在python3中被移除，在python3中使用hashlib模块进行md5操作
from hashlib import md5
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType, BooleanType, FloatType

from zszqyuqing.proj_common.hive_util import HiveUtil

from whetstone.core.entry import Entry

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark import SparkConf, Row
from pyspark.sql import SparkSession, DataFrame

# DWH_NAME = "dw"
# SPARK_APP_NAME = "zszqyuqing_guanlianfang"
# 测试环境
# OUTPUT_HDFS_PATH = "hdfs:///tmp/thk/zentao16226"
# 测试环境外管局名单存放路径
# ORIGINAL_LIS_PATH = "hdfs:///tmp/thk/data/zentao16226/"
# 生产环境所有上市公司名单存放路径
# ALL_PUBLIC_COMPANY_PATH = "hdfs:///user/zszqyuqing/zszq/yq_v1/data"
# OUTPUT_HDFS_PARTITION_NUMS = 1
DISPLAY_SQL = True


# 执行sql得到DataFrame
def ss_sql(entry: Entry, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        print(sql)
    return entry.spark.sql(sql)


# 判断公司是否是上市公司
def get_is_public_company(bbd_qyxx_id, entry: Entry):
    num = ss_sql(entry, f"""
        select
            bbd_qyxx_id
        from pub_comp_qyxx_id_table pc
        where pc.bbd_qyxx_id={bbd_qyxx_id}
    """).count()
    if num > 0:
        return "上市公司"
    else:
        return "非上市公司"


# 将关联度数转为文字
def get_associated_degree_name(associated_degree):
    if associated_degree == 0:
        return "自身"
    elif associated_degree == 1:
        return "1度关联公司"
    elif associated_degree == 2:
        return "2度关联公司"


# simple filter function
@udf(returnType=BooleanType())
def ratio_filter(invest_ratio, bili):
    if invest_ratio == '' or invest_ratio == 'null':
        invest_ratio = '0'
    ratio_f = float(invest_ratio.strip('%'))  # 去掉invest_ratio 字符串中的 %
    ratio = ratio_f / 100.0
    return ratio > bili


@udf(returnType=FloatType())
def get_float(invest_ratio):
    if invest_ratio == '' or invest_ratio == 'null':
        invest_ratio = '0'
    ratio_f = float(invest_ratio.strip('%'))  # 去掉invest_ratio 字符串中的 %
    ratio = ratio_f / 100.0
    return ratio


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    new_dt_relation = HiveUtil.newest_partition(entry.spark, "dw.off_line_relations")
    new_dt_company_a = HiveUtil.newest_partition(entry.spark, "pj.ms_ipo_company_a")
    new_dt_company_h = HiveUtil.newest_partition(entry.spark, "pj.ms_ipo_company_h")

    # 先取出a、h股上市公司的离线关联方
    destination_fields = ["a.bbd_qyxx_id", "company_name", "destination_name", "destination_bbd_id",
                          "destination_degree"]
    # 初始化hive表
    destination_df_1 = ss_sql(entry, f'''
        SELECT
            {', '.join(destination_fields)}
        FROM dw.off_line_relations a 
        where dt='{new_dt_relation}'
            and destination_isperson=0
            and destination_degree<=1
    ''').cache()
    destination_df_1.createOrReplaceTempView('off_line_relations_1')

    company_a_df = ss_sql(entry, f'''
        select 
        distinct bbd_qyxx_id 
        from pj.ms_ipo_company_a 
        where dt = {new_dt_company_a}
    ''').cache()
    company_a_df.createOrReplaceTempView('ms_ipo_company_a')

    company_h_df = ss_sql(entry, f'''
            select 
            distinct bbd_qyxx_id 
            from pj.ms_ipo_company_h
            where dt = {new_dt_company_h}
        ''').cache()
    company_h_df.createOrReplaceTempView('ms_ipo_company_h')

    # 分别对投资方和被投资方进行匹配
    destination_df_a = ss_sql(entry, f"""
        SELECT
            {', '.join(destination_fields)}
        FROM off_line_relations_1 a 
        join ms_ipo_company_a b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """)
    destination_df_h = ss_sql(entry, f"""
        SELECT
            {', '.join(destination_fields)}
        FROM off_line_relations_1 a 
        join ms_ipo_company_h b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """)
    destination_df_a.union(destination_df_h).createOrReplaceTempView("destination_table")

    source_fields = ["a.bbd_qyxx_id", "company_name", "source_name", "source_bbd_id", "source_degree"]

    destination_df_2 = ss_sql(entry, f'''
                   SELECT
                       {', '.join(source_fields)}
                   FROM dw.off_line_relations a 
                   where dt='{new_dt_relation}'
                       and source_isperson=0
                       and source_degree<=1
               ''').cache()
    destination_df_2.createOrReplaceTempView('off_line_relations_2')

    source_df_a = ss_sql(entry, f"""
        SELECT
            {', '.join(source_fields)}
        FROM off_line_relations_2 a 
        join ms_ipo_company_a b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """)
    source_df_h = ss_sql(entry, f"""
        SELECT
            {', '.join(source_fields)}
        FROM off_line_relations_2 a 
        join ms_ipo_company_h b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """)
    source_df_a.union(source_df_h).createOrReplaceTempView("source_table")

    # 获取0,1,2度被投资方
    # 0度被投资方字段
    destination_0_fields = ["bbd_qyxx_id", "company_name", "destination_name as associated_name",
                            "destination_bbd_id as associated_id", "destination_degree as associated_degree",
                            "1 as invest_ratio", "1 as relation"]
    # 0度被投资方
    destination_0_df = ss_sql(entry, f"""
        SELECT
            {', '.join(destination_0_fields)}
        FROM destination_table
        where destination_degree=0
    """)

    # 1度被投资方字段
    destination_1_2_fields = ["a.bbd_qyxx_id", "a.company_name", "a.destination_name as associated_name",
                              "a.destination_bbd_id as associated_id", "a.destination_degree as associated_degree",
                              "1 as invest_ratio", "1 as relation"]
    # 1度被投资方,持股比例需大于5%，才可以认定为一度关联关系
    destination_1_df = ss_sql(entry, f"""
        SELECT
            {', '.join(destination_1_2_fields)}
        FROM destination_table a
        where destination_degree=1
        """)

    # 直系二度关联关系，公司A对B的持股比例x与B对C的持股比例y必须分别大于20%
    ss_sql(entry, f"""
        SELECT
            {', '.join(destination_1_2_fields)}
        FROM destination_table a
        where destination_degree=1
        """).createOrReplaceTempView("destination_20_table")
    destination_2z_df = ss_sql(entry, f"""
        SELECT
          a.bbd_qyxx_id,
          a.company_name,
          b.associated_name,
          b.associated_id,
          2 as associated_degree,
          1 as invest_ratio,
          1 as relation
        FROM destination_20_table a
        inner join destination_20_table b
          on a.associated_id=b.bbd_qyxx_id
        """)

    # 并行二度关联关系，公司A对B的持股比例x与C对B的持股比例y必须分别大于30%
    ss_sql(entry, f"""
        SELECT
            {', '.join(destination_1_2_fields)}
        FROM destination_table a
        where destination_degree=1
        """).createOrReplaceTempView("destination_30_table")
    destination_2b_df = ss_sql(entry, f"""
        SELECT
          a.bbd_qyxx_id,
          a.company_name,
          b.company_name as associated_name,
          b.bbd_qyxx_id as associated_id,
          2 as associated_degree,
          1 as invest_ratio,
          2 as relation
        FROM destination_30_table a
        inner join destination_30_table b
          on a.associated_id=b.associated_id
          and a.bbd_qyxx_id!=b.bbd_qyxx_id
        """)

    # 获取0,1,2度投资方
    # 0度投资方字段
    source_0_fields = ["bbd_qyxx_id", "company_name", "source_name as associated_name",
                       "source_bbd_id as associated_id", "source_degree as associated_degree", "1 as invest_ratio",
                       "1 as relation"]
    # 0度投资方
    source_0_df = ss_sql(entry, f"""
        SELECT
            {', '.join(source_0_fields)}
        FROM source_table
        where source_degree=0
        """)

    # 1度投资方字段
    source_1_2_fields = ["a.bbd_qyxx_id", "a.company_name", "a.source_name as associated_name",
                         "a.source_bbd_id as associated_id", "a.source_degree as associated_degree",
                         "1 as invest_ratio", "1 as relation"]
    # 1度投资方,持股比例需大于5%，才可以认定为一度关联关系
    source_1_df = ss_sql(entry, f"""
        SELECT
            {', '.join(source_1_2_fields)}
        FROM source_table a
        where source_degree=1""")

    # 直系二度关联关系，公司A对B的持股比例x与B对C的持股比例y必须分别大于20%
    ss_sql(entry, f"""
        SELECT
            {', '.join(source_1_2_fields)}
        FROM source_table a
        where source_degree=1
        """).createOrReplaceTempView("source_20_table")
    source_2z_df = ss_sql(entry, f"""
        SELECT
          a.bbd_qyxx_id,
          a.company_name,
          b.associated_name,
          b.associated_id,
          2 as associated_degree,
          1 as invest_ratio,
          1 as relation
        FROM source_20_table a
        inner join source_20_table b
          on a.associated_id=b.bbd_qyxx_id
        """)

    # 并行二度关联关系，公司A对B的持股比例x与C对B的持股比例y必须分别大于30%
    ss_sql(entry, f"""
        SELECT
            {', '.join(source_1_2_fields)}
        FROM source_table a
        where source_degree=1
        """).createOrReplaceTempView("source_30_table")
    source_2b_df = ss_sql(entry, f"""
        SELECT
          a.bbd_qyxx_id,
          a.company_name,
          b.company_name as associated_name,
          b.bbd_qyxx_id as associated_id,
          2 as associated_degree,
          1 as invest_ratio,
          2 as relation
        FROM source_30_table a
        inner join source_30_table b
          on a.associated_id=b.associated_id
          and a.bbd_qyxx_id!=b.bbd_qyxx_id
        """)

    # 所有二度以内公司
    # all_company_df = destination_0_df\
    #     .union(destination_1_df)\
    #     .union(destination_2z_df)\
    #     .union(destination_2b_df)\
    #     .union(source_0_df)\
    #     .union(source_1_df)\
    #     .union(source_2z_df)\
    #     .union(source_2b_df)\
    #     .dropDuplicates()
    all_company_df = destination_0_df \
        .union(destination_1_df) \
        .union(destination_2z_df) \
        .union(source_0_df) \
        .union(source_1_df) \
        .union(source_2z_df) \
        .dropDuplicates()
    all_company_df.createOrReplaceTempView("all_company_table")

    # 得到所有上市公司关联方(2度以内)
    associated_fields = ["ac.bbd_qyxx_id", "company_name", "associated_name", "associated_id", "associated_degree",
                         "invest_ratio", "relation"]
    associated_df_a = ss_sql(entry, f"""
        SELECT
            {', '.join(associated_fields)}
        FROM all_company_table ac
        join 
        (select distinct bbd_qyxx_id from pj.ms_ipo_company_a ) b 
        on ac.bbd_qyxx_id = b.bbd_qyxx_id
        """)
    associated_df_h = ss_sql(entry, f"""
        SELECT
            {', '.join(associated_fields)}
        FROM all_company_table ac
        join 
        (select distinct bbd_qyxx_id from pj.ms_ipo_company_h ) b 
        on ac.bbd_qyxx_id  = b.bbd_qyxx_id
        """)
    associated_df = associated_df_a.union(associated_df_h).dropDuplicates()
    get_associated_degree_name_udf = udf(get_associated_degree_name, StringType())
    # 将关联度数转为中文
    associated_new_df = associated_df.withColumn("associated_degree_name",
                                                 get_associated_degree_name_udf(associated_df.associated_degree))
    associated_new_df.createOrReplaceTempView("associated_new_table")

    associated_all_fields = ["bbd_qyxx_id", "company_name", "associated_name", "associated_id", "associated_degree",
                             "invest_ratio", "relation", "associated_degree_name"]
    # 将数据存入上市公司关联名单表(associated_id包含所有上市公司bbd_qyxx_id，已验证)
    now_day = entry.version
    ss_sql(entry, f"""
        insert overwrite table
            pj.ms_ipo_company_related_party_new_test partition(dt='{now_day}')        SELECT
            {', '.join(associated_all_fields)}
        FROM associated_new_table
        """)
