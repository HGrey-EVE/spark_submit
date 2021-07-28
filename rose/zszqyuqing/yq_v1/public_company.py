# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/7/20 13:32
"""

import os
import re
from typing import Text

# 由于MD5模块在python3中被移除，在python3中使用hashlib模块进行md5操作
from hashlib import md5
from datetime import date, datetime, timedelta

from whetstone.core.entry import Entry
from zszqyuqing.proj_common.hive_util import HiveUtil

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark import SparkConf, Row
from pyspark.sql import SparkSession, DataFrame

# DWH_NAME = "dw"
# SPARK_APP_NAME = "zszqyuqing_public_company"
# 测试环境
# OUTPUT_HDFS_PATH = "hdfs:///tmp/thk/zentao16226"
# 测试环境外管局名单存放路径
# ORIGINAL_LIS_PATH = "hdfs:///tmp/thk/data/zentao16226/"
# 生产环境所有上市公司名单存放路径
# ALL_PUBLIC_COMPANY_PATH = "hdfs:///user/zszqyuqing/zszqyuqing/yq_v1/data"


# OUTPUT_HDFS_PARTITION_NUMS = 1
DISPLAY_SQL = True


# 执行sql得到DataFrame
def ss_sql(entry: Entry, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        entry.logger.info(sql)
    df = entry.spark.sql(sql)
    entry.logger.info(df.show())
    return df


# 获取所有上市公司名单
def get_pub_comp_list(entry: Entry, filename) -> DataFrame:
    ALL_PUBLIC_COMPANY_PATH = entry.cfg_mgr.hdfs.get_fixed_path('hdfs', 'hdfs_input_path')
    ALL_PUBLIC_COMPANY_PATH = os.path.join(ALL_PUBLIC_COMPANY_PATH, filename)
    entry.logger.info(ALL_PUBLIC_COMPANY_PATH)
    df = entry.spark.read.csv(path=f"{ALL_PUBLIC_COMPANY_PATH}", header=True)
    # linerow = lines.rdd.map(lambda l: Row(company_name=l))
    # df = entry.spark.createDataFrame(linerow)
    # entry.logger.info(df.show())
    return df


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


# 获取前1天或N天的日期，beforeOfDay=1：前1天；beforeOfDay=N：前N天
def getdate(beforeOfDay):
    today = datetime.now()
    # 计算偏移量
    offset = timedelta(days=-beforeOfDay)
    # 获取想要的日期的时间
    re_date = (today + offset).strftime('%Y%m%d')
    return re_date


def newest_dt(table, entry: Entry, dw="dw"):
    return HiveUtil.newest_partition(entry.spark, table_name=f"{dw}.{table}")


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    # 获取所有上市公司名单,a是A股公司，h是H股公司
    a_comp_df = get_pub_comp_list(entry, "a_company.txt")
    a_comp_df.createOrReplaceTempView("a_comp_table")

    h_comp_df = get_pub_comp_list(entry, "h_company.txt")
    h_comp_df.createOrReplaceTempView("h_comp_table")

    # 获取所有上市公司bbd_qyxx_id
    a_comp_qyxx_id_df = ss_sql(entry, f"""
        SELECT
            distinct qb.bbd_qyxx_id,
            qb.company_name
        FROM dw.qyxx_basic qb
        join
        (select 
        regexp_replace(cast(company_name as string),'[|]', '') as company_name 
        from  a_comp_table 
        ) b 
        on qb.company_name = b.company_name
        and qb.dt={newest_dt(table='qyxx_basic', entry=entry)}
    """)
    a_comp_qyxx_id_df.createOrReplaceTempView("a_comp_qyxx_id_table")

    h_comp_qyxx_id_df = ss_sql(entry, f"""
        SELECT
            distinct qb.bbd_qyxx_id,
            qb.company_name
        FROM dw.qyxx_basic qb
        join 
        (select 
        regexp_replace(cast(company_name as string),'[|]', '') as company_name 
        from  h_comp_table 
        ) b 
        on qb.company_name =b.company_name
        and qb.dt={newest_dt(table='qyxx_basic', entry=entry)} 
    """)
    h_comp_qyxx_id_df.createOrReplaceTempView("h_comp_qyxx_id_table")

    # 将今天时间格式化成指定形式
    today = entry.version
    # 将数据存入上市公司关联名单表
    ss_sql(entry, f"""
        insert overwrite table
            pj.ms_ipo_company_a_test partition(dt={today})
        SELECT
            bbd_qyxx_id,
            company_name
        FROM a_comp_qyxx_id_table
    """)
    ss_sql(entry, f"""
        insert overwrite table
            pj.ms_ipo_company_h_test partition(dt={today})
        SELECT
            bbd_qyxx_id,
            company_name
        FROM h_comp_qyxx_id_table
    """)

    # 处理小文件,上市公司数据只需要运行一次
    os.system(
        f"hive -e 'alter table pj.ms_ipo_company_a_test partition (dt={newest_dt(table='ms_ipo_company_a', entry=entry, dw='pj')}) concatenate' ")
    os.system(
        f"hive -e 'alter table pj.ms_ipo_company_h_test partition (dt={newest_dt(table='ms_ipo_company_h', entry=entry, dw='pj')}) concatenate' ")
