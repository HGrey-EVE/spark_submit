#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 智慧楼宇企业名录
:Author: zhouchao@bbdservice.com
:Date: 2021-05-06 16:08
"""
import datetime
import time
from whetstone.core.entry import Entry
from zhly.proj_common.hive_util import HiveUtil
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as fun, SparkSession, DataFrame
from zhly.proj_common.log_track_util import log_track

from zhly.proj_common.date_util import DateUtils


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    spark: SparkSession = entry.spark

    # 项目共有配置
    hive_table = entry.cfg_mgr.get("hive", "database")
    out_path = entry.cfg_mgr.hdfs.get_input_path("zhly", "ly_company_path")
    entry.logger.info(f"配置信息hive_db:{hive_table}, hdfs_output_path:{out_path}")

    # 获取相关表最新dt
    qyxx_tag_dt = HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_tag")
    qyxx_tag_white_dt = HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_tag_white")
    qyxx_tag_black_dt = HiveUtil.newest_partition(spark, f"{hive_table}.qyxx_tag_black")
    entry.logger.info(f"qyxx_tag:{qyxx_tag_dt}, qyxx_tag_white:{qyxx_tag_white_dt}, "
                      f"qyxx_tag_black:{qyxx_tag_black_dt}")
    res_path = out_path

    # 获取名录信息
    entry.logger.info("获取名录信息")
    df_res = get_company_data(entry, spark, hive_table, qyxx_tag_dt, qyxx_tag_white_dt, qyxx_tag_black_dt)

    # 写入hdfs
    df_res.repartition(10).write.parquet(res_path, mode='overwrite')
    entry.logger.info(f"写入hdfs成功路径:{res_path}")



def get_company_data(entry, spark, hive_table, qyxx_tag_dt, qyxx_tag_white_dt, qyxx_tag_black_dt):
    bbd_table = 'index_data'
    bbd_type = 'index_data'
    bbd_uptime = int(time.time())
    bbd_dotime = time.strftime("%Y-%m-%d", time.localtime())
    # 获取tag表里楼宇数据
    df_tag = spark.sql(f"""
            select bbd_qyxx_id, company_name, 
                    '{bbd_table}' as bbd_table, '{bbd_type}' as bbd_type,
                    '{bbd_uptime}' as bbd_uptime, '{bbd_dotime}' as bbd_dotime
              from {hive_table}.qyxx_tag
            where tag like 'TX智慧楼宇%'
              and dt={qyxx_tag_dt}
        """)
    entry.logger.info(f'获取tag表中数据完成,共{df_tag.count()}家')
    # 获取tag_white表里楼宇数据
    df_tag_white = spark.sql(f"""
                select bbd_qyxx_id, company_name,
                        '{bbd_table}' as bbd_table, '{bbd_type}'as bbd_type,
                    '{bbd_uptime}' as bbd_uptime, '{bbd_dotime}' as bbd_dotime
                  from {hive_table}.qyxx_tag_white
                where tag like 'TX智慧楼宇%'
                  and dt={qyxx_tag_white_dt}
            """)
    entry.logger.info(f'获取tag_white表中数据完成,共{df_tag_white.count()}家')
    # 获取tag_black表里楼宇数据
    df_tag_black = spark.sql(f"""
                select bbd_qyxx_id, company_name,
                    '{bbd_table}' as bbd_table, '{bbd_type}' as bbd_type,
                    '{bbd_uptime}' as bbd_uptime, '{bbd_dotime}' as bbd_dotime
                  from {hive_table}.qyxx_tag_black
                where tag like 'TX智慧楼宇%'
                  and dt={qyxx_tag_black_dt}
            """)
    entry.logger.info(f'获取tag_black表中数据完成,共{df_tag_black.count()}家')

    df_res = df_tag.union(df_tag_white).distinct().subtract(df_tag_black)
    entry.logger.info(f'楼宇企业共{df_res.count()}家')

    return df_res


def post_check(entry: Entry):
    return True
