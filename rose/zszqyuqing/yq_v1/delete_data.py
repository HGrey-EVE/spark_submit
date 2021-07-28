# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/7/22 15:42
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

DISPLAY_SQL = True


# 获取前1天或N天的日期，beforeOfDay=1：前1天；beforeOfDay=N：前N天
def getdate(beforeOfDay):
    today = datetime.now()
    # 计算偏移量
    offset = timedelta(days=-beforeOfDay)
    # 获取想要的日期的时间
    re_date = (today + offset).strftime('%Y%m%d')
    return re_date


# 执行sql得到DataFrame
def ss_sql(entry: Entry, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        entry.logger.info(sql)
    return entry.spark.sql(sql)


# 获取数仓表的最新分区日期
def newest_dt(table, entry: Entry, dw="dw"):
    return HiveUtil.newest_partition(entry.spark, table_name=f"{dw}.{table}")

def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    # 得到7天前日期
    olddate7 = getdate(7)
    # 删除7天前分区数据
    ss_sql(entry, f"alter table pj.ms_ipo_company_a drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.ms_ipo_company_h drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.ms_ipo_raw_event_new drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.ms_ipo_raw_event_main drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.ms_ipo_public_sentiment_events_new drop if exists partition(dt={olddate7}) ")

    ss_sql(entry, f"alter table pj.gg_event drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.gg_event_result drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.yq_event drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.yq_event_main drop if exists partition(dt={olddate7}) ")
    ss_sql(entry, f"alter table pj.yq_event_result drop if exists partition(dt={olddate7}) ")

    # 得到21天前日期
    olddate21 = getdate(21)
    ss_sql(entry, f"alter table pj.ms_ipo_company_related_party_new drop if exists partition(dt={olddate21}) ")

    # 处理小文件
    os.system(
        f"hive -e 'alter table pj.ms_ipo_raw_event_main partition (dt={newest_dt(entry=entry, table='ms_ipo_raw_event_main', dw='pj')}) concatenate' ")
    # 处理小文件
    os.system(
        f"hive -e 'alter table pj.ms_ipo_public_sentiment_events_new partition (dt={newest_dt(table='ms_ipo_public_sentiment_events_new', entry=entry, dw='pj')}) concatenate' ")
