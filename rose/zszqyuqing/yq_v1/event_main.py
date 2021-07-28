# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 招商证券舆情-v1模块迁移

Date: 2021/7/20 11:15
"""

import os
import re
from typing import Text
from datetime import date
from zszqyuqing.proj_common.hive_util import HiveUtil
from whetstone.core.entry import Entry

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark import SparkConf, Row
from pyspark.sql import SparkSession, DataFrame

DWH_NAME = "dw"
SPARK_APP_NAME = "zszqyuqing_event_main"
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
        print(sql)
    return entry.spark.sql(sql)


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    database = entry.cfg_mgr.get('hive-database', 'database')
    today = entry.version
    new_dt = HiveUtil.newest_partition(spark=entry.spark, table_name=f'{database}.ms_ipo_raw_event_new')
    # 读取pj.ms_ipo_raw_event_new表
    event_fields = ["id", "event_subject", "subject_id", "event_object", "object_id", "event_type", "bbd_url",
                    "bbd_unique_id", "search_id", "regexp_replace(main,',','，') main"]
    ss_sql(entry, f"""
        SELECT
            {', '.join(event_fields)}
        FROM pj.ms_ipo_raw_event_new
        where dt='{new_dt}'
    """).createOrReplaceTempView("event_table")

    entry.spark.sql(f"""
        select
            id,
            event_subject,
            subject_id,
            event_object,
            object_id,
            event_type,
            bbd_url,
            bbd_unique_id,
            cast(split(search_id,'_')[0] as int) as search_id,
            substring(main,2,length(main)-2) main
        from event_table
    """).createOrReplaceTempView("event_table_2")

    # 将今天时间格式化成指定形式
    # today = date.today().strftime("%Y%m%d")
    # 存入hive
    entry.spark.sql(f"""
        insert overwrite table
            pj.ms_ipo_raw_event_main partition(dt={today})
        select
            id,
            event_subject,
            subject_id,
            event_object,
            object_id,
            event_type,
            bbd_url,
            bbd_unique_id,
            search_id,
            split(main,'[？。！!?]')[search_id-1] as main
        from event_table_2
    """)

# if __name__ == "__main__":
#     ss = get_spark_session()
#     main(ss)
