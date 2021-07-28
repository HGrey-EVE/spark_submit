#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: tanghuakui@bbdservice.com
:Date: 2021-04-09 16:08
"""
import os
from datetime import date
from pyspark.sql import SparkSession, DataFrame, Row
from whetstone.core.entry import Entry
from typing import Text
import sys
from whetstone.utils.utils_hive import HiveUtils
from pyspark.sql.types import StringType, IntegerType,StructType,StructField
from yunjian.proj_common.hbase_util import HbaseWriteHelper

DISPLAY_SQL = True
# 将今天时间格式化成指定形式
today = date.today().strftime("%Y%m%d")
dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(dir))

def pre_check(entry: Entry):
    return True

def ss_sql(entry,ss: SparkSession, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        entry.logger.info("当前sql--" + sql)
    return ss.sql(sql)

def get_index_df(entry,ss,hive_db,table_info) -> DataFrame:
    table = table_info[0]
    index_name = table_info[1]
    flag = table_info[2]
    max_dt = HiveUtils.newest_partition(ss, f"{hive_db}.{table}")

    if flag==1:
        tmp_df = ss_sql(entry, ss, f"""
            SELECT
                bbd_qyxx_id,
                {index_name} as index_name,
                count(*) over(partition by bbd_qyxx_id) cnt
            FROM {hive_db}.{table}
            WHERE dt={max_dt}
                and bbd_qyxx_id is not null
                and bbd_qyxx_id !='NULL'
            """).dropDuplicates()
    else:
        if len(table_info) == 4:
            condition = table_info[3]
        else:
            condition = ''
        tmp_df = ss_sql(entry,ss,f"""
            SELECT
                bbd_qyxx_id,
                {index_name} as index_name,
                size(collect_set(bbd_xgxx_id) over(partition by bbd_qyxx_id)) cnt
            FROM {hive_db}.{table}
            WHERE dt={max_dt}
                and bbd_qyxx_id is not null
                and bbd_qyxx_id !='NULL'
                {condition}
            """).dropDuplicates()

    return tmp_df

def get_many_to_one(entry,ss,table_info) -> DataFrame:
    # 将多行转为一行，并将index_name与cnt进行拼接
    tmp_cnt_df = ss_sql(entry, ss, f"""
        SELECT
          bbd_qyxx_id as rowkey,
          collect_list(concat(concat('\"',index_name,'\"'),':',concat('\"',cnt,'\"'))) as cnt
        FROM {table_info}
        GROUP BY bbd_qyxx_id
        """)
    tmp_cnt_df.createOrReplaceTempView("tmp_cnt")

    # 将cnt由数组转为json格式
    result_df = ss_sql(entry, ss, """
        SELECT
          rowkey,
          concat('{',concat_ws(',',cnt),'}') as cnt
        FROM tmp_cnt
        """)
    return result_df

def main(entry: Entry):
    hive_db = entry.cfg_mgr.get('hive', 'database')
    out_path = entry.cfg_mgr.get('hdfs', 'hdfs_output_path')

    ss:SparkSession = entry.spark

    # (表名，指标名,标志,[条件]),其中标志1代表不根据bbd_xgxx_id去重，2代表需要去重
    get_index_list = (
        ('qyxx_sharesimpawn', "'sharesimpawn'",1),
        ('qyxx_liquidation', "'liquidation'",1),
        ('qyxx_jyyc', "'jyyc'",1),
        ('company_illegal_info', "'yzwf'", 2),
        ('company_mortgage_info', "'mortgage'", 2),
        ('xzcf', "'xzcf'", 2),
        ('ktgg', "'ktgg'", 2),
        ('legal_adjudicative_documents', "'zgcpwsw'", 2,
         "and source_key in ('accuser','defendant')"),
        ('company_send_announcement', "'sdgg'", 2),
        ('company_court_register', "'laxx'", 2),
        ('legal_court_notice', "'rmfygg'", 2),
        ('legal_persons_subject_to_enforcement', "'zhixing'", 2,
         "and source_key in ('pname','pname_origin')"),
        ('legal_dishonest_persons_subject_to_enforcement', "'dishonesty'", 2,
         "and source_key in ('pname','pname_origin')")
    )

    entry.logger.info("计算指标开始......")
    # 指标计算
    index_list = [get_index_df(entry,ss, hive_db, i) for i in get_index_list]
    entry.logger.info("计算指标完成......")

    # 创建一个空dataframe
    schema = StructType([
        StructField("bbd_qyxx_id", StringType(), True),
        StructField("index_name", StringType(), True),
        StructField("cnt", StringType(), True)])
    dfresult = ss.createDataFrame(ss.sparkContext.emptyRDD(), schema)

    entry.logger.info("数据union开始......")
    for i in index_list:
        dfresult = dfresult.union(i)
    entry.logger.info("数据union完成......")
    dfresult.createOrReplaceTempView("df_table")

    # 数据处理，多行转一行等
    dfresult = get_many_to_one(entry,ss,'df_table')

    index_8_path = f"{out_path}/index_8/{today}"

    # 数据保存入hive
    dfresult.createOrReplaceTempView("index_8_tmp")
    ss_sql(entry,ss, f"insert overwrite table yunjian.index_8 partition(dt={today}) select rowkey,cnt from index_8_tmp")
    entry.logger.info("数据写入hdfs开始......")
    os.system("hadoop fs -rm -r " + index_8_path)
    dfresult.repartition(100).write.option("delimiter", "|").option('escapeQuotes', False).csv(index_8_path)
    entry.logger.info("数据写入hdfs完成......")

    entry.logger.info("将数据写入HBASE开始......")
    # 将数据写入hbase
    table_name = 'yunjian:person_company_risk_' + today
    hfile_absolute_path = f"{out_path}/index_8/h_file/"
    source_data_absolute_path = f"{out_path}/index_8/" + today
    hbase_write_helper = HbaseWriteHelper(
        entry.logger,
        table_name,
        'info',
        None,
        hfile_absolute_path=hfile_absolute_path,
        source_data_absolute_path=source_data_absolute_path,
        source_csv_delimiter='|',
        hbase_columns='HBASE_ROW_KEY,info:cnt',
        meta_table_name='yunjian:api_meta',
        meta_row='person_company_risk_address_v1',
        meta_shell_path='')
    hbase_write_helper.exec_write()
    entry.logger.info("将数据写入HBASE完成......")

def post_check(entry: Entry):
    return True