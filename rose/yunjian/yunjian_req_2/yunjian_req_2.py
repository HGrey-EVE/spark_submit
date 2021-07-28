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
dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(dir))

def pre_check(entry: Entry):
    return True

def ss_sql(entry,ss: SparkSession, sql: Text) -> DataFrame:
    if DISPLAY_SQL:
        entry.logger.info("当前sql--" + sql)
    return ss.sql(sql)

def get_index_df(entry,ss, hive_db, table_info) -> DataFrame:
    table = table_info[0]
    index_name = table_info[1]
    pubdate = table_info[2]
    flag = table_info[3]
    max_dt = HiveUtils.newest_partition(ss, f"{hive_db}.{table}")

    if flag==1:
        tmp_df = ss_sql(entry,ss,f"""
            SELECT
                bbd_qyxx_id,
                {index_name} as index_name,
                substr({pubdate}, 0,7) pubdate,
                count(*) over(partition by bbd_qyxx_id,substr({pubdate}, 0,7)) cnt
            FROM {hive_db}.{table}
            WHERE dt={max_dt}
                and bbd_qyxx_id is not null
                and bbd_qyxx_id !='NULL'
        """).dropDuplicates()
    else:
        if len(table_info) == 5:
            condition = table_info[4]
        else:
            condition = ''
        tmp_df = ss_sql(entry, ss, f"""
            SELECT
                bbd_qyxx_id,
                {index_name} as index_name,
                substr({pubdate}, 0,7) pubdate,
                size(collect_set(bbd_xgxx_id) over(partition by bbd_qyxx_id,substr({pubdate}, 0,7))) cnt
            FROM {hive_db}.{table}
            WHERE dt={max_dt}
                and bbd_qyxx_id is not null
                and bbd_qyxx_id !='NULL'
                {condition}
            """).dropDuplicates()
    tmp_df.createOrReplaceTempView("tmp")

    # 将pubdate为空的字段转为null
    tmp_basic_df = ss_sql(entry, ss, f"""
        SELECT
          bbd_qyxx_id,
          index_name,
          if(pubdate is not null,pubdate,'null') as pubdate,
          cnt
        FROM tmp
        """)
    tmp_basic_df.createOrReplaceTempView("tmp_basic")

    # 将pubdate中的'-'去掉
    tmp_pubdate_df = ss_sql(entry, ss, f"""
            SELECT
              bbd_qyxx_id,
              index_name,
              regexp_replace(pubdate, '-', '') as pubdate,
              cnt
            FROM tmp_basic
            """)
    tmp_pubdate_df.createOrReplaceTempView("tmp_pubdate")

    # 将多行转为一行，并将pubdate与cnt进行拼接
    tmp_cnt_df = ss_sql(entry, ss, f"""
            SELECT
              concat(bbd_qyxx_id,'_',index_name) as rowkey,
              collect_list(concat(concat('\"',pubdate,'\"'),':',concat('\"',cnt,'\"'))) as cnt
            FROM tmp_pubdate
            GROUP BY bbd_qyxx_id,index_name
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
    # 配置文件获取
    hive_db = entry.cfg_mgr.get("hive", "database")
    out_path = entry.cfg_mgr.get('hdfs', 'hdfs_output_path')
    # 将今天时间格式化成指定形式
    today = date.today().strftime("%Y%m%d")
    ss:SparkSession = entry.spark

    # (表名，指标名，时间字段，标志，[条件])),其中标志1代表不根据bbd_xgxx_id去重，2代表需要去重
    get_index_list = (
        ('qyxx_sharesimpawn', "'sharesimpawn'", 'imponrecdate',1),
        ('qyxx_liquidation', "'liquidation'", 'ligenddate',1),
        ('qyxx_jyyc', "'jyyc'", 'rank_date',1),
        ('qyxx_sharesfrost', "'sharesfrost'", 'frofrom',1),
        ('qyxx_xzxk', "'xzxk'", 'vaildfrom',1),
        ('company_illegal_info', "'yzwf'", 'put_date', 2),
        ('company_mortgage_info', "'mortgage'", 'reg_date', 2),
        ('xzcf', "'xzcf'", 'punish_date', 2),
        ('ktgg', "'ktgg'", 'trial_date', 2),
        ('legal_adjudicative_documents', "'zgcpwsw'", 'sentence_date', 2,
         "and source_key in ('accuser','defendant')"),
        ('company_send_announcement', "'sdgg'", 'start_date', 2),
        ('company_court_register', "'laxx'", 'filing_date', 2),
        ('legal_court_notice', "'rmfygg'", 'notice_time', 2),
        ('legal_persons_subject_to_enforcement', "'zhixing'", 'register_date', 2,
         "and source_key in ('pname','pname_origin')"),
        ('legal_dishonest_persons_subject_to_enforcement', "'dishonesty'",
         'register_date', 2,"and source_key in ('pname','pname_origin')"),
        ('company_zxr_restrict', "'zxr_restrict'", 'case_create_time', 2),
        ('company_zxr_final_case', "'final_case'", 'case_create_time', 2),
        ('risk_tax_owed', "'tax_owed'", 'tax_start', 2),
        ('prop_domain_website', "'domain'", 'approval_time', 2),
        ('xgxx_shangbiao', "'shangbiao'", 'application_date', 2)
    )

    entry.logger.info("计算指标开始......")
    # 计算指标
    index_list = [get_index_df(entry,ss,hive_db,i) for i in get_index_list]
    entry.logger.info("计算指标完成......")

    # 创建一个空dataframe
    schema = StructType([
        StructField("rowkey", StringType(), True),
        StructField("cnt", StringType(), True)])
    dfresult = ss.createDataFrame(ss.sparkContext.emptyRDD(), schema)

    entry.logger.info("数据union开始......")
    for i in index_list:
        dfresult = dfresult.union(i)
    entry.logger.info("数据union完成......")

    index_2_path = f"{out_path}/index_2/{today}"

    entry.logger.info("数据写入hdfs开始......")
    os.system("hadoop fs -rm -r " + index_2_path)
    # option('escapeQuotes', False)，关闭引号转义，如此引号就不会被转义
    dfresult.coalesce(100).write.option("delimiter", "|").option('escapeQuotes', False).csv(index_2_path)
    entry.logger.info("数据写入hdfs完成......")

    entry.logger.info("将数据写入HBASE开始......")
    # 将数据写入hbase
    table_name = 'yunjian:history_month_stat_' + today
    hfile_absolute_path = f"{out_path}/index_2/h_file/"
    source_data_absolute_path = f"{out_path}/index_2/" + today
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
        meta_row='history_month_stat_address_v1',
        meta_shell_path='')
    hbase_write_helper.exec_write()
    entry.logger.info("将数据写入HBASE完成......")

def post_check(entry: Entry):
    return True
