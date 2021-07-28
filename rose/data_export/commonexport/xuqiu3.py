# encoding:utf-8
__author__ = 'houguanyu'

from whetstone.core.entry import Entry
from ..proj_common.hive_util import HiveUtil
from datetime import date
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import io

'''
项目：临时导数据，数字资本对传统资本的挤出效应
数据源：dw数据库表 

'''


def pre_check(entry: Entry):
    return True


def xuqiu3_1(entry: Entry, projList):
    spark = entry.spark
    projList.createOrReplaceTempView('projects')
    xuqiu3_1 = spark.sql('''
    with cr as
    (select *
    from dw.risk_punishment
    where dt = {})
    
    select cr.* , keyword 
    from cr join 
        (select `value` keyword from projects) a
    where locate(keyword ,punish_content)>0 or locate(keyword ,punish_fact)>0
    '''.format(HiveUtil.newest_partition_with_data(spark, "dw.risk_punishment")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu3_1_data")
    xuqiu3_1.repartition(1).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')


def xuqiu3_2(entry: Entry, projList):
    spark = entry.spark
    projList.createOrReplaceTempView('projects')
    xuqiu3_2 = spark.sql('''
    with cr as
    (select *
    from dw.legal_adjudicative_documents
    where dt = {})

    select cr.* , keyword 
    from cr join 
        (select `value` keyword from projects) a
    where locate(keyword ,notice_content)>0
    '''.format(HiveUtil.newest_partition_with_data(spark, "dw.legal_adjudicative_documents")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu3_2_data")
    xuqiu3_2.repartition(200).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')

def xuqiu3_3(entry: Entry, projList):
    spark = entry.spark
    projList.createOrReplaceTempView('projects')
    xuqiu3_3 = spark.sql('''
    with cr as
    (select *
    from dw.ktgg
    where dt = {})

    select cr.* , keyword 
    from cr join 
        (select `value` keyword from projects) a
    where locate(keyword ,main)>0
    '''.format(HiveUtil.newest_partition_with_data(spark, "dw.ktgg")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu3_3_data")
    xuqiu3_3.repartition(10).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')

def xuqiu3_4(entry: Entry, projList):
    spark = entry.spark
    projList.createOrReplaceTempView('projects')
    xuqiu3_4 = spark.sql('''
    with cr as
    (select *
    from dw.legal_court_notice
    where dt = {})

    select cr.* , keyword 
    from cr join 
        (select `value` keyword from projects) a
    where locate(keyword ,notice_content)>0
    '''.format(HiveUtil.newest_partition_with_data(spark, "dw.legal_court_notice")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu3_4_data")
    xuqiu3_4.repartition(10).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')

def main(entry: Entry):
    """
       程序主入口,配置初始化和业务逻辑入口 newest_partition_with_data
       """
    entry.logger.info(entry.cfg_mgr.get("spark-submit-opt", "queue"))
    entry.logger.info("start")
    spark = entry.spark
    lst = spark.read.text(r'/user/commondataexport/hougy/projList')
    # entry.logger.info(f"开始执行 xuqiu3_1数据")
    # xuqiu3_1(entry, lst)
    # entry.logger.info(f"开始执行 xuqiu3_2数据")
    # xuqiu3_2(entry, lst)
    # entry.logger.info(f"开始执行 xuqiu3_3数据")
    # xuqiu3_3(entry, lst)
    entry.logger.info(f"开始执行 xuqiu3_4数据")
    xuqiu3_4(entry, lst)
    entry.logger.info("finish")


def post_check(entry: Entry):
    return True


