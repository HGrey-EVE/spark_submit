#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 园区分析
:Author: lirenchao@bbdservice.com
:Date: 22021-06-02 17:18
"""
import os
import datetime
from whetstone.core.entry import Entry
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame, Row
from jingxinting.proj_common.date_util import DateUtils
from jingxinting.proj_common.hive_util import HiveUtil
from jingxinting.proj_common.json_util import JsonUtils
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy
from jingxinting.proj_common.data_output_util import ResultOutputUtil


def pre_check(entry: Entry):
    return True


def map_parse(row):
    """
    行解析
    :param row:
    :return:
    """
    row_dict = row.asDict()
    return (row_dict['park_id'], row)


def industry_distribution_parse(rows_iter):
    """
    解析园区产业分布
    :param rows_iter:
    :return:
    """
    ret_list = list()
    rows = list(rows_iter)
    park_id = rows[0]['park_id']
    for row in rows:
        ret_dict = dict()
        ret_dict['company_industry'] = row['company_industry']
        ret_dict['industry_enterprise_num'] = row['industry_enterprise_num']
        ret_dict['industry_per_enterprise'] = row['industry_per_enterprise']
        ret_list.append(ret_dict)

    return {'park_id': park_id, 'industry_distribution': JsonUtils.to_string(ret_list)}


def extract_basic_data(entry: Entry):
    """
    抽取基础数据
    :param entry:
    :return:
    """
    logger = entry.logger
    basic_data_helper = BasicDataProxy(entry=entry)
    try:
        basic_data_helper.get_qyxx_basic_df()\
            .where("park_id is not null and company_enterprise_status='存续'")\
            .select("bbd_qyxx_id", "company_industry", "park_id", "key_enterprise", "unicorns", "gazelle")\
            .persist(StorageLevel.MEMORY_AND_DISK)\
            .createOrReplaceTempView("basic_data")
        basic_data_helper.get_patent_info_df()\
            .select("bbd_qyxx_id", "patent_code", "patent_type", "publidate")\
            .persist(StorageLevel.MEMORY_AND_DISK)\
            .createOrReplaceTempView("patent_info")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error("Failed to load the basic data!")
        traceback.print_exc()
        raise e


def calculate_index(entry: Entry):
    spark = entry.spark
    logger = entry.logger
    version = entry.version  # 202106
    year = version[:4]
    month = version[4:6]
    # 计算发生月份
    str2date = datetime.datetime.strptime(version, "%Y%m")
    current_month = DateUtils.add_date_2str(date_time=str2date, fmt="%Y%m", months=1)
    # 统计开始月份
    start_static_month = year + "-01"
    # 统计截止月份
    end_static_month = year + "-" + month
    # 经信厅HDFS暂存目录
    # hdfs_temp_path = os.path.join(entry.cfg_mgr.hdfs.get_tmp_path('hdfs-biz', 'path_basic_data'), "park_analysis")

    logger.info("============park analysis start calculating indexes============")
    logger.info("Park analysis is calculating indexes...")

    # 各园区月度招聘数量
    recruit_num_monthly_df = spark.sql(
        '''
        SELECT 
            a.park_id, 
            SUM (b.bbd_recruit_num) AS recruit_num_monthly
        FROM basic_data a
        LEFT JOIN (
                    SELECT 
                        bbd_qyxx_id, 
                        bbd_recruit_num
                    FROM dw.manage_recruit
                    WHERE dt={DT} 
                    AND date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                ) b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        GROUP BY a.park_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_recruit', current_month),
                   CURRENT=end_static_month)
    )

    # 各园区招聘数量总数
    recruit_num_df = spark.sql(
        '''
        SELECT 
            a.park_id, 
            SUM (b.bbd_recruit_num) AS recruit_num
        FROM basic_data a
        LEFT JOIN (
                    SELECT 
                        bbd_qyxx_id, 
                        bbd_recruit_num
                    FROM dw.manage_recruit
                    WHERE dt={DT} 
                    AND '{START_MONTH}'<=date_format(pubdate, 'yyyy-MM')
                    AND date_format(pubdate, 'yyyy-MM')<='{CURRENT}'
                ) b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        GROUP BY a.park_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_recruit', current_month),
                   START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # 各园区专利数量总数
    patent_num_df = spark.sql(
        '''
        SELECT 
            a.park_id, 
            COUNT (DISTINCT b.patent_code) AS patent_num
        FROM basic_data a
        LEFT JOIN (
                    SELECT 
                        bbd_qyxx_id,
                        patent_code
                    FROM patent_info 
                    WHERE '{START_MONTH}'<=date_format(publidate, 'yyyy-MM') 
                    AND date_format(publidate, 'yyyy-MM')<='{CURRENT}'
                ) b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        GROUP BY a.park_id
        '''.format(START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # 各园区发明专利数量总数
    patent_num_invent_df = spark.sql(
        '''
        SELECT 
            a.park_id, 
            COUNT (DISTINCT b.patent_code) AS patent_num_invent
        FROM basic_data a
        LEFT JOIN (
                    SELECT 
                        bbd_qyxx_id, 
                        patent_code
                    FROM patent_info
                    WHERE patent_type='发明专利'
                    AND '{START_MONTH}'<=date_format(publidate, 'yyyy-MM')
                    AND date_format(publidate, 'yyyy-MM')<='{CURRENT}'
                ) b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        GROUP BY a.park_id
        '''.format(START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # 各园区实用专利数量总数
    patent_num_utility_df = spark.sql(
        '''
        SELECT 
            a.park_id, 
            COUNT (DISTINCT b.patent_code) AS patent_num_utility
        FROM basic_data a
        LEFT JOIN (
                    SELECT 
                        bbd_qyxx_id, 
                        patent_code
                    FROM patent_info
                    WHERE patent_type='实用新型'
                    AND '{START_MONTH}'<=date_format(publidate, 'yyyy-MM')
                    AND date_format(publidate, 'yyyy-MM')<='{CURRENT}'
                ) b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        GROUP BY a.park_id
        '''.format(START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # 各园区月度企业数量
    company_number_df = spark.sql(
        '''
        SELECT 
            park_id, 
            COUNT (DISTINCT bbd_qyxx_id) AS company_number
        FROM basic_data 
        GROUP BY park_id
        '''
    )

    # 各园区月度重点企业数量
    key_enterprise_num_df = spark.sql(
        '''
        SELECT 
            park_id, 
            COUNT (DISTINCT bbd_qyxx_id) AS key_enterprise_num
        FROM basic_data 
        WHERE key_enterprise=1
        GROUP BY park_id
        '''
    )

    # 各园区月度独角兽企业数量
    unicorns_num_df = spark.sql(
        '''
        SELECT 
            park_id, 
            COUNT (DISTINCT bbd_qyxx_id) AS unicorns_num
        FROM basic_data 
        WHERE unicorns=1
        GROUP BY park_id
        '''
    )

    # 各园区月度瞪羚企业数量
    gazelle_enterprise_num_df = spark.sql(
        '''
        SELECT 
            park_id, 
            COUNT (DISTINCT bbd_qyxx_id) AS gazelle_enterprise_num
        FROM basic_data 
        WHERE gazelle=1
        GROUP BY park_id
        '''
    )

    # 各园区月度行业分布
    # 计算园区行业分布数据
    industry_distribution_tmp_df = spark.sql(
        '''
        SELECT 
            a.park_id, 
            a.company_industry, 
            a.industry_enterprise_num, 
            a.industry_enterprise_num / b.enterprise_num AS industry_per_enterprise
        FROM(
                SELECT 
                    park_id, 
                    company_industry, 
                    COUNT (DISTINCT bbd_qyxx_id) industry_enterprise_num
                FROM basic_data
                GROUP BY park_id, company_industry
            ) a
        LEFT JOIN(
                    SELECT 
                        park_id, 
                        COUNT (DISTINCT bbd_qyxx_id) AS enterprise_num
                    FROM basic_data
                    GROUP BY park_id
                  ) b
        ON a.park_id=b.park_id
        '''
    ).fillna(0)

    # 合并园区行业分布数据
    industry_distribution_df = industry_distribution_tmp_df\
        .rdd\
        .map(map_parse)\
        .groupByKey()\
        .map(lambda x: industry_distribution_parse(x[1]))\
        .map(lambda x: Row(park_id=x['industry_distribution'], industry_distribution=x['park_id']))\
        .toDF(["park_id", "industry_distribution"])
    # 合并指标
    park_analysis = recruit_num_monthly_df.join(recruit_num_df, ['park_id'], "left") \
        .join(patent_num_df, ['park_id'], "left") \
        .join(patent_num_invent_df, ['park_id'], "left") \
        .join(patent_num_utility_df, ['park_id'], "left") \
        .join(company_number_df, ['park_id'], "left") \
        .join(key_enterprise_num_df, ['park_id'], "left") \
        .join(unicorns_num_df, ['park_id'], "left") \
        .join(gazelle_enterprise_num_df, ['park_id'], "left") \
        .join(industry_distribution_df, ['park_id'], "left") \
        .selectExpr(
        "'{STATIC_MONTH}' as static_month".format(STATIC_MONTH=version),
        "park_id",
        "recruit_num_monthly",
        "recruit_num",
        "patent_num",
        "patent_num_invent",
        "patent_num_utility",
        "company_number",
        "key_enterprise_num",
        "unicorns_num",
        "gazelle_enterprise_num",
        "industry_distribution").fillna(0).repartition(1)

    # 测试使用--查看计算结果
    # output_hdfs_path = os.path.join(hdfs_temp_path, "result")
    # park_analysis.write.csv(output_hdfs_path, header=True, mode="overwrite")
    # logger.info(f"park analysis has written all indexes to {output_hdfs_path}!")

    # 结果输出到HDFS
    out_put_util = ResultOutputUtil(entry, table_name="monthly_info_park")
    out_put_util.save(park_analysis)

    output_hdfs_path = os.path.join(entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'path_result'), "monthly_info_park")
    logger.info(f"Park analysis has written index results to {output_hdfs_path}!")
    logger.info("============park analysis indexes has been calculated============")


def main(entry: Entry):
    # 抽取基础数据
    extract_basic_data(entry)
    # 计算园区分析相关指标
    calculate_index(entry)


def post_check(entry: Entry):
    return True
