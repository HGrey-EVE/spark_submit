#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 产业金融服务平台
:Author: lirenchao@bbdservice.com
:Date: 22021-06-11 15:54
"""
import os
import datetime
from pyspark import StorageLevel
from whetstone.core.entry import Entry
from jingxinting.proj_common.date_util import DateUtils
from jingxinting.proj_common.data_output_util import ResultOutputUtil
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy


def pre_check(entry: Entry):
    return True


def extract_basic_data(entry: Entry):
    """
    抽取基础数据
    :param entry:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    version = entry.version  # 202106
    # 上月月份
    str2date = datetime.datetime.strptime(version, "%Y%m")
    last_month = DateUtils.add_date_2str(date_time=str2date, fmt="%Y%m", months=-1)
    # 经信厅上月依赖数据HDFS根目录
    last_dep_data_path = os.path.join(entry.cfg_mgr.get("hdfs", "hdfs_root_path"), last_month, "input/basic_data/dependency_data")
    # 经信厅本月依赖数据HDFS根目录
    dep_data_path = os.path.join(entry.cfg_mgr.hdfs.get_input_path('hdfs-biz', 'path_basic_data'), "dependency_data")
    # 阿拉丁分数上月存储路径
    last_alandding_data_path = os.path.join(last_dep_data_path, "SC_JINGXIN_company_aladding_score.csv")
    # 阿拉丁分数本月存储路径
    alandding_data_path = os.path.join(dep_data_path, "SC_JINGXIN_company_aladding_score.csv")
    basic_data_helper = BasicDataProxy(entry=entry)
    try:
        basic_data_helper.get_qyxx_basic_df() \
            .select("bbd_qyxx_id", "company_name", "city_code", "company_industry")\
            .where("company_enterprise_status='存续'")\
            .persist(StorageLevel.MEMORY_AND_DISK) \
            .createOrReplaceTempView("basic_data")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error("Failed to load the basic data of the current month!")
        traceback.print_exc()
        raise e
    try:
        spark.read\
            .csv(last_alandding_data_path, header=True)\
            .select("bbd_qyxx_id", "total_score")\
            .fillna(0)\
            .createOrReplaceTempView("last_alandding_socre")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to read alandding score data last month located {last_alandding_data_path}!")
        traceback.print_exc()
        raise e
    try:
        spark.read\
            .csv(alandding_data_path, header=True)\
            .fillna(0)\
            .persist(StorageLevel.MEMORY_AND_DISK)\
            .createOrReplaceTempView("alandding_socre")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to read alandding score data this month located {alandding_data_path}!")
        traceback.print_exc()
        raise e


def calculate_index(entry: Entry):
    """
    计算产业金融服务平台相关指标
    :param entry:
    :return:
    """
    spark = entry.spark
    logger = entry.logger

    logger.info("============industry finance start calculating indexes============")
    logger.info("Industry finance is calculating indexes...")

    # 企业综合实力评估
    spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            CASE WHEN total_score < 500 THEN '极差'
                    WHEN 500 <= total_score AND total_score < 530 THEN '较差'
                    WHEN 530 <= total_score AND total_score < 560 THEN '一般'
                    WHEN 560 <= total_score AND total_score < 580 THEN '较好'
                    ELSE '极好' END zhsl_evaluation
        FROM alandding_socre
        '''
    ).createOrReplaceTempView("zhsl_evaluation")

    # 行业排名
    spark.sql(
        '''
        SELECT
            b.bbd_qyxx_id,
            CASE WHEN b.industry_rank < 5 THEN '前5%'
                    WHEN 5 <= b.industry_rank AND b.industry_rank < 10 THEN '前10%'
                    WHEN 10 <= b.industry_rank AND b.industry_rank < 20 THEN '前20%'
                    WHEN 20 <= b.industry_rank AND b.industry_rank < 30 THEN '前30%'
                    WHEN 30 <= b.industry_rank AND b.industry_rank < 40 THEN '前40%'
                    WHEN 40 <= b.industry_rank AND b.industry_rank < 50 THEN '前50%'
                    WHEN 50 <= b.industry_rank AND b.industry_rank < 60 THEN '前60%'
                    WHEN 60 <= b.industry_rank AND b.industry_rank < 70 THEN '前70%'
                    WHEN 70 <= b.industry_rank AND b.industry_rank < 80 THEN '前80%'
                    WHEN 80 <= b.industry_rank AND b.industry_rank < 90 THEN '前90%'
                    ELSE '前100%' END industry_rank
        FROM(
            SELECT
                a.bbd_qyxx_id,
                a.industry,
                (a.rank / COUNT (1) OVER(PARTITION BY a.industry)) * 100 AS industry_rank
            FROM(
                SELECT
                    bbd_qyxx_id,
                    industry,
                    RANK() OVER(PARTITION BY industry ORDER BY total_score DESC) rank
                FROM alandding_socre
                ) a
            ) b
        
        '''
    ).createOrReplaceTempView("industry_rank")

    # 发展趋势
    spark.sql(
        '''
        SELECT
            a.bbd_qyxx_id,
            CASE WHEN 0 < (a.total_score - b.total_score) THEN '上升'
                    WHEN a.total_score = b.total_score THEN '平稳'
                    ELSE '下降' END dev_trend
        FROM alandding_socre a
        LEFT JOIN last_alandding_socre b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        '''
    ).createOrReplaceTempView("dev_trend")

    # 地区排名
    spark.sql(
        '''
        SELECT
            e.bbd_qyxx_id,
            CASE WHEN e.region_rank < 5 THEN '前5%'
                    WHEN 5 <= e.region_rank AND e.region_rank < 10 THEN '前10%'
                    WHEN 10 <= e.region_rank AND e.region_rank < 20 THEN '前20%'
                    WHEN 20 <= e.region_rank AND e.region_rank < 30 THEN '前30%'
                    WHEN 30 <= e.region_rank AND e.region_rank < 40 THEN '前40%'
                    WHEN 40 <= e.region_rank AND e.region_rank < 50 THEN '前50%'
                    WHEN 50 <= e.region_rank AND e.region_rank < 60 THEN '前60%'
                    WHEN 60 <= e.region_rank AND e.region_rank < 70 THEN '前70%'
                    WHEN 70 <= e.region_rank AND e.region_rank < 80 THEN '前80%'
                    WHEN 80 <= e.region_rank AND e.region_rank < 90 THEN '前90%'
                    ELSE '前100%' END region_rank
        FROM(
            SELECT
                d.bbd_qyxx_id,
                (d.rank / COUNT (1) OVER(PARTITION BY d.city_code)) * 100 AS region_rank
            FROM(
                SELECT
                    c.bbd_qyxx_id,
                    c.city_code,
                    RANK() OVER(PARTITION BY c.city_code ORDER BY c.total_score DESC) rank
                FROM(
                    SELECT
                        a.bbd_qyxx_id,
                        b.city_code,
                        a.total_score
                    FROM alandding_socre a
                    LEFT JOIN basic_data b
                    ON a.bbd_qyxx_id=b.bbd_qyxx_id
                    ) c
                WHERE c.city_code IS NOT NULL
                ) d
            ) e
        '''
    ).createOrReplaceTempView("region_rank")

    # 合并指标
    industry_finance = spark.sql(
        '''
        SELECT 
            {STATIC_MONTH} as static_month,
            t1.bbd_qyxx_id,
            t1.company_name,
            t1.company_industry,
            t1.city_code,
            t2.total_score AS total_score,
            t2.dtglf_M AS dtglf_score,
            t2.jtglf_M AS jtglf_score,
            t2.qycx_M AS qycx_score,
            t2.qyfz_M AS qyfz_score,
            t2.qyjy_M AS qyjy_score,
            t2.zhsl_M AS zhsl_score,
            t3.zhsl_evaluation,
            t4.industry_rank,
            t5.dev_trend,
            t6.region_rank
        FROM basic_data t1
        JOIN alandding_socre t2
        ON t1.bbd_qyxx_id=t2.bbd_qyxx_id
        JOIN zhsl_evaluation t3
        ON t1.bbd_qyxx_id=t3.bbd_qyxx_id
        JOIN industry_rank t4
        ON t1.bbd_qyxx_id=t4.bbd_qyxx_id
        JOIN dev_trend t5
        ON t1.bbd_qyxx_id=t5.bbd_qyxx_id
        JOIN region_rank t6
        ON t1.bbd_qyxx_id=t6.bbd_qyxx_id
        '''.format(STATIC_MONTH=entry.version)
    ).fillna(0).repartition(1)

    # 结果输出到HDFS
    # 经信厅HDFS暂存目录
    hdfs_temp_path = os.path.join(entry.cfg_mgr.hdfs.get_tmp_path('hdfs-biz', 'path_basic_data'), "industry_finance")
    try:
        industry_finance.write.csv(os.path.join(hdfs_temp_path, "result"), header=True, mode="overwrite")
        logger.info(f"Industry finance has written index results to {os.path.join(hdfs_temp_path, 'result')}!")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to load the temporary data to the {hdfs_temp_path} directory!")
        traceback.print_exc()
        raise e

    output_hdfs_path = os.path.join(entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'path_result'), "monthly_industry_finance")
    out_put_util = ResultOutputUtil(entry, table_name="monthly_industry_finance")

    try:
        out_put_util.save(industry_finance)
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to load the result data to the {output_hdfs_path} directory!")
        traceback.print_exc()
        raise e

    logger.info(f"Industry finance has written index results to {output_hdfs_path}!")

    # 删除暂存数据
    delete_cmd = "hdfs dfs -rm -r -f " + hdfs_temp_path
    if os.system(delete_cmd) != 0:
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to delete the temporary data from the {hdfs_temp_path} directory!")
        raise Exception("暂存目录删除失败！")
    else:
        logger.info(f"The temporary data on the {hdfs_temp_path} directory has been deleted!")
    logger.info("============industry finance indexes has been calculated============")


def main(entry: Entry):
    # 抽取基础数据
    extract_basic_data(entry)
    # 计算园区分析相关指标
    calculate_index(entry)


def post_check(entry: Entry):
    return True
