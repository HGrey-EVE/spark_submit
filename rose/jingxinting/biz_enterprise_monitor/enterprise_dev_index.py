#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 企业发展指数
:Author: lirenchao@bbdservice.com
:Date: 2021-06-02 17:18
"""
import os
import datetime
from whetstone.core.entry import Entry
from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession, DataFrame, Row
from jingxinting.proj_common.date_util import DateUtils
from jingxinting.proj_common.hive_util import HiveUtil
from jingxinting.proj_common.json_util import JsonUtils
from jingxinting.proj_common.data_output_util import ResultOutputUtil


def pre_check(entry: Entry):
    return True


def map_parse(row: Row):
    """
    行解析
    :param row:
    :return:
    """
    row_dict = row.asDict()
    return (row_dict['bbd_qyxx_id'], row)


def patent_distribution_parse(rows_iter):
    """
    解析企业专利分布
    :param rows_iter:
    :return:
    """
    ret_list = list()
    rows = list(rows_iter)
    bbd_qyxx_id = rows[0]['bbd_qyxx_id']

    for row in rows:
        ret_dict = dict()
        ret_dict['patent_type'] = row['patent_type']
        ret_dict['patent_num'] = row['patent_num']
        ret_list.append(ret_dict)

    return {'bbd_qyxx_id': bbd_qyxx_id, 'patent_distribution': JsonUtils.to_string(ret_list)}


def extract_basic_data(entry: Entry):
    """
    抽取基础数据
    :param entry:
    :return:
    """
    spark = entry.spark
    version = entry.version
    str2date = datetime.datetime.strptime(version, "%Y%m")
    # 计算发生月份
    current_month = DateUtils.add_date_2str(date_time=str2date, fmt="%Y%m", months=1)
    spark.sql(
        '''
        SELECT 
            bbd_qyxx_id,
            company_name,
            company_industry,
            regcap_amount
        FROM dw.qyxx_basic
        WHERE dt={DT}
        AND company_province='四川'
        AND company_enterprise_status='存续'
        AND bbd_qyxx_id is not null
        AND bbd_qyxx_id != 'null'
        AND substr(company_companytype, 1, 2) not in ('91', '92', '93')
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'qyxx_basic', current_month))
    ).persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("basic_data")


def calculate_company_index_rank_percent(spark: SparkSession, dataframe: DataFrame, index_name: str, order="ASC") -> DataFrame:
    """
    计算公司指标排名百分比
    :param spark:
    :param dataframe:
    :param index_name:
    :param order:
    :return:
    """
    basic_index_view = index_name + "_tmp"
    dataframe.createOrReplaceTempView(basic_index_view)
    # 利用基础数据表为基础指标视图添加行业列，并对指标列缺失值进行补0操作
    spark.sql(
        '''
        SELECT 
            a.bbd_qyxx_id,
            b.company_industry,
            NVL (a.{INDEX_NAME}, 0) {INDEX_NAME}
        FROM {BASIC_VIEW_NAEM} a
        LEFT JOIN basic_data b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        '''.format(INDEX_NAME=index_name,
                   BASIC_VIEW_NAEM=basic_index_view)
    ).createOrReplaceTempView(basic_index_view)
    # 分行业对指标列进行排名
    spark.sql(
        '''
        SELECT 
            bbd_qyxx_id,
            {INDEX_NAME},
            company_industry, 
            ROW_NUMBER() OVER(PARTITION BY company_industry ORDER BY {INDEX_NAME} {ORDER}) rank
        FROM {BASIC_VIEW_NAEM}
        '''.format(INDEX_NAME=index_name,
                   ORDER=order,
                   BASIC_VIEW_NAEM=basic_index_view)
    ).createOrReplaceTempView("t1")
    # 指标值相同时，排名取均值
    spark.sql(
        '''
        SELECT 
            t1.bbd_qyxx_id, 
            t1.company_industry, 
            t1.{INDEX_NAME}, 
            t2.rank
        FROM t1 
        LEFT JOIN(
                    SELECT 
                        company_industry, 
                        {INDEX_NAME}, 
                        AVG (rank) AS rank
                    FROM t1 
                    GROUP BY company_industry, {INDEX_NAME}
                ) t2 
        ON t1.company_industry = t2.company_industry AND t1.{INDEX_NAME} = t2.{INDEX_NAME}
        '''.format(INDEX_NAME=index_name)
    ).createOrReplaceTempView("t3")
    # 计算公司指标行业排名百分比
    result_dataframe = spark.sql(
        '''
        SELECT 
            DISTINCT t3.bbd_qyxx_id, 
            t3.company_industry,
            t3.{INDEX_NAME},
            t3.rank / t4.max_value AS {RANK_PERCENT_NAME}
        FROM t3 
        LEFT JOIN(
                    SELECT company_industry, MAX (rank) max_value
                    FROM t3
                    GROUP BY company_industry
                ) t4 
        ON t3.company_industry=t4.company_industry
        '''.format(INDEX_NAME=index_name,
                   RANK_PERCENT_NAME=index_name + "_rank_percent")
    )
    return result_dataframe


def company_is_listed_standard(spark: SparkSession, dataframe: DataFrame, index_name: str) -> DataFrame:
    """
    公司是否上市相关指标标准化
    :param spark:
    :param dataframe:
    :param index_name:
    :return:
    """
    basic_index_view = index_name + "_tmp"
    # 对指标列缺失值进行补0操作
    dataframe.fillna(0).createOrReplaceTempView(basic_index_view)
    # 判断相关公司是否上市
    result_df = spark.sql(
        '''
        SELECT 
            a.bbd_qyxx_id,
            CASE WHEN SUM (a.{INDEX_NAME}) > 0 THEN 1 ELSE 0 END {INDEX_NAME}
        FROM {BASIC_VIEW_NAEM} a
        GROUP BY a.bbd_qyxx_id
        '''.format(INDEX_NAME=index_name,
                   BASIC_VIEW_NAEM=basic_index_view)
    )
    return result_df


def recruit_index_standard(spark: SparkSession, part_dataframe: DataFrame, total_dataframe: DataFrame, index_type: str) -> DataFrame:
    """
    公司招聘相关指标标准化
    :param spark:
    :param part_dataframe:
    :param total_dataframe:
    :param index_type:
    :return:
    """
    part_dataframe.createOrReplaceTempView("part")
    total_dataframe.createOrReplaceTempView("total")
    # 计算各招聘指标人数占总招聘人数百分比
    result_dataframe = spark.sql(
        '''
        SELECT 
            a.bbd_qyxx_id,
            b.recruit_num / a.recruit_num {INDEX_NAME}
        FROM total a
        LEFT JOIN part b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        WHERE a.recruit_num != 0
        '''.format(INDEX_NAME=index_type + "_recruit_num_percent")
    )
    return result_dataframe


def standardization_of_business_behavior(spark: SparkSession, dataframe: DataFrame, index_name: str) -> DataFrame:
    """
    企业经营行为相关指标标准化
    :param spark:
    :param dataframe:
    :param index_name:
    :return:
    """
    dataframe.createOrReplaceTempView("tmp")
    # （1）若index_name为异常，则为0；若index_name为正常，则为1；
    # （2）按照bbd_qyxx_id聚合，若sum(index_name)>0,则输出0.5；否则输出0
    result_df = spark.sql(
        '''
        SELECT 
            a.bbd_qyxx_id, 
            CASE WHEN SUM ({INDEX_NAME}) > 0 THEN 0.5 ELSE 0 END {INDEX_NAME}
        FROM(
                SELECT 
                    bbd_qyxx_id, 
                    CASE WHEN {INDEX_NAME} == '正常' THEN 1 ELSE 0 END {INDEX_NAME}
                FROM tmp
            ) a
        GROUP BY a.bbd_qyxx_id
        '''.format(INDEX_NAME=index_name)
    )
    return result_df


def calculate_dev_index_background(entry: Entry, current_month: str, end_static_month: str, hdfs_temp_path: str):
    """
    计算背景实力指数
    :param entry:
    :param current_month:
    :param end_static_month:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("Enterprise monitor is calculating dev_index_background index...")
    # STEP 1：原始数据提取 #
    # 企业注册资本
    regcap_amount_df = spark.sql(
        '''
        SELECT bbd_qyxx_id,
               nvl(regcap_amount, 0) regcap_amount
        FROM basic_data
        '''
    )

    # 企业的关联企业是否上市
    related_company_is_listed_df = spark.sql(
        '''
        SELECT
            e.bbd_qyxx_id,
            CASE WHEN SUM (e.is_listed) > 0 THEN 1 ELSE 0 END related_company_is_listed
        FROM(
                SELECT
                    c.bbd_qyxx_id,
                    CASE WHEN d.listed_section='上市公司' THEN 1 ELSE 0 END is_listed
                FROM(
                        SELECT
                            a.bbd_qyxx_id,
                            b.source_bbd_id
                        FROM basic_data a
                        LEFT JOIN(
                                    SELECT
                                        bbd_qyxx_id,
                                        source_bbd_id
                                    FROM dw.off_line_relations
                                    WHERE dt={DT1} AND source_degree<=3 AND destination_degree=0 AND source_isperson=0
                                 ) b
                        ON a.bbd_qyxx_id=b.bbd_qyxx_id
                    ) c
                LEFT JOIN(
                            SELECT
                                bbd_qyxx_id,
                                listed_section
                            FROM dw.qyxg_jqka_ipo_basic
                            WHERE dt={DT2}
                         ) d
                ON c.source_bbd_id=d.bbd_qyxx_id
                UNION
                SELECT
                    c.bbd_qyxx_id,
                    CASE WHEN d.listed_section='上市公司' THEN 1 ELSE 0 END is_listed
                FROM(
                        SELECT
                            a.bbd_qyxx_id,
                            b.destination_bbd_id
                        FROM basic_data a
                        LEFT JOIN(
                                    SELECT
                                        bbd_qyxx_id,
                                        destination_bbd_id
                                    FROM dw.off_line_relations
                                    WHERE dt={DT1} AND destination_degree<=3 AND source_degree=0 AND destination_isperson=0
                                 ) b
                        ON a.bbd_qyxx_id=b.bbd_qyxx_id
                    ) c
                LEFT JOIN(
                            SELECT
                                bbd_qyxx_id,
                                listed_section
                            FROM dw.qyxg_jqka_ipo_basic
                            WHERE dt={DT2}
                         ) d
                ON c.destination_bbd_id=d.bbd_qyxx_id
            ) e
        GROUP BY e.bbd_qyxx_id
        '''.format(DT1=HiveUtil.get_month_newest_partition(spark, 'off_line_relations', current_month),
                   DT2=HiveUtil.get_month_newest_partition(spark, 'qyxg_jqka_ipo_basic', current_month))
    )

    # 企业的分支机构数量
    fzjg_num_df = spark.sql(
        '''
        SELECT
            a.bbd_qyxx_id,
            COUNT (DISTINCT a.name) AS fzjg_num
        FROM(
                SELECT
                    L.bbd_qyxx_id,
                    R.name
                FROM basic_data  L
                LEFT JOIN(
                            SELECT
                                bbd_qyxx_id,
                                name
                            FROM dw.qyxx_fzjg
                            WHERE dt={DT1}
                         ) R
                ON L.bbd_qyxx_id=R.bbd_qyxx_id
            ) a
        JOIN(
                SELECT
                    company_name
                FROM dw.qyxx_basic
                WHERE dt={DT2}
                AND company_enterprise_status='存续'
            ) b
        ON a.name=b.company_name
        GROUP BY a.bbd_qyxx_id
        '''.format(DT1=HiveUtil.get_month_newest_partition(spark, 'qyxx_fzjg', current_month),
                   DT2=HiveUtil.get_month_newest_partition(spark, 'qyxx_basic', current_month))
    )

    # 企业的中标金额
    bid_amount_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            SUM (R.bid_amount) AS bid_amount
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        company_name_win,
                        bid_amount
                    FROM dw.manage_bidwinning
                    WHERE dt={DT}
                    AND date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                 ) R
        ON L.company_name=R.company_name_win
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_bidwinning', current_month),
                   CURRENT=end_static_month)
    )

    # STEP 2：二级指标基础数据计算 #
    # 企业注册资本的排名百分比
    regcap_amount_rank_percent_df = calculate_company_index_rank_percent(spark, regcap_amount_df, "regcap_amount")
    # 企业关联企业是否上市标准化
    related_company_is_listed_standard_df = company_is_listed_standard(spark, related_company_is_listed_df, 'related_company_is_listed')
    # 企业分支机构数量的排名百分比
    fzjg_num_rank_percent_df = calculate_company_index_rank_percent(spark, fzjg_num_df, "fzjg_num")
    # 企业中标金额的排名百分比
    bid_amount_rank_percent_df = calculate_company_index_rank_percent(spark, bid_amount_df, "bid_amount")

    # STEP 3：二级指标计算 #
    # 注册资本的得分
    regcap_amount_score = regcap_amount_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((regcap_amount_rank_percent * 5), 0) as regcap_amount_score")
    # 关联企业是否上市的得分
    related_company_is_listed_score = related_company_is_listed_standard_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((related_company_is_listed * 10), 0) as related_company_is_listed")
    # 分支机构数量的得分
    fzjg_num_score = fzjg_num_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((fzjg_num_rank_percent * 5), 0) as fzjg_num_score")
    # 中标金额的得分
    bid_amount_score = bid_amount_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((bid_amount_rank_percent * 5), 0) as bid_amount_score")

    # STEP 4：一级指标计算 #
    # 背景实力指数得分
    regcap_amount_score.join(related_company_is_listed_score, ['bbd_qyxx_id'], 'left') \
        .join(fzjg_num_score, ['bbd_qyxx_id'], 'left') \
        .join(bid_amount_score, ['bbd_qyxx_id'], 'left').createOrReplaceTempView("background_strength_indexes")
    # 标准化背景实力指数得分
    dev_index_background = spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            index_background * 100 / MAX (index_background) OVER() AS dev_index_background
        FROM(
                SELECT
                    bbd_qyxx_id,
                    nvl(regcap_amount_score, 0) + nvl(related_company_is_listed, 0) + nvl(fzjg_num_score, 0)
                    + nvl(bid_amount_score, 0) AS index_background
                FROM background_strength_indexes
            )
        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_background")
    dev_index_background.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_background index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_background index calculation!")
    dev_index_background.createOrReplaceTempView("dev_index_background")


def calculate_dev_index_recruit(entry: Entry, current_month: str, end_static_month: str, hdfs_temp_path: str):
    """
    计算人才招聘指数
    :param entry:
    :param current_month:
    :param end_static_month:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("Enterprise monitor is calculating dev_index_recruit index...")
    # STEP 1：原始数据提取 #
    # 企业的新增招聘数量
    new_recruit_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            SUM (R.bbd_recruit_num) AS recruit_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        bbd_recruit_num
                    FROM dw.manage_recruit
                    WHERE dt={DT}
                    AND date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_recruit', current_month),
                   CURRENT=end_static_month)
    )

    # 企业的本科及以上招聘数量
    highly_educated_recruit_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            SUM (R.bbd_recruit_num) AS recruit_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        bbd_recruit_num
                    FROM dw.manage_recruit
                    WHERE dt={DT}
                    AND date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                    AND education_required RLIKE '本科|博士|硕士'
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_recruit', current_month),
                   CURRENT=end_static_month)
    )

    # STEP 2：二级指标基础数据计算 #
    # 企业新增招聘数量的排名百分比
    new_recruit_num_rank_percent_df = calculate_company_index_rank_percent(spark, new_recruit_num_df, "recruit_num")
    # 企业的本科及以上招聘数量占比
    highly_educated_recruit_num_standard_df = recruit_index_standard(spark, highly_educated_recruit_num_df,
                                                                     new_recruit_num_df, "highly_educated")

    # STEP 3：二级指标计算 #
    # 本科及以上招聘数量占比的得分
    highly_educated_recruit_num_score = highly_educated_recruit_num_standard_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((highly_educated_recruit_num_percent * 10), 0) as highly_educated_recruit_num_score")
    # 新增招聘数量的得分
    new_recruit_num_score = new_recruit_num_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((recruit_num_rank_percent * 5), 0) as new_recruit_num_score")

    # STEP 4：一级指标计算 #
    # 人才招聘得分
    new_recruit_num_score.join(highly_educated_recruit_num_score, ['bbd_qyxx_id'], 'left') \
        .createOrReplaceTempView("talent_recruitment_indexes")
    # 标准化人才招聘指数得分
    dev_index_recruit = spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            dev_index_recruit * 100 / MAX (dev_index_recruit) OVER() AS dev_index_recruit
        FROM(
                SELECT
                    bbd_qyxx_id,
                    nvl(new_recruit_num_score, 0) + nvl(highly_educated_recruit_num_score, 0) dev_index_recruit
                FROM talent_recruitment_indexes
            )

        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_recruit")
    dev_index_recruit.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_recruit index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_recruit calculation!")
    dev_index_recruit.createOrReplaceTempView("dev_index_recruit")


def calculate_dev_index_innovation(entry: Entry, current_month: str, start_static_month: str, end_static_month: str, hdfs_temp_path: str):
    """
    计算创新能力指数
    :param entry:
    :param current_month:
    :param start_static_month:
    :param end_static_month:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("Enterprise monitor is calculating dev_index_innovation index...")
    # STEP 1：原始数据提取 #
    # 企业的新增招聘数量
    new_recruit_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            SUM (R.bbd_recruit_num) AS recruit_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        bbd_recruit_num
                    FROM dw.manage_recruit
                    WHERE dt={DT}
                    AND date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_recruit', current_month),
                   CURRENT=end_static_month)
    )

    # 企业的本年累计专利总数量
    patent_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            COUNT (DISTINCT R.application_code) AS patent_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        application_code
                    FROM dw.prop_patent_data
                    WHERE dt={DT}
                    AND '{START_MONTH}'<=date_format(publidate, 'yyyy-MM')
                    AND date_format(publidate, 'yyyy-MM')<='{CURRENT}'
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'prop_patent_data', current_month),
                   START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "patent_num")
    patent_num_df.fillna(0).repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written patent_num index to {output_hdfs_path}!")

    # 企业的本月专利分布
    patent_dist_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            R.patent_type,
            COUNT (DISTINCT R.application_code) AS patent_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        patent_type,
                        application_code
                    FROM dw.prop_patent_data
                    WHERE dt={DT}
                    AND date_format(publidate, 'yyyy-MM')='{CURRENT}'
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        GROUP BY L.bbd_qyxx_id, R.patent_type
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'prop_patent_data', current_month),
                   CURRENT=end_static_month)
    )
    if patent_dist_df.count() == 0:
        schema = StructType([
            StructField("bbd_qyxx_id", StringType(), True),
            StructField("patent_distribution", StringType(), True)
        ])
        empty_rdd = spark.sparkContext.parallelize([Row(bbd_qyxx_id="-1", patent_distribution="-1")])
        patent_distribution_df = spark.createDataFrame(empty_rdd, schema)
    else:
        patent_distribution_df = patent_dist_df \
            .rdd \
            .map(map_parse) \
            .groupByKey() \
            .map(lambda x: patent_distribution_parse(x[1])) \
            .map(lambda x: Row(bbd_qyxx_id=x['bbd_qyxx_id'], patent_distribution=x['patent_distribution'])) \
            .toDF(["bbd_qyxx_id", "patent_distribution"])
    output_hdfs_path = os.path.join(hdfs_temp_path, "patent_distribution")
    patent_distribution_df.fillna(0).repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written patent_distribution index to {output_hdfs_path}!")

    # 企业的研发人员招聘数量
    rd_recruit_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            SUM (R.bbd_recruit_num) AS recruit_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        bbd_recruit_num
                    FROM dw.manage_recruit
                    WHERE dt={DT}
                    AND date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                    AND (job_title RLIKE '研究|开发|研发|技术|设计|Engineer|engineer'
                    OR job_title_class RLIKE '研究|开发|研发|技术|设计|Engineer|engineer')
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'manage_recruit', current_month),
                   CURRENT=end_static_month)
    )

    # 企业的本年累计商标注册总数量
    brand_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            COUNT (DISTINCT R.application_no) AS brand_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        applicant_name,
                        application_no
                    FROM dw.prop_brand_data
                    WHERE dt={DT}
                    AND '{START_MONTH}'<=date_format(reg_notice_date, 'yyyy-MM')
                    AND date_format(reg_notice_date, 'yyyy-MM')<='{CURRENT}'
                    AND current_status LIKE '%已注册%'
                 ) R
        ON L.company_name=R.applicant_name
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'prop_brand_data', current_month),
                   START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # 企业的本年累计软件著作权总数量
    rz_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            COUNT (DISTINCT R.regnum) AS rz_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        copyright_owner,
                        regnum
                    FROM dw.rjzzq
                    WHERE dt={DT}
                    AND '{START_MONTH}'<=date_format(regdate, 'yyyy-MM')
                    AND date_format(regdate, 'yyyy-MM')<='{CURRENT}'
                 ) R
        ON L.company_name=R.copyright_owner
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'rjzzq', current_month),
                   START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # STEP 2：二级指标基础数据计算 #
    # 企业本年累计专利总数量的排名百分比
    patent_num_rank_percent_df = calculate_company_index_rank_percent(spark, patent_num_df, "patent_num")
    # 企业本年累计商标注册总数量的排名百分比
    brand_num_rank_percent_df = calculate_company_index_rank_percent(spark, brand_num_df, "brand_num")
    # 企业本年累计软件著作权总数量的排名百分比
    rz_num_rank_percent_df = calculate_company_index_rank_percent(spark, rz_num_df, "rz_num")
    # 企业的研发人员招聘数量占比
    rd_recruit_num_standard_df = recruit_index_standard(spark, rd_recruit_num_df, new_recruit_num_df, "rd")

    # STEP 3：二级指标计算 #
    # 本年累计专利总数量的得分
    patent_num_score = patent_num_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((patent_num_rank_percent * 5), 0) as patent_num_score")
    # 研发人员招聘数量占比的得分
    rd_recruit_num_score = rd_recruit_num_standard_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((rd_recruit_num_percent * 10), 0) as rd_recruit_num_score")
    # 本年累计商标注册总数量的得分
    brand_num_score = brand_num_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((brand_num_rank_percent * 2.5), 0) as brand_num_score")
    # 本年累计软件著作权总数量的得分
    rz_num_score = rz_num_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((rz_num_rank_percent * 2.5), 0) as rz_num_score")

    # STEP 4：一级指标计算 #
    # 创新能力得分
    patent_num_score.join(rd_recruit_num_score, ['bbd_qyxx_id'], 'left') \
        .join(brand_num_score, ['bbd_qyxx_id'], 'left') \
        .join(rz_num_score, ['bbd_qyxx_id'], 'left').createOrReplaceTempView("creativity_indexes")
    # 标准化创新能力指数得分
    dev_index_innovation = spark.sql(
        '''
        SELECT bbd_qyxx_id,
               dev_index_innovation * 100 / MAX (dev_index_innovation) OVER() AS dev_index_innovation
        FROM(
                SELECT
                    bbd_qyxx_id,
                    nvl(patent_num_score, 0) + nvl(rd_recruit_num_score, 0) + nvl(brand_num_score, 0)
                    + nvl(rz_num_score, 0)  AS dev_index_innovation
                FROM creativity_indexes
            )

        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_innovation")
    dev_index_innovation.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_innovation index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_innovation calculation!")
    dev_index_innovation.createOrReplaceTempView("dev_index_innovation")


def calculate_dev_index_capital(entry: Entry, current_month: str, end_static_month: str, financing_hdfs_path: str, hdfs_temp_path: str):
    """
    计算资本吸引指数
    :param entry:
    :param current_month:
    :param end_static_month:
    :param financing_hdfs_path:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("Enterprise monitor is calculating dev_index_innovation index...")
    # STEP 1：原始数据提取 #
    # 企业的融资金额
    try:
        spark.read.csv(financing_hdfs_path, header=True, sep='\t').createOrReplaceTempView("financing")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to read the financing amount from the {financing_hdfs_path} path!")
        traceback.print_exc()
        raise e
    financing_amount_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            R.financing_wan AS financing_amount
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        company_name,
                        financing_wan
                    FROM financing
                    WHERE date_format(pubdate, 'yyyy-MM')='{CURRENT}'
                 ) R
        ON L.company_name=R.company_name
        '''.format(CURRENT=end_static_month)
    )

    # 企业是否上市
    company_is_listed_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            CASE WHEN R.listed_section = '上市公司' THEN 1 ELSE 0 END company_is_listed
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        listed_section
                    FROM dw.qyxx_jqka_ipo_basic
                    WHERE dt={DT}
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'qyxx_jqka_ipo_basic', current_month))
    )

    # STEP 2：二级指标基础数据计算 #
    # 企业融资金额的排名百分比
    financing_amount_rank_percent_df = calculate_company_index_rank_percent(spark, financing_amount_df, "financing_amount")
    # 企业是否上市标准化
    company_is_listed_standard_df = company_is_listed_standard(spark, company_is_listed_df, 'company_is_listed')

    # STEP 3：二级指标计算 #
    # 融资金额的得分
    financing_amount_score = financing_amount_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((financing_amount_rank_percent * 5), 0) as financing_amount_score")
    # 是否上市的得分
    company_is_listed_score = company_is_listed_standard_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((company_is_listed * 15), 0) as company_is_listed_score")

    # STEP 4：一级指标计算 #
    # 资本吸引得分
    company_is_listed_score.join(financing_amount_score, ['bbd_qyxx_id'], 'left') \
        .createOrReplaceTempView("capital_attraction_indexes")
    # 标准化资本吸引指数得分
    dev_index_capital = spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            dev_index_capital * 100 / MAX (dev_index_capital) OVER() AS dev_index_capital
        FROM(
                SELECT
                    bbd_qyxx_id,
                    nvl(financing_amount_score, 0) + nvl(company_is_listed_score, 0) AS dev_index_capital
                FROM capital_attraction_indexes
            )

        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_capital")
    dev_index_capital.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_capital index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_capital calculation!")
    dev_index_capital.createOrReplaceTempView("dev_index_capital")


def calculate_dev_index_risk(entry: Entry, current_month: str, start_static_month: str, end_static_month: str, hdfs_temp_path: str):
    """
    计算负面风险指数
    :param entry:
    :param current_month:
    :param start_static_month:
    :param end_static_month:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("Enterprise monitor is calculating dev_index_risk index...")
    # STEP 1：原始数据提取 #
    # 企业是否经营异常过
    jyyc_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            CASE WHEN R.rank_date IS NOT NULL OR R.remove_date IS NOT NULL THEN '异常' ELSE '正常' END AS jyyc
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        rank_date,
                        remove_date
                    FROM dw.qyxx_jyyc
                    WHERE  dt={DT}
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'qyxx_jyyc', current_month))
    ).distinct()

    # 企业是否严重违法过
    yzwf_df = spark.sql(
        '''
        SELECT
            DISTINCT
            L.bbd_qyxx_id,
            CASE WHEN R.rank_date IS NOT NULL OR R.remove_date IS NOT NULL THEN '违法' ELSE '正常' END yzwf
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        bbd_qyxx_id,
                        rank_date,
                        remove_date
                    FROM dw.qyxx_yzwf
                    WHERE dt={DT}
                 ) R
        ON L.bbd_qyxx_id=R.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'qyxx_yzwf', current_month))
    ).distinct()

    # 企业是否失信被执行过
    dishonesty_executed_df = spark.sql(
        '''
        SELECT
            DISTINCT
            L.bbd_qyxx_id,
            CASE WHEN R.pubdate IS NULL THEN '正常' ELSE '失信被执行' END dishonesty_executed
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        pname,
                        pubdate
                    FROM dw.legal_dishonest_persons_subject_to_enforcement
                    WHERE dt={DT}
                 ) R
        ON L.company_name=R.pname
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'legal_dishonest_persons_subject_to_enforcement', current_month))
    ).distinct()

    # 企业的本年累计被执行案件数量
    executed_case_num_df = spark.sql(
        '''
        SELECT
            L.bbd_qyxx_id,
            COUNT (DISTINCT R.case_code) AS executed_case_num
        FROM basic_data L
        LEFT JOIN(
                    SELECT
                        defendant,
                        case_code
                    FROM dw.legal_adjudicative_documents
                    WHERE dt={DT}
                    AND '{START_MONTH}'<=date_format(sentence_date, 'yyyy-MM')
                    AND date_format(sentence_date, 'yyyy-MM')<='{CURRENT}'
                 ) R
        ON L.company_name=R.defendant
        GROUP BY L.bbd_qyxx_id
        '''.format(DT=HiveUtil.get_month_newest_partition(spark, 'legal_adjudicative_documents', current_month),
                   START_MONTH=start_static_month,
                   CURRENT=end_static_month)
    )

    # STEP 2：二级指标基础数据计算 #
    # 企业是否经营异常
    jyyc_standard_df = standardization_of_business_behavior(spark, jyyc_df, "jyyc")
    # 企业是否严重违法
    yzwf_standard_df = standardization_of_business_behavior(spark, yzwf_df, "yzwf")
    # 企业是否失信被执行
    dishonesty_executed_standard_df = standardization_of_business_behavior(spark, dishonesty_executed_df, "dishonesty_executed")
    # 企业被执行案件数量的排名百分比
    executed_case_num_rank_percent_df = calculate_company_index_rank_percent(spark, executed_case_num_df, "executed_case_num", "DESC")

    # STEP 3：二级指标计算 #
    # 是否经营异常的得分
    jyyc_score = jyyc_standard_df.selectExpr("bbd_qyxx_id", "nvl((jyyc * 5), 0) as jyyc_score")
    # 是否严重违法的得分
    yzwf_score = yzwf_standard_df.selectExpr("bbd_qyxx_id", "nvl((yzwf * 5), 0) as yzwf_score")
    # 是否失信被执行的得分
    dishonesty_executed_score = dishonesty_executed_standard_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((dishonesty_executed * 5), 0) as dishonesty_executed_score")
    # 被执行案件数量的得分
    executed_case_num_score = executed_case_num_rank_percent_df.selectExpr(
        "bbd_qyxx_id",
        "nvl((executed_case_num_rank_percent * 5), 0) as executed_case_num_score")

    # 负面风险得分
    jyyc_score.join(yzwf_score, ['bbd_qyxx_id'], 'left') \
        .join(dishonesty_executed_score, ['bbd_qyxx_id'], 'left') \
        .join(executed_case_num_score, ['bbd_qyxx_id'], 'left').createOrReplaceTempView("negative_risk_indexes")
    # 标准化负面风险指数得分
    dev_index_risk = spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            dev_index_risk * 100 / MAX (dev_index_risk) OVER() AS dev_index_risk
        FROM(
                SELECT
                    bbd_qyxx_id,
                    NVL(jyyc_score, 0) + NVL(yzwf_score, 0) + NVL(dishonesty_executed_score, 0)
                    + NVL(executed_case_num_score, 0) AS dev_index_risk
                FROM negative_risk_indexes
            )

        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_risk")
    dev_index_risk.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_risk index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_risk calculation!")
    dev_index_risk.createOrReplaceTempView("dev_index_risk")


def calculate_enterprise_dev_index(entry: Entry, hdfs_temp_path: str):
    """
    计算企业发展指数和企业发展指数百分位
    :param entry:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    logger.info("Enterprise monitor is calculating enterprise_dev_index index...")
    # 四川省每个企业的月度企业发展指数
    dev_index_score = spark.sql(
        '''
        SELECT
            a.bbd_qyxx_id,
            NVL(a.dev_index_background, 0) * 25 / 100 + NVL(b.dev_index_recruit, 0) * 15 / 100 
            + NVL(c.dev_index_innovation, 0) * 20 / 100 + NVL(d.dev_index_capital, 0) * 20 / 100 
            + NVL(e.dev_index_risk, 0) * 20 / 100 AS dev_index_score
        FROM dev_index_background a
        LEFT JOIN dev_index_recruit b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        LEFT JOIN dev_index_innovation c
        ON a.bbd_qyxx_id=c.bbd_qyxx_id
        LEFT JOIN dev_index_capital d
        ON a.bbd_qyxx_id=d.bbd_qyxx_id
        LEFT JOIN dev_index_risk e
        ON a.bbd_qyxx_id=e.bbd_qyxx_id
        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_score")
    dev_index_score.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_score index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_score calculation!")
    dev_index_score.createOrReplaceTempView("dev_index_score")
    # 四川省每个企业的月度企业发展指数的百分位排名
    spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            dev_index_score,
            ROW_NUMBER() OVER(ORDER BY dev_index_score DESC) rank
        FROM dev_index_score
        '''
    ).createOrReplaceTempView("tmp")
    dev_index_percentile = spark.sql(
        '''
        SELECT
            t1.bbd_qyxx_id,
            t2.avg_rank / MAX (t2.avg_rank) OVER() AS dev_index_percentile
        FROM tmp t1
        LEFT JOIN(
                    SELECT
                        dev_index_score,
                        AVG (rank) AS avg_rank
                    FROM tmp
                    GROUP BY dev_index_score
                ) t2
        ON t1.dev_index_score=t2.dev_index_score
        '''
    )
    output_hdfs_path = os.path.join(hdfs_temp_path, "dev_index_percentile")
    dev_index_percentile.repartition(1).write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written dev_index_percentile index to {output_hdfs_path}!")
    logger.info(f"Enterprise monitor has finished dev_index_percentile calculation!")


def merge_and_output_index(entry: Entry, dep_data_path: str, hdfs_temp_path: str):
    """
    合并指标并输出到指定路径
    :param entry:
    :param dep_data_path:
    :param hdfs_temp_path:
    :return:
    """
    spark = entry.spark
    logger = entry.logger
    version = entry.version
    # 合并指标
    try:
        dev_index_score = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_score"), header=True)
        dev_index_percentile = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_percentile"), header=True)
        dev_index_background = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_background"), header=True)
        dev_index_recruit = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_recruit"), header=True)
        dev_index_innovation = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_innovation"), header=True)
        dev_index_capital = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_capital"), header=True)
        dev_index_risk = spark.read.csv(os.path.join(hdfs_temp_path, "dev_index_risk"), header=True)
        patent_num = spark.read.csv(os.path.join(hdfs_temp_path, "patent_num"), header=True)
        patent_distribution = spark.read.csv(os.path.join(hdfs_temp_path, "patent_distribution"), header=True)
        # 阿拉丁分数存储目录
        alandding_hdfs_path = os.path.join(dep_data_path, "SC_JINGXIN_company_aladding_score.csv")
        spark.read.csv(alandding_hdfs_path, header=True).createOrReplaceTempView("alandding_socre")
    except Exception as e:
        import traceback
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to read the index result!")
        traceback.print_exc()
        raise e
    alandding_socre = spark.sql(
        '''
        SELECT 
            a.bbd_qyxx_id,
            b.total_score as credit_risk_score,
            b.qycx_M as risk_index_qycx,
            b.qyfz_M as risk_index_qyfz,
            b.jtglf_M as risk_index_jtglf,
            b.dtglf_M as risk_index_dtglf,
            b.qyjy_M as risk_index_qyjy,
            b.zhsl_M as risk_index_zhsl
        FROM basic_data a
        LEFT JOIN alandding_socre b
        ON a.bbd_qyxx_id=b.bbd_qyxx_id
        '''
    )
    enterprise_monitor = alandding_socre.join(dev_index_score, ['bbd_qyxx_id'], 'left') \
        .join(dev_index_percentile, ['bbd_qyxx_id'], 'left') \
        .join(dev_index_background, ['bbd_qyxx_id'], 'left') \
        .join(dev_index_recruit, ['bbd_qyxx_id'], 'left') \
        .join(dev_index_innovation, ['bbd_qyxx_id'], 'left') \
        .join(dev_index_capital, ['bbd_qyxx_id'], 'left') \
        .join(dev_index_risk, ['bbd_qyxx_id'], 'left') \
        .join(patent_num, ['bbd_qyxx_id'], 'left') \
        .join(patent_distribution, ['bbd_qyxx_id'], 'left') \
        .selectExpr(
        "{STATIC_MONTH} AS static_month".format(STATIC_MONTH=version),
        "*"
    ).filter("credit_risk_score is not null").fillna(0).repartition(1)
    output_hdfs_path = os.path.join(hdfs_temp_path, "result")
    logger.info("Enterprise monitor is merging all indexes...")
    enterprise_monitor.write.csv(output_hdfs_path, header=True, mode="overwrite")
    logger.info(f"Enterprise monitor has written all indexes to {output_hdfs_path}!")
    # 结果输出到HDFS
    output_hdfs_path = os.path.join(entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'path_result'), "monthly_info_company")
    out_put_util = ResultOutputUtil(entry, table_name="monthly_info_company")
    out_put_util.save(enterprise_monitor)
    logger.info(f"Enterprise monitor has written all indexes to {output_hdfs_path}!")


def calculate_index(entry: Entry) -> DataFrame:
    # 20210602
    version = entry.version
    logger = entry.logger
    year = version[:4]
    month = version[4:6]
    # 计算发生月份
    str2date = datetime.datetime.strptime(version, "%Y%m")
    current_month = DateUtils.add_date_2str(date_time=str2date, fmt="%Y%m", months=1)
    # 统计开始月份
    start_static_month = year + "-01"
    # 统计截止月份
    end_static_month = year + "-" + month
    # 经信厅本月依赖数据HDFS根目录
    dep_data_path = os.path.join(entry.cfg_mgr.hdfs.get_input_path('hdfs-biz', 'path_basic_data'), "dependency_data")
    # 经信厅HDFS暂存目录
    hdfs_temp_path = os.path.join(entry.cfg_mgr.hdfs.get_tmp_path('hdfs-biz', 'path_basic_data'), "enterprise_monitor")
    # 融资金额由指数部门提供
    financing_hdfs_path = os.path.join(dep_data_path, "financing_data.csv")

    logger.info("============enterprise monitor start calculating indexes============")

    # 计算背景实力指数
    calculate_dev_index_background(entry, current_month, end_static_month, hdfs_temp_path)
    # 计算人才招聘指数
    calculate_dev_index_recruit(entry, current_month, end_static_month, hdfs_temp_path)
    # 计算创新能力指数
    calculate_dev_index_innovation(entry, current_month, start_static_month, end_static_month, hdfs_temp_path)
    # 计算资本吸引指数
    calculate_dev_index_capital(entry, current_month, end_static_month, financing_hdfs_path, hdfs_temp_path)
    # 计算负面风险指数
    calculate_dev_index_risk(entry, current_month, start_static_month, end_static_month, hdfs_temp_path)
    # 计算企业发展指数和企业发展指数百分位
    calculate_enterprise_dev_index(entry, hdfs_temp_path)

    # 合并指标并输出
    merge_and_output_index(entry, dep_data_path, hdfs_temp_path)

    # 删除暂存数据
    delete_cmd = "hdfs dfs -rm -r -f " + hdfs_temp_path
    if os.system(delete_cmd) == 0:
        logger.info(f"The temporary data on the {hdfs_temp_path} directory has been deleted!")
    else:
        logger.error("#########################################Exception##########################################")
        logger.error(f"Failed to delete the temporary data from the {hdfs_temp_path} directory!")
        raise Exception("暂存目录删除失败！")
    logger.info("============enterprise monitor indexes has been calculated============")


def main(entry: Entry):
    # 抽取基础数据
    extract_basic_data(entry)
    # 计算企业监控相关指标
    calculate_index(entry)


def post_check(entry: Entry):
    return True
