#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 投资价值
:Author: luzihao@bbdservice.com
:Date: 2021-04-28 16:08
"""

import datetime
import os, json, re, time

from pyspark import Row
from pyspark.sql.types import StringType
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from cqxdcy.proj_common.json_util import JsonUtils
from pyspark.sql import functions as fun, DataFrame

from cqxdcy.proj_common.date_util import DateUtils
from cqxdcy.proj_common.log_track_util import LogTrack

CAFE_COMPANY = '/user/cqxdcyfzyjy/temp/cafe_temp'

class CnUtils:
    """
    原始路径
    """
    @classmethod
    def get_source_path(cls, entry):
        # return entry.cfg_mgr.get("hdfs", "hdfs_root_path") + entry.cfg_mgr.get("hdfs", "hdfs_source_path")
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")
    """
    输出路径
    """
    @classmethod
    def get_final_path(cls, entry):
        # return entry.cfg_mgr.get("hdfs", "hdfs_root_path") + entry.cfg_mgr.get("hdfs", "hdfs_index_path")
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")
    """
    临时路径
    """
    @classmethod
    def get_temp_path(cls, entry):
        # return entry.cfg_mgr.get("hdfs", "hdfs_root_path") + entry.cfg_mgr.get("hdfs", "hdfs_temp_path")
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")
    """
    获取当月第一天
    """
    @classmethod
    def get_first_day_of_month(cls):
        return DateUtils.now2str(fmt='%Y%m') + '01'
    """
    获取当前时间
    """
    @classmethod
    def now_time(cls):
        return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    """
    保存到hdfs
    """
    @classmethod
    def saveToHdfs(cls, rdd, path, number = 20):
        os.system("hadoop fs -rm -r " + path)
        rdd.repartition(number).saveAsTextFile(path)
    """
    获取上月月初
    """
    @staticmethod
    def get_first_day_of_last_month():
        return DateUtils.add_date(datetime.datetime.today().replace(day=1)).strftime('%Y-%m-%d')
    """
    获取上月月末
    """
    @staticmethod
    def get_last_day_of_last_month():
        next_month = DateUtils.add_date(datetime.datetime.today()).replace(day=28) + datetime.timedelta(days=4)
        return (next_month - datetime.timedelta(days=next_month.day)).strftime('%Y-%m-%d')
    """
    check data
    """
    @classmethod
    def save_parquet(cls, df: DataFrame,path: str, num_partitions = 20):
        df.repartition(num_partitions).write.parquet(path, mode="overwrite")


def get_tzjz_model(entry):
    path = f"{CnUtils.get_temp_path(entry)}/model/invest_scores.csv"
    model_index = entry.spark.read.csv(path, header=True, sep=',')
    model_index.createOrReplaceTempView('model_index')
    return model_index

def get_enterprise_list(entry):

    # 获取企业基本信息

    company_basic = entry.spark.sql(
        '''
        select a.bbd_qyxx_id, a.industry, a.sub_industry, a.province, a.city, a.region as area, 
        b.company_name, b.esdate,b.regcap_amount ,b.regcap, b.address, b.frname, b.dt, b.company_industry, CONCAT(a.bbd_qyxx_id, b.dt) as id
        from model_index a
        join  (
        select regcap_amount, company_name, esdate, regcap, address, frname, dt, bbd_qyxx_id, company_industry from dw.qyxx_basic where dt = {b_dt}
        ) b
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        '''.format(b_dt=HiveUtil.newest_partition_by_month(entry.spark, 'dw.qyxx_basic', in_month_str=entry.version[:6]))
    )

    # def map1(statistics_time):
    #     timeArray = time.strptime(statistics_time, "%Y%m%d")
    #     otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    #     str2 = str(otherStyleTime)
    #     return str2
    #
    # udf = fun.udf(map1, StringType())
    def map1(statistics_time):
        timeArray = time.strptime(statistics_time, "%Y%m%d")
        otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
        str2 = str(otherStyleTime)
        str3 = f"{str2[0:8]}01"
        return str3
    udf = fun.udf(map1, StringType())
    company_basic = company_basic.withColumn("dt", udf("dt"))
    basic = company_basic.withColumn("update_time", fun.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))).withColumnRenamed('dt', 'date')

    company_scale = entry.spark.sql(
        '''
        select a.bbd_qyxx_id, b.company_scale as scale from (
        select bbd_qyxx_id, company_scale from dw.qyxx_enterprise_scale where dt = {b_dt}
        ) b
        join model_index a 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        '''.format(b_dt=HiveUtil.newest_partition_by_month(entry.spark, 'dw.qyxx_enterprise_scale', in_month_str=entry.version[:6]))
    )

    company_qualification = entry.spark.sql(
        '''
        select a.bbd_qyxx_id, b.nick_name as qualification from (
        select bbd_qyxx_id, nick_name from dw.news_cqzstz where dt = {b_dt}
        ) b
        join model_index a 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        '''.format(b_dt=HiveUtil.newest_partition_by_month(entry.spark, 'dw.news_cqzstz', in_month_str=entry.version[:6]))
    )

    # 综合投资价值分, 投资评分详情

    company_value = entry.spark.sql(
        '''
        select bbd_qyxx_id, T2_score, T4_score, T12_score, T20_score, T32_score,
        T36_score, T38_score, T41_score, T49_score, T54_score, T58_score,
        T62_2_score, T65_score, T66_score, profit_capability, operate_capability, industry_status,
        related_power, technology_capability, total
        from model_index
        '''
    )

    def to_json_data(row):
        """
        返回指定json数据
        :param row:
        :return:
        """
        tmp_data = row.asDict(True)
        print(tmp_data)
        final_data = dict()
        push_data = {
            "盈利能力": {
                "score": tmp_data['profit_capability'],
                "detail": {}
            },
            "运营能力":{
                "score": tmp_data['operate_capability'],
                "detail": {}
            },
            "行业地位":{
                "score": tmp_data['industry_status'],
                "detail": {}
            },
            "关联方实力":{
                "score": tmp_data['related_power'],
                "detail": {}
            },
            "技术能力":{
                "score": tmp_data['technology_capability'],
                "detail": {}
            }
        }
        push_data["盈利能力"]["detail"]["销售净利率"] = tmp_data["T2_score"]
        push_data["盈利能力"]["detail"]["资产负债率"] = tmp_data["T4_score"]
        push_data["盈利能力"]["detail"]["营业总收入增长率"] = tmp_data["T12_score"]
        push_data["运营能力"]["detail"]["总资产周转率"] = tmp_data["T20_score"]
        push_data["运营能力"]["detail"]["目标公司自然人股东对外投资数量"] = tmp_data["T32_score"]
        push_data["运营能力"]["detail"]["目标企业受到行政许可次数"] = tmp_data["T36_score"]
        # push_data["行业地位"]["detail"]["市场占有率"] = tmp_data["T37_score"]
        push_data["行业地位"]["detail"]["企业专利数量"] = tmp_data["T38_score"]
        push_data["行业地位"]["detail"]["企业招聘高级人才"] = tmp_data["T41_score"]
        push_data["关联方实力"]["detail"]["关联方平均专利数量"] = tmp_data["T49_score"]
        push_data["关联方实力"]["detail"]["关联方平均高级人才招聘数量"] = tmp_data["T54_score"]
        push_data["关联方实力"]["detail"]["上市企业关联方数量占比"] = tmp_data["T58_score"]
        push_data["技术能力"]["detail"]["企业硕士及以上招聘数量"] = tmp_data["T62_2_score"]
        push_data["技术能力"]["detail"]["高级研发人员占比"] = tmp_data["T65_score"]
        push_data["技术能力"]["detail"]["研发投入"] = tmp_data["T66_score"]
        final_data["bbd_qyxx_id"] = tmp_data["bbd_qyxx_id"]
        final_data["zh_value_detail"] = JsonUtils.to_string(push_data)
        final_data["zh_value"] = tmp_data["total"]
        return JsonUtils.to_string(final_data)

    company_value_json = company_value.rdd.map(to_json_data)
    CnUtils.saveToHdfs(company_value_json,f"{CnUtils.get_temp_path(entry)}/company_value_1")

    final_company_value = entry.spark.read.json(f"{CnUtils.get_temp_path(entry)}/company_value_1")

    return basic, company_scale, company_qualification, final_company_value

def get_investment_trend(entry):

    company_trend = entry.spark.sql(
        '''
        select a.bbd_qyxx_id, a.total as zh_value, b.dt, CONCAT(a.bbd_qyxx_id,b.dt) as id
        from model_index a
        join (
        select dt, bbd_qyxx_id from dw.qyxx_basic where dt = {b_dt}
        ) b
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        '''.format(b_dt=HiveUtil.newest_partition_by_month(entry.spark, 'dw.qyxx_basic', in_month_str=entry.version[:6]))
    )

    industry_total = entry.spark.sql(
        '''
        select industry, round(avg(total),3) as industry_zh_value_avg
        from model_index 
        group by industry
        '''
    )

    industry_total.createOrReplaceTempView('industry_total')

    industry_avg = entry.spark.sql(
        '''
        select a.bbd_qyxx_id, b.industry_zh_value_avg
        from model_index a
        join industry_total b
        on a.industry = b.industry
        '''
    )

    return company_trend, industry_avg


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    index_model = get_tzjz_model(entry)

    basic, company_scale, company_qualification, company_value_json = get_enterprise_list(entry)

    company_trend, industry_avg = get_investment_trend(entry)

    index_result_1 = index_model.select('bbd_qyxx_id')

    index_result_2 = index_model.select('bbd_qyxx_id')

    for index_df in [basic, company_scale, company_qualification, company_value_json]:
        index_result_1 = index_result_1.join(index_df, ['bbd_qyxx_id'], 'left')


    for index_df in [company_trend, industry_avg]:
        index_result_2 = index_result_2.join(index_df, ['bbd_qyxx_id'], 'left')

    def to_push_data_1(row):
        """
        返回订阅推送结构的数据
        :param row:
        :return:
        """
        tmp_data = row.asDict(True)
        print(tmp_data)
        push_data = {}
        tn = "cq_xdcy_recommend_enterprise_list"
        push_data["tn"] = tn
        push_data["data"] = tmp_data
        return JsonUtils.to_string(push_data)

    result_1 = index_result_1.rdd.map(to_push_data_1)

    CnUtils.saveToHdfs(result_1, f"{CnUtils.get_final_path(entry)}/cq_xdcy_recommend_enterprise_list")

    def to_push_data_2(row):
        """
        返回订阅推送结构的数据
        :param row:
        :return:
        """
        tmp_data = row.asDict(True)
        print(tmp_data)
        push_data = {}
        tn = "cq_xdcy_recommend_investment_trend"
        push_data["tn"] = tn
        push_data["data"] = tmp_data
        return JsonUtils.to_string(push_data)


    result_2 = index_result_2.rdd.map(to_push_data_2)

    CnUtils.saveToHdfs(result_1, f"{CnUtils.get_final_path(entry)}/cq_xdcy_recommend_investment_trend")


def post_check(entry: Entry):
    return True