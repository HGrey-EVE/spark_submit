#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 产业分布
:Author: weifuwan@bbdservice.com
:Date: 2021-04-27 16:08
"""
import datetime
import os

from pyspark.sql.types import IntegerType
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from cqxdcy.proj_common.json_util import JsonUtils
from pyspark.sql import functions as fun, DataFrame
from pyspark.sql import Row, functions as F
from cqxdcy.proj_common.date_util import DateUtils
from cqxdcy.proj_common.log_track_util import LogTrack as T


class CnUtils:
    """
    原始路径
    """
    @classmethod
    def get_source_path(cls, entry):
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")
    """
    输出路径
    """
    @classmethod
    def get_final_path(cls, entry):
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")
    """
    临时路径
    """
    @classmethod
    def get_temp_path(cls, entry):
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
        return DateUtils.add_date(datetime.datetime.today().replace(day=1),months = -1).strftime('%Y-%m-%d')
    """
    获取上月月末
    """
    @staticmethod
    def get_last_day_of_last_month():
        next_month = DateUtils.add_date(datetime.datetime.today(), months = -1).replace(day=28) + datetime.timedelta(days=4)
        return (next_month - datetime.timedelta(days = next_month.day)).strftime('%Y-%m-%d')
    """
    保存parquet
    """
    @classmethod
    def save_parquet(cls, df: DataFrame,path: str, num_partitions = 20):
        df.repartition(num_partitions).write.parquet(path, mode = "overwrite")


class CalIndex:

    @staticmethod
    @T.log_track
    def exec_sql(sql: str, entry: Entry):
        entry.logger.info(sql)
        return entry.spark.sql(sql)

    @staticmethod
    def get_company_county(entry: Entry):
        """
        获取省市区
        :return: DataFrame
        """
        dt = HiveUtil.newest_partition(entry.spark, 'dw.company_county')
        entry.spark.sql("""
                      select company_county, province, city, region
                      from 
                          (select code as company_county, province, city, district as region, 
                                      (row_number() over(partition by code order by tag )) as row_num
                          from dw.company_county 
                          where dt = '{dt}' and tag != -1) b 
                      where row_num = 1
                 """.format(dt=dt)).createOrReplaceTempView('company_country')

    @staticmethod
    def cal_qyxx_basic(entry: Entry):
        entry.spark.read.parquet(f"{CnUtils.get_source_path(entry)}/industry_divide").createOrReplaceTempView("qyxx_basic")

    @classmethod
    @T.log_track
    def cal_province_city_area(cls, entry: Entry):
        cls.cal_qyxx_basic(entry)
        cls.get_company_county(entry)
        _sql = """
                select 
                    R.*,
                    L.province,
                    L.city,
                    L.region as area
                from company_country L 
                right join qyxx_basic R 
                on L.company_county = R.company_county
            """
        return cls.exec_sql(_sql, entry)

    @classmethod
    @T.log_track
    def cal_manage_recruit(cls, entry: Entry):
        column = "bbd_qyxx_id, cast(bbd_recruit_num as int) bbd_recruit_num, pubdate"
        year_month = entry.version
        max_dt = HiveUtil.newest_partition_by_month(entry.spark, 'dw.manage_recruit', in_month_str=year_month[:6])
        entry.logger.info("dw.manage_recruit 最新分区 --- {max_dt}".format(max_dt=max_dt))
        f = CnUtils.get_first_day_of_last_month()
        l = CnUtils.get_last_day_of_last_month()
        # _sql = f""" select
        #                 {column}
        #             from dw.manage_recruit
        #             where dt = '{max_dt}'
        #             and nvl(bbd_qyxx_id,'') != ''
        #             and pubdate between '{f}' and '{l}' """
        entry.logger.info(f"weifuwan-f-{f}")
        entry.logger.info(f"weifuwan-l-{l}")

        if year_month == "20201201":
            f = "2020-12-01"
            l = "2020-12-31"
        elif year_month == "20210101":
            f = "2021-01-01"
            l = "2021-01-31"
        elif year_month == "20210201":
            f = "2021-02-01"
            l = "2021-02-28"
        elif year_month == "20210301":
            f = "2021-03-01"
            l = "2021-03-31"
        elif year_month == "20210401":
            f = "2021-04-01"
            l = "2021-04-30"
        elif year_month == "20210501":
            f = "2021-05-01"
            l = "2021-05-31"
        _sql_new = f"""
        select bbd_qyxx_id, cast(bbd_recruit_num as int) as bbd_recruit_num, pubdate
                  from dw.manage_recruit
                  where dt = '{max_dt}'
                  and nvl(bbd_qyxx_id,'') != ''
                  and pubdate between '{f}' and '{l}'
        """
        # return cls.exec_sql(_sql_new, entry)
        return cls.exec_sql(_sql_new, entry)\
            .groupBy("bbd_qyxx_id")\
            .agg(
                F.sum("bbd_recruit_num").alias("recruit_sum")
            )

    @classmethod
    @T.log_track
    def cal_zhuanli(cls, entry: Entry):
        column = "bbd_qyxx_id,title,publidate"
        year_month = entry.version
        max_dt = HiveUtil.newest_partition_by_month(entry.spark, 'dw.qyxx_wanfang_zhuanli', in_month_str = year_month[:6])
        entry.logger.info("dw.qyxx_wanfang_zhuanli 最新分区 --- {max_dt}".format(max_dt=max_dt))

        _sql = f"""select {column} from dw.qyxx_wanfang_zhuanli where dt = '{max_dt}' and nvl(bbd_qyxx_id,'') != '' """
        return cls.exec_sql(_sql, entry)\
            .groupBy("bbd_qyxx_id")\
            .agg(
                F.countDistinct("title").alias("zhuanli_sum")
            )

    @classmethod
    @T.log_track
    def join_index(cls, entry: Entry):
        df = cls.cal_province_city_area(entry)
        list = [cls.cal_manage_recruit(entry),cls.cal_zhuanli(entry)]

        for index_df in list:
            df = df.join(index_df,["bbd_qyxx_id"],'left')
        result = cls.cal_distribution(entry, df)
        CnUtils.save_parquet(result, f"{CnUtils.get_temp_path(entry)}/cyfb_check_data")
        return result

    @classmethod
    def cal_distribution(cls, entry: Entry, merge: DataFrame):
        column = "industry,sub_industry,province,city,area"

        def map1(capital_input):
            return int(round(capital_input / 100000000))

        udf = fun.udf(map1, IntegerType())
        return merge.groupBy(column.split(",")).agg(
                F.countDistinct("bbd_qyxx_id").alias("enterprise_num"),
                F.sum("recruit_sum").alias("recruiting_num"),
                F.sum("zhuanli_sum").alias("patent_num"),
                F.sum("regcap_amount").alias("capital_input")
            ).withColumn("update_time", fun.lit(CnUtils.now_time()))\
                .withColumn("statistics_time", fun.lit(f"{entry.version[0:4]}-{entry.version[4:6]}-{entry.version[6:8]}")) \
                .fillna(0, subset=['capital_input','patent_num','recruiting_num','enterprise_num'])\
                .withColumn("capital_input", udf("capital_input"))



class PushData:
    """
    转json
    """
    @staticmethod
    def to_push_data(row: Row) -> Row:
        tmp_data = row.asDict(True)
        push_data = dict()
        tn = "cq_xdcy_enterprise_distribution"
        push_data["tn"] = tn
        push_data["data"] = tmp_data
        return JsonUtils.to_string(push_data)

    @classmethod
    @T.log_track
    def push_data(cls, entry: Entry, data: DataFrame):
        data.createOrReplaceTempView("tt")
        data = entry.spark.sql("""
        select
            industry,
            sub_industry,
            province,
            city,
            area,
            enterprise_num,
            recruiting_num,
            patent_num,
            capital_input,
            statistics_time,
            update_time
        from tt
        """)

        CnUtils.saveToHdfs(data.rdd.map(cls.to_push_data),f"{CnUtils.get_final_path(entry)}/cq_xdcy_enterprise_distribution")


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    ## 入口
    PushData.push_data(entry, CalIndex.join_index(entry))
    # entry.spark.read.parquet("/user/cqxdcyfzyjy/20210501/input/cqxdcy/industry_divide").createOrReplaceTempView("qyxx_basic")
    #
    # df = entry.spark.read.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zsjz", header=True, sep='\t').createOrReplaceTempView("tmp")
    # entry.spark.read.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zskxx", header=True, sep='\t').createOrReplaceTempView("tmp2")
    #
    # entry.spark.sql("""
    # select
    #     R.bbd_qyxx_id,R.company_name,R.province,R.city,R.district,L.industry, L.sub_industry,R.S28,R.S29,R.S3,R.S4,R.S7,R.S8,R.S9,R.S10,R.S1,R.S2,R.S5,R.S6,R.S19,R.S20
    # from qyxx_basic L
    # inner join tmp R
    # on L.bbd_qyxx_id = R.bbd_qyxx_id
    # """).repartition(1).write.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zsjz_back",header=True, sep='\t')
    # # "bbd_qyxx_id,company_name,province,city,district,K1,K10,K12,K13,K15,K17,K18,K19,K2,K20,K21,K24,K25,K3,K33,K34,K35,K37,K4,K5,K6,K9,K22,K23,K26,K27,K28,K36"
    # # m: DataFrame = None
    # entry.spark.sql("""
    #    select
    #        R.bbd_qyxx_id,R.company_name,R.province,R.city,R.district,L.industry, L.sub_industry,
    #        R.K1,R.K10,R.K12,R.K13,R.K15,R.K17,R.K18,R.K19,R.K2,R.K20,R.K21,
    #        R.K24,R.K25,R.K3,R.K33,R.K34,R.K35,R.K37,R.K4,R.K5,R.K6,R.K9,R.K22,R.K23,R.K26,R.K27,R.K28,R.K36
    #    from qyxx_basic L
    #    inner join tmp2 R
    #    on L.bbd_qyxx_id = R.bbd_qyxx_id
    #    """).repartition(1).write.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zskxx_back", header=True,
    #                   sep='\t')
    # entry.spark.read.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zskxx_back", header=True, sep='\t').repartition(1).write.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zskxx", header=True, sep='\t',mode='overwrite')
    # entry.spark.read.csv("/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zsjz_back",
    #                      header=True, sep='\t').repartition(1).write.csv(
    #     "/user/cqxdcyfzyjy/20210501/tmp/cqxdcy/cq_xdcy_merchant/cq_xdcy_merchant_zsjz", header=True, sep='\t', mode='overwrite')


def post_check(entry: Entry):
    return True
