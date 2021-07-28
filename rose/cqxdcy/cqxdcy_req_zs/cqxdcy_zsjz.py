#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 招商价值
:Author: weifuwan@bbdservice.com
:Date: 2021-02-03 16:08
"""
import os
import datetime
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession, DataFrame

from cqxdcy.proj_common.date_util import DateUtils


def pre_check(entry: Entry):
    return True


class CnUtils:
    """
    原始路径
    """
    @classmethod
    def get_source_path(cls, entry) -> str:
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")

    """
    输出路径
    """
    @classmethod
    def get_final_path(cls, entry) -> str:
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")

    """
    临时路径
    """
    @classmethod
    def get_temp_path(cls, entry) -> str:
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")

    """
    获取过去N年当前时间
    """
    @classmethod
    def get_current_day_of_past_n_year(cls, n: int) -> str:
        return DateUtils.date2str(DateUtils.add_date(datetime.datetime.now(), years=-n))

    """
    以parquet格式将中间数据保存到指定路径
    """
    @classmethod
    def save_parquet(cls, df: DataFrame, entry: Entry, path: str, num_partitions=20):
        path = os.path.join(CnUtils.get_temp_path(entry), 'cq_xdcy_merchant', path)
        df.repartition(num_partitions).write.parquet(path, mode="overwrite")


def zsjz(entry: Entry):
    """
    """
    spark = entry.spark
    year_age_str = CnUtils.get_current_day_of_past_n_year(1)
    prop_patent_data_dt = HiveUtil.newest_partition(spark, 'dw.prop_patent_data')
    news_public_sentiment_dt = HiveUtil.newest_partition(spark, "dw.news_public_sentiment")
    news_cqzstz_dt = HiveUtil.newest_partition(spark, "dw.news_cqzstz")
    # 企业发明专利数量,patent_type=发明专利，用public_code去重，对bbd_qyxx_id计数
    S7 = spark.sql(f"""
        select
            a.bbd_qyxx_id,
            count(distinct(a.public_code)) as S7
        from
            dw.prop_patent_data a
            inner join qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.dt = '{ prop_patent_data_dt }'
            and a.patent_type = '发明专利'
        group by
            a.bbd_qyxx_id
    """)

    # 企业外观专利数量,patent_type=外观专利，用public_code去重，对bbd_qyxx_id计数
    S8 = spark.sql(f"""
        select
            a.bbd_qyxx_id,
            count(distinct(a.public_code)) as S8
        from
            dw.prop_patent_data a
            inner join qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.dt = '{ prop_patent_data_dt }'
            and a.patent_type = '外观设计'
        group by
            a.bbd_qyxx_id
    """)

    # 企业实用新型专利数量,patent_type=实用新型，用public_code去重，对bbd_qyxx_id计数
    S9 = spark.sql(f"""
        select
            a.bbd_qyxx_id,
            count(distinct(a.public_code)) as S9
        from
            dw.prop_patent_data a
            inner join qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.dt = '{ prop_patent_data_dt }'
            and a.patent_type = '实用新型'
        group by
            a.bbd_qyxx_id
    """)

    # 企业舆情指向情感分数
    S28 = spark.sql(f"""
        select
            a.bbd_qyxx_id,
            count(a.bbd_xgxx_id) as S28
        from
            dw.news_public_sentiment a
            inner join qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.pubdate >= '{year_age_str}'
            and a.dt = '{news_public_sentiment_dt}'
        group by
            a.bbd_qyxx_id
    """).fillna(0)

    # 企业荣誉类舆情数量,orit_senti=1
    S29 = spark.sql(f"""
        select
            a.bbd_qyxx_id,
            sum(a.gen_senti) as S29
        from
            dw.news_public_sentiment a
            inner join qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.pubdate >= '{year_age_str}'
            and a.dt = '{news_public_sentiment_dt}'
            and a.gen_senti > 0
        group by
            a.bbd_qyxx_id
    """)

    def get_num_by_nick_name_and_company_type(nick_name, company_type):
        total = 0
        nick_names = [x for x in nick_name.replace("，", ",").split(",") if x]
        if company_type == "重庆市瞪羚企业" and "高新技术企业" in nick_names:
            total += 3
        if company_type == "重庆市牛羚企业" and "高新技术企业" in nick_names:
            total += 5

        if company_type == "国家企业技术中心" or company_type == "重庆市企业技术中心" or company_type == "国家级科技企业孵化器" or company_type == "国家众创空间":
            total += len(nick_names) * 2

        if company_type == "重庆市科技企业孵化器" or "科技部科技型中小企业":
            total += len(nick_names)

        if company_type == "国家技术创新示范企业":
            total += len(nick_names) * 9

        return total

    spark.udf.register("get_num_by_nick_name_and_company_type", get_num_by_nick_name_and_company_type, IntegerType())

    S10 = spark.sql(f"""
        select
            bbd_qyxx_id,
            sum(S10_tmp) as S10
        from(
            select
                a.bbd_qyxx_id,
                get_num_by_nick_name_and_company_type(b.nick_name, b.company_type) as S10_tmp
            from
                qyxx_basic a
                join (select * 
                      from dw.news_cqzstz 
                      where dt = {news_cqzstz_dt} and bbd_qyxx_id is not null ) b on a.bbd_qyxx_id = b.bbd_qyxx_id
            )
        group by bbd_qyxx_id
    """)

    for i in [[S7, "S7"], [S8, "S8"], [S9, "S9"], [S28, "S28"], [S29, "S29"],
              [S10, "S10"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


def load_tag_end_qyxx(entry: Entry):
    path = os.path.join(CnUtils.get_source_path(entry), 'industry_divide')
    entry.spark.read.parquet(path).persist().createOrReplaceTempView("qyxx_basic")


def indexPool(entry: Entry):
    spark = entry.spark
    # 注意顺序一一对应
    index = {
        "S1": "qy_nsxypj",
        "S2": "qy_qs_num",
        "S5": "qy_sb_num",
        "S6": "qy_rz_num",
        "S19": "qy_zprs_num",
        "S20": "qy_ssjyszp_num"
    }

    sql = f"""
        select L.bbd_qyxx_id,{",".join([f"{v} as {k}" for k,v in index.items()])} 
        from dw.index_pool L 
        inner join qyxx_basic R on L.bbd_qyxx_id = R.bbd_qyxx_id  
        where nvl(L.bbd_qyxx_id,'') != ''
    """
    print(sql)
    pool_df = spark.sql(sql)
    CnUtils.save_parquet(pool_df, entry, 'zsjz_index_pool')


def S4_S3(entry: Entry):
    spark = entry.spark
    sql = f"""
        select L.bbd_qyxx_id,
               (L.qy_txjsqyzz_is+L.qy_jzlqyzz_is+L.qy_gcjlzz_is+L.qy_xxxtgcjlzz_is+L.qy_srrz_is+L.qy_gycpscxkz_is+L.qy_nyscxkz_is+L.qy_spscxkz_is+L.qy_ypscxkz_is+L.qy_ypjyxkz_is+L.qy_hzpscxkz_is+L.qy_GMPrz_is+L.qy_hgdj_is+L.qy_cktkxk_is) as S4 
        from dw.index_pool L 
        inner join qyxx_basic R on L.bbd_qyxx_id = R.bbd_qyxx_id 
        where nvl(L.bbd_qyxx_id,'') != ''"""
    pool_df = spark.sql(sql)

    CnUtils.save_parquet(pool_df, entry, 'S4')

    sql = f"""
        select L.bbd_qyxx_id,
               (L.qy_txjsqyzz_is+L.qy_jzlqyzz_is+L.qy_gcjlzz_is+L.qy_gcjlzz_is+L.qy_GMPrz_is+L.qy_hgdj_is) as S3 
        from dw.index_pool L 
        inner join qyxx_basic R on L.bbd_qyxx_id = R.bbd_qyxx_id 
        where nvl(L.bbd_qyxx_id,'') != ''
    """
    pool_df = spark.sql(sql)

    CnUtils.save_parquet(pool_df, entry, 'S3')


def merge_data(entry: Entry):
    spark = entry.spark
    index_path_prefix = os.path.join(CnUtils.get_temp_path(entry), 'cq_xdcy_merchant')
    path = [
        os.path.join(index_path_prefix, 'S28'),
        os.path.join(index_path_prefix, 'S29'),
        os.path.join(index_path_prefix, 'S3'),
        os.path.join(index_path_prefix, 'S4'),
        os.path.join(index_path_prefix, 'S7'),
        os.path.join(index_path_prefix, 'S8'),
        os.path.join(index_path_prefix, 'S9'),
        os.path.join(index_path_prefix, 'S10'),
        os.path.join(index_path_prefix, 'zsjz_index_pool')
    ]

    def get_company_county():
        """
        获取省市区
        :return: DataFrame
        """
        dt = HiveUtil.newest_partition(spark, 'dw.company_county')
        df = spark.sql("""
                    select company_county, province, city, region
                    from
                        (select code as company_county, province, city, district as region,
                                    (row_number() over(partition by code order by tag )) as row_num
                        from dw.company_county
                        where dt = '{dt}' and tag != -1) b
                    where row_num = 1
                """.format(dt=dt))
        return df

    def cal_province_city_region():
        """
        计算公司省 市 区
        :return: DataFrame
        """
        get_company_county().createOrReplaceTempView('company_country')
        sql = """
                select
                    R.*,
                    L.province,
                    L.city,
                    L.region
                from company_country L
                right join qyxx_basic R
                on L.company_county = R.company_county
        """
        company_county_df = spark.sql(sql)

        return company_county_df

    d = []
    for p in path:
        df = spark.read.parquet(p)
        if df:
            d.append(df)

    cal_province_city_region().createOrReplaceTempView("tag_end_qyxx_info")

    df = spark.sql(
        """
        select bbd_qyxx_id,company_name,province,city,region as district,industry,sub_industry 
        from tag_end_qyxx_info
        """
    )

    for _df in d:
        df = df.join(_df, "bbd_qyxx_id", "left").fillna(0)

    df.repartition(1).write.csv(os.path.join(index_path_prefix, 'cq_xdcy_merchant_zsjz'),
                                mode='overwrite', header=True, sep="\t")


def main(entry: Entry):
    load_tag_end_qyxx(entry)
    zsjz(entry)
    indexPool(entry)
    S4_S3(entry)
    merge_data(entry)


def post_check(entry: Entry):
    return True
