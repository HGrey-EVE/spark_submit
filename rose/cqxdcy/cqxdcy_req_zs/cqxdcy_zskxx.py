#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 招商可行性
:Author: lirenchao@bbdservice.com
:Date: 2021-02-03 16:08
"""
import os
from typing import List, Set
from datetime import date, datetime
from whetstone.core.entry import Entry
from pyspark.sql import SparkSession, DataFrame
from cqxdcy.proj_common.hive_util import HiveUtil
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
        return DateUtils.date2str(DateUtils.add_date(datetime.now(), years=-n))

    """
    以parquet格式将中间数据保存到指定路径
    """
    @classmethod
    def save_parquet(cls, df: DataFrame, entry: Entry, path: str, num_partitions=20):
        path = os.path.join(CnUtils.get_temp_path(entry), 'cq_xdcy_merchant', path)
        df.repartition(num_partitions).write.parquet(path, mode="overwrite")


def load_tag_end_qyxx(entry: Entry):
    path = os.path.join(CnUtils.get_source_path(entry), 'industry_divide')
    entry.spark.read.parquet(path).persist().createOrReplaceTempView("qyxx_basic")


def _get_time_sq(spark: SparkSession, temp_view_name: str) -> Set[List[str]]:
    qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhsy")
    four_year_age_str = CnUtils.get_current_day_of_past_n_year(4)
    # 获取最近4年的结果
    spark.sql(f"""
        select
            a.report_date,
            b.industry,
            sum(nvl(cast(a.total_revenue as int),0)) as total_revenue
        from
            dw.qyxx_jqka_cwzb_zhsy a
        inner join
            qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
            and a.report_date >= '{four_year_age_str}'
        group by
            a.report_date,
            b.industry
    """).persist().createOrReplaceTempView(temp_view_name)

    # 获取最新的三年的周期
    # 获取最近的一个报告周期
    print("获取最新的三年的周期\n" * 10)
    max_report_date: date = spark.sql(f"""
        select DISTINCT report_date 
        from {temp_view_name} 
        order by report_date desc 
        limit 1
    """).take(1)[0]['report_date']
    max_report_date = datetime(max_report_date.year, max_report_date.month, max_report_date.day)
    if max_report_date.month == 12:
        latest_period_date = [max_report_date]
    else:
        latest_period_date = [
            max_report_date,
            datetime(max_report_date.year - 1, 12, 31),
            datetime(max_report_date.year - 1, max_report_date.month,
                     30 if max_report_date.month in [6, 9] else 31)
        ]
    year_ago_period_list = [
        DateUtils.date2str(DateUtils.add_date(x, years=-1))
        for x in latest_period_date
    ]
    three_year_ago_period_list = [
        DateUtils.date2str(DateUtils.add_date(x, years=-2))
        for x in latest_period_date
    ]
    latest_period_list = [DateUtils.date2str(x) for x in latest_period_date]
    return latest_period_list, year_ago_period_list, three_year_ago_period_list


def zskxx_k_1_2_3(entry: Entry):
    """招商大数据-招商可行性大数据： https://192.168.112.201/svn/higgs/项目/2029-重庆市现代产业发展研究院大数据产业招商投资平台/1-过程库/1-正式研发/2-产品需求/模型文档/招商模块-模型指标说明_v5.0.xls
    """
    spark = entry.spark
    temp_view_name = "zskxx_hyys"
    latest_period_list, year_ago_period_list, three_year_ago_period_list = _get_time_sq(spark, temp_view_name)
    print(f"{latest_period_list},{year_ago_period_list},{three_year_ago_period_list},bbders")
    print("xxxxxx\n" * 10)

    def _get_year_total_revenue(year_period_list: List[str], key_name: str) -> DataFrame:
        if len(year_period_list) == 1:
            return spark.sql(f"""
            select
                industry,total_revenue as {key_name}
            from
                {temp_view_name}
            where
                report_date = '{year_period_list[0]}'
        """)
        else:
            return spark.sql(f"""
            select
                a.industry,
                a.total_revenue+b.total_revenue-c.total_revenue as {key_name}
            from
                (
                    select
                        industry,
                        total_revenue
                    from
                        zskxx_hyys
                    where
                        report_date = '{year_period_list[0]}'
                ) a
                join (
                    select
                        industry,
                        total_revenue
                    from
                        zskxx_hyys
                    where
                        report_date = '{year_period_list[1]}'
                ) b on a.industry = b.industry
                join
                (
                    select
                        industry,
                        total_revenue
                    from
                        zskxx_hyys
                    where
                        report_date = '{year_period_list[2]}'
                ) c on a.industry = c.industry
        """)

    K1 = _get_year_total_revenue(latest_period_list, "K1")
    print("K1\n" * 10)
    _get_year_total_revenue(year_ago_period_list, "last_K1").join(
        K1, "industry", "left").fillna(0).createOrReplaceTempView("k2_temp")
    K2 = spark.sql(f"""
            select
                industry,K1/last_K1 as K2
            from
                k2_temp
        """)
    _get_year_total_revenue(three_year_ago_period_list, "three_year_ago_K1").join(
        K1, "industry", "left").fillna(0).createOrReplaceTempView("k3_temp")
    K3 = spark.sql("""
            select
                industry,power((K1/three_year_ago_K1),1/3)-1 as K3
            from
                k3_temp
        """)

    def _join_industry_to_bbd_qyxx_id(info) -> DataFrame:
        df, key = info
        return spark.sql(f"""select * from qyxx_basic""").join(df, "industry").select("bbd_qyxx_id", key)

    K1, K2, K3 = map(_join_industry_to_bbd_qyxx_id, [[K1, "K1"], [K2, "K2"], [K3, "K3"]])

    for i in [[K1, "K1"], [K2, "K2"], [K3, "K3"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


def zskxx_k_4_5_6(entry: Entry):
    spark = entry.spark
    qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhsy")
    temp_view_name = "zskxx_hyys"
    four_year_age_str = CnUtils.get_current_day_of_past_n_year(4)
    latest_period_list, year_ago_period_list, three_year_ago_period_list = _get_time_sq(spark, temp_view_name)
    spark.sql(f"""
        select
            a.report_date,
            b.industry,
            b.sub_industry,
            sum(nvl(cast(a.total_revenue as int),0)) as sub_total_revenue
        from
            dw.qyxx_jqka_cwzb_zhsy a
        inner join
            qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
            and a.report_date >= '{four_year_age_str}'
        group by
            a.report_date,
            b.industry,
            b.sub_industry
    """).createOrReplaceTempView("sub_total")
    # spark.sql(f"""
    #     select *
    #     from sub_total a
    #     left join zskxx_hyys b on a.report_date = b.report_date and a.industry = b.sub_industry
    # """).createOrReplaceTempView("sub_total")
    spark.sql(f"""
        select a.*,b.sub_industry,b.sub_total_revenue 
        from {temp_view_name} a 
        left join sub_total b on a.report_date = b.report_date and a.industry = b.industry
    """).createOrReplaceTempView("sub_temp")

    def _get_sub_industry_count(year_period_list: List[str]):
        if len(year_period_list) == 1:
            return spark.sql(f"""
            select
                industry,sub_industry,total_revenue,sub_total_revenue
            from 
                sub_temp
            where
                report_date = '{year_period_list[0]}'
        """)
        else:
            return spark.sql(f"""
            select
                a.industry,
                a.sub_industry,
                (a.total_revenue+b.total_revenue-c.total_revenue) as total_revenue,
                (a.sub_total_revenue+b.sub_total_revenue-c.sub_total_revenue) as sub_total_revenue
            from
                (
                    select
                        industry,
                        sub_industry,
                        total_revenue,
                        sub_total_revenue
                    from
                        sub_temp
                    where
                        report_date = '{year_period_list[0]}'
                ) a
                join (
                    select
                        industry,
                        sub_industry,
                        total_revenue,
                        sub_total_revenue
                    from
                        sub_temp
                    where
                        report_date = '{year_period_list[1]}'
                ) b on a.industry = b.industry and a.sub_industry = b.sub_industry
                join 
                (
                    select
                        industry,
                        sub_industry,
                        total_revenue,
                        sub_total_revenue
                    from
                        sub_temp
                    where
                        report_date = '{year_period_list[2]}'
                ) c on a.industry = c.industry and a.sub_industry = c.sub_industry
        """)

    _get_sub_industry_count(latest_period_list).createOrReplaceTempView("K4_tmp")
    K4 = spark.sql(f"""
        select a.bbd_qyxx_id,(b.sub_total_revenue/b.total_revenue) as K4 
        from qyxx_basic a 
        join K4_tmp b on a.sub_industry = b.sub_industry and a.industry = b.industry
    """)
    K4.createOrReplaceTempView("K4_for_K5_K6_tmp")

    _get_sub_industry_count(year_ago_period_list).createOrReplaceTempView("K5_tmp")

    spark.sql(f"""
        select a.bbd_qyxx_id,(b.sub_total_revenue/b.total_revenue) as K5 
        from qyxx_basic a 
        join K5_tmp b on a.sub_industry = b.sub_industry and a.industry = b.industry
    """).createOrReplaceTempView("K5_tmp")

    K5 = spark.sql(f"""
        select a.bbd_qyxx_id,(a.K4/b.K5) as K5 
        from K4_for_K5_K6_tmp a 
        join K5_tmp b on a.bbd_qyxx_id = b.bbd_qyxx_id
    """)

    _get_sub_industry_count(three_year_ago_period_list).createOrReplaceTempView("K6_tmp")

    spark.sql(f"""
        select a.bbd_qyxx_id,(b.sub_total_revenue/b.total_revenue) as K6 
        from qyxx_basic a 
        join K6_tmp b on a.sub_industry = b.sub_industry and a.industry = b.industry
    """).createOrReplaceTempView("K6_tmp")

    K6 = spark.sql(f"""
        select a.bbd_qyxx_id,power((a.K4/b.K6),1/3)-1 as K6 
        from K4_for_K5_K6_tmp a 
        join K6_tmp b on a.bbd_qyxx_id = b.bbd_qyxx_id
    """)

    for i in [[K4, "K4"], [K5, "K5"], [K6, "K6"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


def zskxx_k_9_10_15(entry: Entry):
    spark = entry.spark
    qyxx_basic_dt = HiveUtil.newest_partition(spark, "dw.qyxx_basic")
    manage_bidwinning_dt = HiveUtil.newest_partition(spark, "dw.manage_bidwinning")
    qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhsy")
    # 第一步，先求所有在招标中的招标的（甲方）上市公司
    spark.sql(f"""
        select
            DISTINCT a.bbd_qyxx_id
        from
            dw.qyxx_basic a
        join
            dw.manage_bidwinning b on a.company_name = b.company_name_invite
        where
            a.dt = '{qyxx_basic_dt}'
            and b.dt = '{manage_bidwinning_dt}'
            and a.ipo_company = '上市公司'
        """).createOrReplaceTempView("zb_ssgs")
    # 算出目标公司（manage_bidwinning.company_win/tag_end_qyxx_info.bbd_qyxx_id）（乙方）的最新财报时间,只有上市公司才有财报
    spark.sql(f"""
        select
        DISTINCT
            a.bbd_qyxx_id,
            a.company_name,
            max(b.report_date) as max_report_date
        from
            qyxx_basic a
            join dw.qyxx_jqka_cwzb_zhsy b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            b.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
        group by
            a.company_name,
            a.bbd_qyxx_id
        """).createOrReplaceTempView("yf_zxsj")
    # 算出对应的甲方/乙方关系，同为上市公司的，那就很少了的数据了
    spark.sql(f"""
        select
            DISTINCT
            b.bbd_qyxx_id as jf_qyxx_id,
            c.bbd_qyxx_id as yf_qyxx_id
        from
            dw.manage_bidwinning a
            JOIN dw.qyxx_basic b on a.company_name_invite = b.company_name
            JOIN yf_zxsj c on a.company_name_win =c.company_name
        where
            a.dt = '{manage_bidwinning_dt}'
            and b.dt = '{qyxx_basic_dt}'
            and b.ipo_company = '上市公司'
            and a.pubdate between add_months(c.max_report_date,-12) and c.max_report_date
            and b.bbd_qyxx_id != c.bbd_qyxx_id
        """).createOrReplaceTempView("xxxx")
    # 算出甲方的最新财报时间
    spark.sql(f"""
        select
            DISTINCT
            a.jf_qyxx_id,
            max(b.report_date) as max_report_date
        from
            xxxx a
            join dw.qyxx_jqka_cwzb_zhsy b on a.jf_qyxx_id = b.bbd_qyxx_id
        where
            b.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
        group by
            a.jf_qyxx_id
        """).createOrReplaceTempView("ppppp")
    # 算出甲方的最近一年营收
    spark.sql(f"""
        select
            t2.bbd_qyxx_id,
            nvl(cast(t2.total_revenue as int), 0) total_revenue
        from
            ppppp t1
            join dw.qyxx_jqka_cwzb_zhsy t2 on t1.jf_qyxx_id = t2.bbd_qyxx_id and t1.max_report_date = t2.report_date
        where
            month(t1.max_report_date) = 12
            and t2.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
        UNION ALL
        select
            pa.bbd_qyxx_id as bbd_qyxx_id,
            (
                nvl(cast(pa.total_revenue as int), 0) + nvl(cast(pb.total_revenue as int), 0) - nvl(cast(pc.total_revenue as int), 0)
            ) as total_revenue
        from
            ppppp a
            join dw.qyxx_jqka_cwzb_zhsy pa on a.jf_qyxx_id = pa.bbd_qyxx_id and pa.report_date = a.max_report_date
            join dw.qyxx_jqka_cwzb_zhsy pb on a.jf_qyxx_id = pb.bbd_qyxx_id and pb.report_date = Date(
                concat(
                    year(max_report_date) -1,
                    '-12-31'
                )
            )
            join dw.qyxx_jqka_cwzb_zhsy pc on a.jf_qyxx_id = pc.bbd_qyxx_id and pc.report_date = Date(
                concat(
                    year(max_report_date)-1,
                    '-',
                    month(max_report_date),
                    '-',
                    CASE
                        WHEN month(max_report_date) in (6, 9) THEN '30'
                        ELSE '31'
                    END
                )
            )
        where
            month(a.max_report_date) != 12
            and pa.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
            and pb.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
            and pc.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
        """).createOrReplaceTempView("jf_ys")

    K9 = spark.sql("""
            SELECT
                t1.bbd_qyxx_id,
                t2.K9
            from
                qyxx_basic t1
                left join (
                    select
                        a.bbd_qyxx_id,
                        sum(a._rank) as K9
                    from
                        (
                            select
                                a.yf_qyxx_id as bbd_qyxx_id,
                                rank() over(PARTITION by b.total_revenue ORDER BY  b.total_revenue desc) as _rank
                            from
                                xxxx a
                                join jf_ys b on a.jf_qyxx_id = b.bbd_qyxx_id
                        ) a
                    where
                        a._rank <= 5
                    group by
                        a.bbd_qyxx_id
                ) t2 on t1.bbd_qyxx_id = t2.bbd_qyxx_id
            """)

    # K10 先求出三年前的营收
    spark.sql(f"""
        select
            t2.bbd_qyxx_id,
            nvl(cast(t2.total_revenue as int), 0) total_revenue
        from
            ppppp t1
            join dw.qyxx_jqka_cwzb_zhsy t2
            on t1.jf_qyxx_id = t2.bbd_qyxx_id  and t2.report_date = add_months(t1.max_report_date,-36)
        where
            month(t1.max_report_date) = 12
            and t2.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
        UNION ALL
        select
            pa.bbd_qyxx_id as bbd_qyxx_id,
            (
                nvl(cast(pa.total_revenue as int), 0) + nvl(cast(pb.total_revenue as int), 0) - nvl(cast(pc.total_revenue as int), 0)
            ) as total_revenue
        from
            ppppp a
            join dw.qyxx_jqka_cwzb_zhsy pa
            on a.jf_qyxx_id = pa.bbd_qyxx_id and pa.report_date = add_months(a.max_report_date,-36)
            join dw.qyxx_jqka_cwzb_zhsy pb
            on a.jf_qyxx_id = pb.bbd_qyxx_id and pb.report_date = Date(
                concat(
                    year(max_report_date) -4,
                    '-12-31'
                )
            )
            join dw.qyxx_jqka_cwzb_zhsy pc on a.jf_qyxx_id = pc.bbd_qyxx_id and pc.report_date = Date(
                concat(
                    year(max_report_date)-4,
                    '-',
                    month(max_report_date),
                    '-',
                    CASE
                        WHEN month(max_report_date) in (6, 9) THEN '30'
                        ELSE '31'
                    END
                )
            )
        where
            month(a.max_report_date) != 12
            and pa.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
            and pb.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
            and pc.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
        """).createOrReplaceTempView("K10_jf_ys")

    spark.sql("""
            SELECT
                t1.bbd_qyxx_id,
                t2.K10_tmp
            from
                qyxx_basic t1
                left join (
                    select
                        a.bbd_qyxx_id,
                        sum(a._rank) as K10_tmp
                    from
                        (
                            select
                                a.yf_qyxx_id as bbd_qyxx_id,
                                rank() over(PARTITION by b.total_revenue ORDER BY  b.total_revenue desc) as _rank
                            from
                                xxxx a
                                join K10_jf_ys b on a.jf_qyxx_id = b.bbd_qyxx_id
                        ) a
                    where
                        a._rank <= 5
                    group by
                        a.bbd_qyxx_id
                ) t2 on t1.bbd_qyxx_id = t2.bbd_qyxx_id
            """).createOrReplaceTempView("K10_tmp_1")
    tmp_path = os.path.join(CnUtils.get_temp_path(entry), 'cq_xdcy_merchant', "K9_tmp")
    K9.write.parquet(tmp_path)
    tmp_K9:DataFrame = spark.read.parquet(tmp_path).cache()
    tmp_K9.createOrReplaceTempView("K9_view_for_k10_K15")
    # K9.createOrReplaceTempView("K9_view_for_k10_K15")

    K10 = spark.sql("""
            select
                t1.bbd_qyxx_id,
                power((t1.K9/t1.K10_tmp),1/3)-1 as K10
            from (
                select
                    a.bbd_qyxx_id,
                    a.K9,
                    b.K10_tmp
                from
                    K9_view_for_k10_K15 a
                join
                    K10_tmp_1 b on a.bbd_qyxx_id = b.bbd_qyxx_id
            ) t1
        """)

    K15 = spark.sql("""
        select
            a.bbd_qyxx_id,
            b.K9 as K15
        from 
            qyxx_basic a
        join K9_view_for_k10_K15 b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.ipo_company = '上市公司'
            and a.company_province = '重庆'
        """)

    for i in [[K9, "K9"], [K10, "K10"], [K15, "K15"]]:
        CnUtils.save_parquet(i[0], entry, i[1])
    tmp_K9.unpersist()


def zskxx_k_12_13(entry: Entry):
    spark = entry.spark
    K12 = spark.sql("""
        select a.bbd_qyxx_id,a.K15_new/b.K15_over as K12 from 
        (select bbd_qyxx_id,count(*) over(partition by industry) K15_new 
         from qyxx_basic 
         where approval_date is not null and approval_date != 'null' and approval_date> '2020-02-28') a
        join (select bbd_qyxx_id,count(*) over(partition by industry) K15_over 
              from qyxx_basic 
              where cancel_date is not null and cancel_date != 'null' and cancel_date> '2020-02-28') b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """).fillna(0)

    K13 = spark.sql("""
                select 
                    bbd_qyxx_id,
                    count(*) over(partition by company_county) K13
                from 
                    qyxx_basic
                """)

    for i in [[K12, "K12"], [K13, "K13"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


def zskxx_k_17_18_19(entry: Entry):
    spark = entry.spark
    qyxx_jqka_cwzb_zhzb_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhzb")
    qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhsy")
    qyxx_jqka_cwzb_zcfz_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zcfz")
    K17 = spark.sql(f"""
            select 
                a.bbd_qyxx_id,
                (b.net_profit/b.total_revenue) as K17
            from 
                qyxx_basic a
            join (
                select 
                    bbd_qyxx_id,
                    net_profit,
                    total_revenue,
                    row_number() over(PARTITION by report_date order by report_date desc) _rank
                from 
                    dw.qyxx_jqka_cwzb_zhzb
                where
                    dt = '{qyxx_jqka_cwzb_zhzb_dt}'
                    and month(report_date) = 12
            ) b on a.bbd_qyxx_id = b.bbd_qyxx_id
            where b._rank = 1
            """)

    K18 = spark.sql(f"""
            select 
                a.bbd_qyxx_id,
                (b.net_profit/b.total_operating_cost) as K18
            from 
                qyxx_basic a
            join (
                select 
                    bbd_qyxx_id,
                    net_profit,
                    total_operating_cost,
                    row_number() over(PARTITION by report_date order by report_date desc) _rank
                from 
                    dw.qyxx_jqka_cwzb_zhsy
                where
                    dt = '{qyxx_jqka_cwzb_zhsy_dt}'
                    and month(report_date) = 12
            ) b on a.bbd_qyxx_id = b.bbd_qyxx_id
            where b._rank = 1
            """)

    K19 = spark.sql(f"""
            select
                t1.bbd_qyxx_id,
                t2.net_profit-t2.dividends_payable/t2.total_assets as K19
            from
                qyxx_basic t1
                join (
                    select
                        *
                    from
                        (
                            select
                                a.bbd_qyxx_id,
                                a.net_profit,
                                b.dividends_payable,
                                b.total_assets,
                                row_number() over(
                                    PARTITION BY a.report_date
                                    ORDER BY
                                        a.report_date desc
                                ) _rank
                            from
                                dw.qyxx_jqka_cwzb_zhsy a
                                join dw.qyxx_jqka_cwzb_zcfz b on a.bbd_qyxx_id = b.bbd_qyxx_id
                                and a.report_date = b.report_date
                            where
                                a.dt = '{qyxx_jqka_cwzb_zhsy_dt}'
                                and b.dt = '{qyxx_jqka_cwzb_zcfz_dt}'
                                and month(a.report_date) = 12
                        )
                    where
                        _rank = 1
                ) t2 on t1.bbd_qyxx_id = t2.bbd_qyxx_id
                
            """)

    for i in [[K17, "K17"], [K18, "K18"], [K19, "K19"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


def zskxx_k_20_21_24(entry: Entry):
    spark = entry.spark
    qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhsy")
    qyxx_jqka_cwzb_zcfz_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zcfz")
    qyxx_jqka_cwzb_zhzb_dt = HiveUtil.newest_partition(spark, "dw.qyxx_jqka_cwzb_zhzb")
    K24 = spark.sql(f"""
        select distinct a.bbd_qyxx_id, b.r_and_d_cost as K24
        from qyxx_basic a
        join (select r_and_d_cost, report_date, bbd_qyxx_id, bbd_xgxx_id  
              from (select coalesce(r_and_d_cost, 0) as r_and_d_cost, report_date, bbd_qyxx_id, bbd_xgxx_id,
                           row_number() over(partition by bbd_qyxx_id order by report_date desc) as rn 
                    from dw.qyxx_jqka_cwzb_zhsy 
                    where dt='{qyxx_jqka_cwzb_zhsy_dt}' and date_format(report_date, 'MM') = '12') c 
              where c.rn = 1 ) b
        on a.bbd_qyxx_id = b.bbd_qyxx_id
    """)

    K20 = spark.sql(f"""
          select distinct a.bbd_qyxx_id, b.return_on_equity as K20
          from qyxx_basic a
          join (select return_on_equity, report_date, bbd_qyxx_id, bbd_xgxx_id  
                from (select coalesce(return_on_equity, 0) as return_on_equity, report_date, bbd_qyxx_id, bbd_xgxx_id,
                             row_number() over(partition by bbd_qyxx_id order by report_date desc) as rn 
                      from dw.qyxx_jqka_cwzb_zhzb 
                      where dt='{qyxx_jqka_cwzb_zhzb_dt}' and date_format(report_date, 'MM') = '12' ) c 
                where c.rn = 1 ) b
          on a.bbd_qyxx_id = b.bbd_qyxx_id
      """)

    K21_net_profit = spark.sql(f"""
            select distinct a.bbd_qyxx_id, b.net_profit as net_profit
            from qyxx_basic a
            join (select net_profit, report_date, bbd_qyxx_id, bbd_xgxx_id  
                  from (select coalesce(net_profit, 0) as net_profit, report_date, bbd_qyxx_id, bbd_xgxx_id,
                               row_number() over(partition by bbd_qyxx_id order by report_date desc) as rn 
                        from dw.qyxx_jqka_cwzb_zhsy 
                        where dt='{qyxx_jqka_cwzb_zhsy_dt}' and date_format(report_date, 'MM') = '12' ) c 
                  where c.rn = 1 ) b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
        """).createOrReplaceTempView("K21_net_profit")

    K21_dividends_payable_and_total_assets = spark.sql(f"""
                select distinct a.bbd_qyxx_id, b.dividends_payable, b.total_assets
                from qyxx_basic a
                join (select dividends_payable, total_assets, report_date, bbd_qyxx_id, bbd_xgxx_id  
                      from (select coalesce(dividends_payable, 0) as dividends_payable, 
                                   coalesce(total_assets, 0) as total_assets, report_date, bbd_qyxx_id, bbd_xgxx_id,
                                   row_number() over(partition by bbd_qyxx_id order by report_date desc) as rn 
                            from dw.qyxx_jqka_cwzb_zcfz 
                            where dt='{qyxx_jqka_cwzb_zcfz_dt}' and date_format(report_date, 'MM') = '12' ) c 
                      where c.rn = 1 ) b
                on a.bbd_qyxx_id = b.bbd_qyxx_id
            """).createOrReplaceTempView("K21_dividends_payable_and_total_assets")

    K21 = spark.sql("""
        SELECT 
        a.bbd_qyxx_id,
        CASE WHEN a.total_assets != 0 then round((b.net_profit - a.dividends_payable) / a.total_assets * 100, 0)
               else 0
               end as K21
        FROM K21_dividends_payable_and_total_assets a 
        JOIN K21_net_profit b
        ON a.bbd_qyxx_id  = b.bbd_qyxx_id
    """)

    for i in [[K24, "K24"], [K21, "K21"], [K20, "K20"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


def zskxx_k_25(entry: Entry):
    spark = entry.spark
    prop_patent_data_dt = HiveUtil.newest_partition(spark, "dw.prop_patent_data")
    K25 = spark.sql(f"""
        select
            a.bbd_qyxx_id,
            count(distinct(a.public_code)) as K25
        from
            dw.prop_patent_data a
            inner join qyxx_basic b on a.bbd_qyxx_id = b.bbd_qyxx_id
        where
            a.dt = '{ prop_patent_data_dt }'
            and a.patent_type = '发明专利'
        group by
            a.bbd_qyxx_id
    """)
    for i in [[K25, "K25"]]:
        CnUtils.save_parquet(i[0], entry, i[1])


# 企业对外投资金额 K37
def cal_qy_dy_tz_je(entry: Entry):
    spark = entry.spark
    qyxx_jqka_ipo_kggs = "dw.qyxx_jqka_ipo_kggs"
    dt = HiveUtil.newest_partition(spark, qyxx_jqka_ipo_kggs)
    spark.sql(f""" 
        select
            a.*
        from (select 
                * 
            from {qyxx_jqka_ipo_kggs} 
            where dt = '{dt}') a
        inner join qyxx_basic b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        """).createOrReplaceTempView("tmp")
    max_report_date = spark.sql(
        """
        select report_date 
        from (select report_date, row_number() over(order by report_date desc) rank 
              from tmp) a 
        where rank=1
        """).collect()[0]
    max_report_date = max_report_date['report_date']
    max_report_date = str(max_report_date)
    dt = datetime.strptime(max_report_date, '%Y-%m-%d')
    max_report_date_start = DateUtils.add_date(dt, years=-1)
    cal_qy_dy_tz_je_df = spark.sql(f"""
        select
            bbd_qyxx_id,
            sum(amount) K37
        from (
            select
                bbd_qyxx_id,
                invest_amount,
                case when invest_amount regexp('.*万元.*') then cast(substr(invest_amount,0,length(invest_amount)-2) as double) * 10000 
                     when invest_amount regexp('.*亿元.*') then cast(substr(invest_amount,0,length(invest_amount)-2) as double) * 100000000 
                     when invest_amount regexp('.*元.*') then cast(substr(invest_amount,0,length(invest_amount)-2) as double) * 1 
                else 0 end amount
            from ( 
                select 
                    bbd_qyxx_id,
                    invest_amount
                from tmp where report_date between '{max_report_date_start}' and '{max_report_date}'
            ) a
        ) m
        group by bbd_qyxx_id
    """)

    CnUtils.save_parquet(cal_qy_dy_tz_je_df, entry, "K37")


# 企业对外投资公司数量 K35
def qy_dy_tz_gs_num(entry: Entry):
    spark = entry.spark
    off_line_relations = "dw.off_line_relations"
    dt = HiveUtil.newest_partition(spark, off_line_relations)
    qy_dy_tz_gs_num_df = spark.sql(f"""
        select
            bbd_qyxx_id,count(distinct destination_bbd_id) as K35
        from (
            select
                L.bbd_qyxx_id,
                L.source_bbd_id,
                L.destination_bbd_id,
                L.source_degree,
                L.destination_degree,
                L.source_isperson,
                L.destination_isperson,
                L.relation_type
            from (
                select 
                    * 
                from {off_line_relations}
                where dt = '{dt}' 
                and source_degree = 0
                and destination_degree = 1
                and relation_type = 'INVEST'
                and destination_isperson = 0
            ) L 
            inner join qyxx_basic R
            on L.bbd_qyxx_id = R.bbd_qyxx_id
        ) T
        group by bbd_qyxx_id
    """)

    CnUtils.save_parquet(qy_dy_tz_gs_num_df, entry, "K35")


# 法人股东所在省份数量 K34
def fm_gd_sf_num(entry: Entry):
    spark = entry.spark
    off_line_relations = "dw.off_line_relations"
    dt = HiveUtil.newest_partition(spark, off_line_relations)
    fm_gd_sf_num_df = spark.sql(f"""
            select
                bbd_qyxx_id,count(distinct company_province) as K34
            from (
                select
                    L.bbd_qyxx_id,
                    L.source_bbd_id,
                    L.destination_bbd_id,
                    L.source_degree,
                    L.destination_degree,
                    L.source_isperson,
                    L.destination_isperson,
                    L.relation_type,
                    R.company_province
                from (
                    select 
                        * 
                    from {off_line_relations}
                    where dt = '{dt}' 
                    and source_degree = 1
                    and destination_degree = 0
                    and relation_type = 'INVEST'
                    and destination_isperson = 0
                ) L 
                inner join qyxx_basic R
                on L.source_bbd_id = R.bbd_qyxx_id
            ) T
            group by bbd_qyxx_id
        """)

    CnUtils.save_parquet(fm_gd_sf_num_df, entry, "K34")


# 对外投资公司所在身份 K33
def dw_tz_gs_sf(entry: Entry):
    spark = entry.spark
    off_line_relations = "dw.off_line_relations"
    dt = HiveUtil.newest_partition(spark, off_line_relations)
    dw_tz_gs_sf_df = spark.sql(f"""
                select
                    bbd_qyxx_id,count(distinct company_province) as K33
                from (
                    select
                        L.bbd_qyxx_id,
                        L.source_bbd_id,
                        L.destination_bbd_id,
                        L.source_degree,
                        L.destination_degree,
                        L.source_isperson,
                        L.destination_isperson,
                        L.relation_type,
                        R.company_province
                    from (
                        select 
                            * 
                        from {off_line_relations}
                        where dt = '{dt}' 
                        and source_degree = 0
                        and destination_degree = 1
                        and relation_type = 'INVEST'
                        and destination_isperson = 0
                    ) L 
                    inner join qyxx_basic R
                    on L.destination_bbd_id = R.bbd_qyxx_id
                ) T
                group by bbd_qyxx_id
            """)

    CnUtils.save_parquet(dw_tz_gs_sf_df, entry, "K33")


def indexPool(entry: Entry):
    spark = entry.spark
    # 注意顺序一一对应
    index = {
        "K22": "qy_nsxypj",
        "K23": "qy_qs_num",
        "K26": "qy_djgdwtz_num",
        "K27": "qy_ggdwtz_num",
        "K28": "qy_zrrgddwtz_num",
        "K36": "qy_fzjg_num"
    }
    sql = f"""
        select L.bbd_qyxx_id,{",".join([f"{v} as {k}" for k,v in index.items()])} 
        from dw.index_pool L 
        inner join qyxx_basic R on L.bbd_qyxx_id = R.bbd_qyxx_id  
        where nvl(L.bbd_qyxx_id,'') != ''"""
    print(sql)
    pool_df = spark.sql(sql)

    CnUtils.save_parquet(pool_df, entry, "zsmk_index_pool")


def merge_data(entry: Entry):
    spark = entry.spark
    index_path_prefix = os.path.join(CnUtils.get_temp_path(entry), 'cq_xdcy_merchant')
    path = [
        os.path.join(index_path_prefix, 'K1'),
        os.path.join(index_path_prefix, 'K10'),
        os.path.join(index_path_prefix, 'K12'),
        os.path.join(index_path_prefix, 'K13'),
        os.path.join(index_path_prefix, 'K15'),
        os.path.join(index_path_prefix, 'K17'),
        os.path.join(index_path_prefix, 'K18'),
        os.path.join(index_path_prefix, 'K19'),
        os.path.join(index_path_prefix, 'K2'),
        os.path.join(index_path_prefix, 'K20'),
        os.path.join(index_path_prefix, 'K21'),
        os.path.join(index_path_prefix, 'K24'),
        os.path.join(index_path_prefix, 'K25'),
        os.path.join(index_path_prefix, 'K3'),
        os.path.join(index_path_prefix, 'K33'),
        os.path.join(index_path_prefix, 'K34'),
        os.path.join(index_path_prefix, 'K35'),
        os.path.join(index_path_prefix, 'K37'),
        os.path.join(index_path_prefix, 'K4'),
        os.path.join(index_path_prefix, 'K5'),
        os.path.join(index_path_prefix, 'K6'),
        os.path.join(index_path_prefix, 'K9'),
        os.path.join(index_path_prefix, 'zsmk_index_pool')
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
        spark.read.parquet(os.path.join(CnUtils.get_source_path(entry), 'industry_divide')).dropDuplicates(
            ['bbd_qyxx_id']).createOrReplaceTempView('qyxx_basic')

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
        """)

    for _df in d:
        df = df.join(_df, "bbd_qyxx_id", "left").fillna(0)

    df.coalesce(1).write.csv(os.path.join(index_path_prefix, 'cq_xdcy_merchant_zskxx'),
                             mode='overwrite', header=True, sep="\t")


def main(entry: Entry):
    load_tag_end_qyxx(entry)
    zskxx_k_1_2_3(entry)
    zskxx_k_4_5_6(entry)
    zskxx_k_9_10_15(entry)
    zskxx_k_12_13(entry)
    zskxx_k_17_18_19(entry)
    zskxx_k_20_21_24(entry)
    zskxx_k_25(entry)
    # 企业对外投资金额 K37
    cal_qy_dy_tz_je(entry)
    # 企业对外投资公司数量 K35
    qy_dy_tz_gs_num(entry)
    # 法人股东所在省份数量 K34
    fm_gd_sf_num(entry)
    # 对外投资公司所在身份 K33
    dw_tz_gs_sf(entry)
    indexPool(entry)
    merge_data(entry)


def post_check(entry: Entry):
    return True
