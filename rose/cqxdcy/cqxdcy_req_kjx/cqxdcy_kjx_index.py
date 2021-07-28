#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/4/28 : 16:33
@Author: wenhao@bbdservice.com
"""
import os
import re
import time
from copy import deepcopy

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StringType, StructType

from cqxdcy.proj_common.date_util import DateUtils
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil


KJX_MODULE_NAME = "cq_xdcy_kejixing"

TEMP_VIEW_PROP_PATENT_DATA = "temp_view_prop_patent_data"
TEMP_VIEW_QYXX_JQKA_CWZB_ZHSY = "temp_view_qyxx_jqka_cwzb_zhsy"
TEMP_VIEW_QYXX_TAG_WHITE = "temp_view_qyxx_tag_white"
TEMP_VIEW_MANAGE_TENDERBID = "temp_view_manage_tenderbid"
TEMP_VIEW_COMPANY_COUNTY = "temp_view_company_county"


class ComUtils:

    @staticmethod
    def get_source_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")

    @staticmethod
    def get_temp_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")

    @staticmethod
    def get_current_month_first_day():
        return DateUtils.now2str("%Y%m") + "01"

    @staticmethod
    def get_end_time():
        return time.strftime("%Y-%m-%d", time.localtime(time.time()))

    @staticmethod
    def get_current_day_of_last_year() -> str:
        last_year = ComUtils.get_last_year()
        current_day = ComUtils.get_current_month_day()
        return f"{last_year}-{current_day}"

    @staticmethod
    def get_first_day_of_last_year() -> str:
        last_year = ComUtils.get_last_year()
        return f"{last_year}-01-01"

    @staticmethod
    def get_first_day_of_this_year() -> str:
        this_year = ComUtils.get_this_year()
        return f"{this_year}-01-01"

    @staticmethod
    def get_last_day_of_last_year() -> str:
        last_year = ComUtils.get_last_year()
        return f"{last_year}-12-31"

    @staticmethod
    def get_current_day_of_before_last_year():
        before_last_year = int(ComUtils.get_this_year()) - 2
        current_day = ComUtils.get_current_month_day()
        return f"{before_last_year}-{current_day}"

    @staticmethod
    def get_this_year() -> str:
        return time.strftime('%Y', time.localtime(time.time()))

    @staticmethod
    def get_last_year() -> int:
        return int(ComUtils.get_this_year()) - 1

    @staticmethod
    def get_current_month_day() -> str:
        return time.strftime('%m-%d', time.localtime(time.time()))


def save_temp_result_as_parquet(index_name):
    """
    装饰器：
    在计算指标结果 前后记录日志，并将计算结果，以parquet形式，保存到 配置的 hdfs_index_path中,；
    target 函数（方法）需要return 计算出来的 dataFrame；
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            entry = args[1]
            index_result_path = os.path.join(ComUtils.get_temp_path(entry), KJX_MODULE_NAME, index_name)

            # logging info before executing
            entry.logger.info(f"prepare to calculate the index : {index_name};")

            # execute the original function
            index_data_frame: DataFrame = func(*args, **kwargs)
            # write the result data frame to the specified path in parquet mode
            index_data_frame.write.parquet(index_result_path, mode="overwrite")

            # logging info after saving to path
            entry.logger.info(f"finish to calculate the index: {index_name}; "
                              f"and the result is saved in '{index_result_path}'")
            return index_data_frame
        return wrapper
    return decorator


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    command_prefix = "hadoop fs -test -e "

    model_result_invest_score_path = os.path.join(ComUtils.get_temp_path(entry), "model", "invest_scores.csv")
    model_result_merchant_zsjz_path = os.path.join(ComUtils.get_temp_path(entry), "model", "zsjz_result.csv")
    industry_divide_path = os.path.join(ComUtils.get_source_path(entry), "industry_divide")

    check_path_list = [model_result_invest_score_path, model_result_merchant_zsjz_path, industry_divide_path]
    for path in check_path_list:
        file_existence = os.system(command_prefix + path)
        if file_existence != 0:
            entry.logger.error(f"{path} does not exist!")
            return False

    return True


def main(entry: Entry):
    # dw 表创建temp view
    create_dw_table_temp_view(entry)

    # 计算指标
    CalculateIndex.calculate(entry)
    # 融合指标，保存指标
    MergeIndex.merge_result(entry)


def create_dw_table_temp_view(entry: Entry):
    # dw.prop_patent_data 的 temp view
    entry.spark.sql(f"""select * from dw.prop_patent_data 
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.prop_patent_data')}
                    """).createOrReplaceTempView(TEMP_VIEW_PROP_PATENT_DATA)

    # dw.qyxx_jqka_cwzb_zhsy 的 temp view
    entry.spark.sql(f"""select * from dw.qyxx_jqka_cwzb_zhsy 
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.qyxx_jqka_cwzb_zhsy')}
                    """).createOrReplaceTempView(TEMP_VIEW_QYXX_JQKA_CWZB_ZHSY)

    # dw.qyxx_tag_white 的 temp view
    entry.spark.sql(f"""select * from dw.qyxx_tag_white
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.qyxx_tag_white')}
                    """).createOrReplaceTempView(TEMP_VIEW_QYXX_TAG_WHITE)

    # dw.manage_tenderbid 的 temp view
    entry.spark.sql(f"""select * from dw.manage_tenderbid
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.manage_tenderbid')}
                    """).createOrReplaceTempView(TEMP_VIEW_MANAGE_TENDERBID)

    # dw.company_county 的 temp view
    entry.spark.sql(f"""select * from dw.company_county
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.company_county')}
                    """).createOrReplaceTempView(TEMP_VIEW_COMPANY_COUNTY)


def post_check(entry: Entry):
    return True


class CalculateIndex:

    @classmethod
    def calculate(cls, entry: Entry):
        # 创建temp view: basic_v;
        basic_v_path = os.path.join(ComUtils.get_source_path(entry), "industry_divide")
        entry.spark.read.parquet(basic_v_path).createOrReplaceTempView("basic_v")

        # 计算指标池;
        cls.cal_kjx_index_pool(entry)

        # 计算各个指标
        cls.cal_v1(entry)
        cls.cal_v2(entry)
        cls.cal_v3(entry)
        cls.cal_v4(entry)
        cls.cal_v6_1(entry)
        cls.cal_v6_2(entry)
        cls.cal_v13(entry)
        cls.cal_v14(entry)
        cls.cal_v16(entry)
        cls.cal_v17(entry)
        cls.cal_v18(entry)
        cls.cal_v19(entry)
        cls.cal_v21(entry)
        cls.cal_v24(entry)
        cls.cal_v26(entry)
        cls.cal_v27(entry)
        cls.cal_v28(entry)

    @classmethod
    @save_temp_result_as_parquet(index_name="kjx_index_pool")
    def cal_kjx_index_pool(cls, entry: Entry) -> DataFrame:
        """
        方法（函数）本身只计算出dataFrame;
        write dataFrame 的工作交给装饰器 save_index_result
        """
        index = {
            "444C": "qy_rz_num",
            "460C": "qy_zlzy_num",
            "306C": "qy_zprs_num",
            "52C": "qy_dxzp_num",
            "53C": "qy_dzjyxhbxzyzp_num",
            "187C": "qy_ssjyszp_num"
        }
        sql_str = f"""
                    select L.bbd_qyxx_id, 
                        {','.join([f'{v} as {k}' for k, v in index.items()])}
                    from dw.index_pool L
                    inner join basic_v R
                    on L.bbd_qyxx_id = R.bbd_qyxx_id
                    where nvl(L.bbd_qyxx_id, '') != ''
                   """
        kjx_index_pool_data_frame = entry.spark.sql(sql_str)
        return kjx_index_pool_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v1")
    def cal_v1(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select bbd_qyxx_id, count(bbd_qyxx_id) as V1
                    from (
                        select a.*, b.company_name as name
                        from (
                            select distinct public_code, * 
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                            where patent_type = '发明专利'
                                and bbd_qyxx_id is not null
                        ) as a
                        join basic_v as b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )   
                    group by bbd_qyxx_id
                   """
        v1_data_frame = entry.spark.sql(sql_str)
        return v1_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v2")
    def cal_v2(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select bbd_qyxx_id, count(bbd_qyxx_id) as V2
                    from (
                        select a.*, b.company_name as name
                        from (
                            select distinct public_code, * 
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                            where patent_type != '发明专利'
                                and bbd_qyxx_id is not null
                        ) as a
                        join basic_v as b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )   
                    group by bbd_qyxx_id
                   """
        v2_data_frame = entry.spark.sql(sql_str)
        return v2_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v3")
    def cal_v3(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select bbd_qyxx_id, count(bbd_qyxx_id) as V3
                    from (
                        select a.*, b.company_name as name
                        from (
                            select distinct public_code, * 
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                            where patent_type = '发明专利'
                                and company_name not rlike ';'
                                and bbd_qyxx_id is not null
                        ) as a
                        join basic_v as b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )   
                    group by bbd_qyxx_id
                   """
        v3_data_frame = entry.spark.sql(sql_str)
        return v3_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v4")
    def cal_v4(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select bbd_qyxx_id, count(bbd_qyxx_id) as V4
                    from (
                        select a.*, b.company_name as name
                        from (
                            select distinct public_code, * 
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                            where patent_type != '发明专利'
                                and company_name not rlike ';'
                                and bbd_qyxx_id is not null
                        ) as a
                        join basic_v as b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )   
                    group by bbd_qyxx_id
                   """
        v4_data_frame = entry.spark.sql(sql_str)
        return v4_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v6_1")
    def cal_v6_1(cls, entry: Entry) -> DataFrame:
        start_date = ComUtils.get_current_day_of_last_year()
        end_date = ComUtils.get_end_time()

        sql_str = f"""
                    select bbd_qyxx_id, count(bbd_qyxx_id) as V6_1
                    from (
                        select a.*, b.company_name as name
                        from (
                            select distinct public_code, *
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                            where bbd_qyxx_id is not null
                                and abstract rlike '半导体|晶体|晶圆|集成电路|芯片|存储器|SOC|封装|硅背板|
                                EVA|焊带|光伏|电池|硅|支架|逆变器|汇流箱|EPC|PET基膜氟模|电站|核电|反应堆|
                                叶片|塔架|风电|发电机|控制系统|碳酸锂|氟化锂|稀土|动力电池|电机|充电|电动车|
                                电解水|化石|氢|高压储运|固态储运|有机液态储运|电力|供热|燃料电池|金属|永磁|
                                发光|储氢|催化|抛光|靶材|合金|陶瓷|特种|无机非金属|压电|磁性|导电|激光|光导|
                                纤维|结构陶瓷|人造|树脂|复合材料|碳纤维|功能|高性能氟材料|功能性膜材料|纳米|
                                生物|智能|超导'
                                and application_date between date('{start_date}') and date('{end_date}')
                        ) a
                        join basic_v b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )
                    group by bbd_qyxx_id
                   """
        v6_1_data_frame = entry.spark.sql(sql_str)
        return v6_1_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v6_2")
    def cal_v6_2(cls, entry: Entry) -> DataFrame:
        start_date = ComUtils.get_current_day_of_before_last_year()
        end_date = ComUtils.get_end_time()

        sql_str = f"""
                    select bbd_qyxx_id, 
                        count(bbd_qyxx_id) as V6_2
                    from (
                        select a.*, 
                            b.company_name as name
                        from (
                            select distinct public_code, *
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                        where bbd_qyxx_id is not null
                            and abstract rlike '半导体|晶体|晶圆|集成电路|芯片|存储器|SOC|封装|硅背板|
                            EVA|焊带|光伏|电池|硅|支架|逆变器|汇流箱|EPC|PET基膜氟模|电站|核电|反应堆|
                            叶片|塔架|风电|发电机|控制系统|碳酸锂|氟化锂|稀土|动力电池|电机|充电|电动车|
                            电解水|化石|氢|高压储运|固态储运|有机液态储运|电力|供热|燃料电池|金属|永磁|
                            发光|储氢|催化|抛光|靶材|合金|陶瓷|特种|无机非金属|压电|磁性|导电|激光|光导|
                            纤维|结构陶瓷|人造|树脂|复合材料|碳纤维|功能|高性能氟材料|功能性膜材料|纳米|
                            生物|智能|超导'
                            and application_date between date('{start_date}') and date('{end_date}')
                        ) a
                        join basic_v b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )
                    group by bbd_qyxx_id
                   """
        v6_2_data_frame = entry.spark.sql(sql_str)
        return v6_2_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v13")
    def cal_v13(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select a.bbd_qyxx_id, 
                        sum(r_and_d_cost) as V13
                    from (
                        select bbd_qyxx_id, 
                            r_and_d_cost
                        from {TEMP_VIEW_QYXX_JQKA_CWZB_ZHSY}
                        where substring(report_date, 6, 2) = 12
                    ) a 
                    join basic_v b
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                    group by a.bbd_qyxx_id
                   """
        v13_data_frame = entry.spark.sql(sql_str)
        return v13_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v14")
    def cal_v14(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select a.bbd_qyxx_id, 
                        total_revenue/r_and_d_cost as V14
                    from (
                        select * 
                        from (
                            select bbd_qyxx_id, 
                                r_and_d_cost, 
                                total_revenue,
                                rank() over (partition by bbd_qyxx_id order by report_date desc) as date
                            from {TEMP_VIEW_QYXX_JQKA_CWZB_ZHSY}
                            where substring(report_date, 6, 2) = 12
                        )
                        where date = 1
                    ) a
                    join basic_v b
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                   """
        v14_data_frame = entry.spark.sql(sql_str)
        return v14_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v16")
    def cal_v16(cls, entry: Entry) -> DataFrame:
        tmp_sql = f"""
                    select * 
                    from {TEMP_VIEW_PROP_PATENT_DATA}
                    where title rlike '新型|芯片|制造|制备|设备|传感器|电路|储存|结构|算法|
                        装置|装备|太阳能|光伏|核电|叶片|电池|氢|制备|加工|工艺'
                   """
        v16_tmp_rdd = entry.spark.sql(tmp_sql).rdd \
            .map(lambda x: x.asDict()) \
            .flatMap(cls.check_type)

        schema_list = []
        schema_type = entry.spark.sql(f"select * from {TEMP_VIEW_PROP_PATENT_DATA} limit 1").schema
        for schema in schema_type:
            schema_list.append(schema)
        schema_list.append(StructField("industry", StringType(), True))

        v16_temp_view = "v16_tmp_v"
        entry.spark.createDataFrame(v16_tmp_rdd,
                                    schema=StructType(schema_list)) \
            .createOrReplaceTempView(v16_temp_view)

        sql_str = f"""
                    select bbd_qyxx_id, 
                        count(distinct public_code) as V16 
                    from (
                        select b.bbd_qyxx_id, b.company_name, public_code
                        from {v16_temp_view} a 
                        join basic_v b
                        on a.bbd_qyxx_id=b.bbd_qyxx_id 
                            and a.industry=b.industry
                    )
                    group by bbd_qyxx_id
                   """
        v16_data_frame = entry.spark.sql(sql_str)
        return v16_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v17")
    def cal_v17(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select a.bbd_qyxx_id, 
                        case when b.company_name is not null 
                             then 1 
                             else 0 
                        end  as V17
                    from basic_v a
                    left join (
                        select * 
                        from {TEMP_VIEW_QYXX_TAG_WHITE} 
                        where company_name is not null
                    ) b
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                   """
        v17_data_frame = entry.spark.sql(sql_str)
        return v17_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v18")
    def cal_v18(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select bbd_qyxx_id,
                        case when industry='集成电路' then 80
                             when industry='新材料' then 90
                             when industry='新能源' then 90
                        end  as V18
                    from basic_v
                   """
        v18_data_frame = entry.spark.sql(sql_str)
        return v18_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v19")
    def cal_v19(cls, entry: Entry) -> DataFrame:
        model_result_invest_score_path = os.path.join(ComUtils.get_temp_path(entry), "model", "invest_scores.csv")
        model_result_merchant_zsjz_path = os.path.join(ComUtils.get_temp_path(entry), "model", "zsjz_result.csv")
        # 投资价值
        tzjz = entry.spark.read.csv(model_result_invest_score_path,
                                    header=True)
        # 招商价值
        zsjz = entry.spark.read.csv(model_result_merchant_zsjz_path,
                                    header=True)

        jz_temp_view = "jz_tmp"
        zsjz.join(tzjz, "bbd_qyxx_id").createOrReplaceTempView(jz_temp_view)

        sql_str = f"""
                    select bbd_qyxx_id,  
                        0.5*coalesce(zsjz_score, 0) + 0.5*coalesce(total, 0) as V19
                    from {jz_temp_view}
                   """
        v19_data_frame = entry.spark.sql(sql_str)
        return v19_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v21")
    def cal_v21(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select bbd_qyxx_id, 
                        count(bbd_qyxx_id) as V21 
                    from (
                        select a.*, 
                            b.company_name as name 
                        from ( 
                            select distinct public_code, * 
                            from {TEMP_VIEW_PROP_PATENT_DATA}
                            where patent_type='发明专利' 
                                and bbd_qyxx_id is not null
                        ) a
                        join basic_v b
                        on a.bbd_qyxx_id = b.bbd_qyxx_id
                    )
                    group by bbd_qyxx_id
                   """
        v21_data_frame = entry.spark.sql(sql_str)
        return v21_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v24")
    def cal_v24(cls, entry: Entry) -> DataFrame:
        sql_str = f"""
                    select a.bbd_qyxx_id,
                        total_revenue as V24
                    from (
                        select bbd_qyxx_id, 
                            company_name, 
                            total_revenue 
                        from ( 
                            select bbd_qyxx_id, 
                                company_name, 
                                total_revenue, 
                                rank() over (partition by bbd_qyxx_id order by report_date desc) as date
                            from {TEMP_VIEW_QYXX_JQKA_CWZB_ZHSY} 
                            where substring(report_date, 6,2)=12 
                                or substring(report_date, 6,2)=03 
                                or substring(report_date, 6,2)=06 
                                or substring(report_date, 6,2)=09
                        )
                        where date=1
                    ) a
                    join basic_v b
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                   """
        v24_data_frame = entry.spark.sql(sql_str)
        return v24_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v26")
    def cal_v26(cls, entry: Entry) -> DataFrame:
        tenderbid_start_date = ComUtils.get_current_day_of_last_year()
        tenderbid_end_date = ComUtils.get_end_time()
        tmp_sql = f"""
                    select a.bbd_qyxx_id, 
                        b.company_name, 
                        bid_amount, 
                        fr_bid_amount 
                    from ( 
                        select bbd_qyxx_id, 
                            company_win, 
                            bid_amount, 
                            rank() over (partition by company_name_invite order by bid_amount desc) as fr_bid_amount
                        from {TEMP_VIEW_MANAGE_TENDERBID} 
                        where bid_amount != 'null'
                            and pubdate between date('{tenderbid_start_date}') and date('{tenderbid_end_date}')
                    ) a
                    join basic_v b
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                   """
        bid_temp_view = "tmp_v"
        entry.spark.sql(tmp_sql).createOrReplaceTempView(bid_temp_view)

        sql_str = f"""
                    select a.bbd_qyxx_id, 
                        b.total_amount/a.total_amount as V26 
                    from ( 
                        select bbd_qyxx_id, 
                            company_name, 
                            sum(bid_amount) as total_amount 
                        from {bid_temp_view} 
                        group by bbd_qyxx_id, company_name
                    ) a
                    join ( 
                        select bbd_qyxx_id, 
                            company_name, 
                            sum(bid_amount) as total_amount 
                        from ( 
                            select * 
                            from {bid_temp_view} 
                            where fr_bid_amount < 6 
                        ) c
                        group by bbd_qyxx_id, company_name
                    ) b
                    on a.bbd_qyxx_id=b.bbd_qyxx_id
                   """
        v26_data_frame = entry.spark.sql(sql_str)
        return v26_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v27")
    def cal_v27(cls, entry: Entry) -> DataFrame:
        report_temp_sql = f"""
                            select bbd_qyxx_id, 
                                company_name, 
                                total_revenue, 
                                rank() over (partition by bbd_qyxx_id order by report_date desc) as rank
                            from ( 
                                select * 
                                from ( 
                                    select bbd_qyxx_id, 
                                        company_name, 
                                        total_revenue, 
                                        report_date,
                                        year_str,
                                        rank() over (partition by bbd_qyxx_id, year_str order by year_str desc) as date
                                    from ( 
                                        select *, 
                                            substring(report_date, 1,4) as year_str 
                                        from {TEMP_VIEW_QYXX_JQKA_CWZB_ZHSY} 
                                    )
                                )
                                where date=1
                            )
                           """
        report_temp_view = "report_temp_view"
        entry.spark.sql(report_temp_sql).createOrReplaceTempView(report_temp_view)

        sql_str = f"""
                    select a.bbd_qyxx_id, 
                        (a.total_revenue-b.total_revenue)/a.total_revenue as V27 
                    from ( 
                        select bbd_qyxx_id, 
                            company_name, 
                            total_revenue 
                        from {report_temp_view}
                        where rank=1
                    ) a
                    join ( 
                        select bbd_qyxx_id, 
                            company_name, 
                            total_revenue 
                        from {report_temp_view}
                        where rank=2
                    ) b
                    on a.bbd_qyxx_id=b.bbd_qyxx_id
                   """
        v27_data_frame = entry.spark.sql(sql_str)
        return v27_data_frame

    @classmethod
    @save_temp_result_as_parquet(index_name="v28")
    def cal_v28(cls, entry: Entry) -> DataFrame:
        start_date = ComUtils.get_first_day_of_this_year()
        end_date = ComUtils.get_end_time()
        pr_start_date = ComUtils.get_current_day_of_last_year()
        pr_end_date = ComUtils.get_last_day_of_last_year()

        prop_temp_sql = f"""
                            select c.bbd_qyxx_id, 
                                c.company_name, 
                                (c.index - d.index)/d.index as prop_index 
                            from ( 
                                select bbd_qyxx_id, 
                                    name as company_name, 
                                    count(bbd_qyxx_id) as index
                                from (
                                    select a.*, 
                                        b.company_name as name 
                                    from ( 
                                        select distinct public_code, * 
                                        from {TEMP_VIEW_PROP_PATENT_DATA}
                                        where bbd_qyxx_id is not null
                                            and publidate between date('{start_date}') and date('{end_date}')
                                    ) a
                                    join basic_v b
                                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                                )
                                group by bbd_qyxx_id, name 
                            ) c
                            join ( 
                                select bbd_qyxx_id, 
                                    name as company_name, 
                                    count(bbd_qyxx_id) as index 
                                from (
                                    select k.*, 
                                        j.company_name as name 
                                    from ( 
                                        select distinct public_code, * 
                                        from {TEMP_VIEW_PROP_PATENT_DATA}
                                        where bbd_qyxx_id is not null
                                            and publidate between date('{pr_start_date}') and date('{pr_end_date}')
                                    ) k
                                    join basic_v j
                                    on k.bbd_qyxx_id = j.bbd_qyxx_id
                                )
                                group by bbd_qyxx_id, name
                            ) d
                            on c.bbd_qyxx_id = d.bbd_qyxx_id
                         """
        prop_temp_view = "prop_v"
        entry.spark.sql(prop_temp_sql).createOrReplaceTempView(prop_temp_view)
        v27_temp_view = "tmp_v27"

        entry.spark.read.parquet(os.path.join(ComUtils.get_temp_path(entry),
                                              KJX_MODULE_NAME,
                                              "v27")
                                 ).createOrReplaceTempView(v27_temp_view)

        sql_str = f"""
                    select a.bbd_qyxx_id, 
                        a.V27/b.prop_index as V28
                    from {v27_temp_view} a
                    join {prop_temp_view} b
                    on a.bbd_qyxx_id = b.bbd_qyxx_id
                   """
        v28_data_frame = entry.spark.sql(sql_str)
        return v28_data_frame

    @classmethod
    def check_type(cls, prop_dict):
        prop_return = []
        if re.findall('新型|芯片|制造|制备|设备|传感器|电路|储存|结构|算法', prop_dict['title']):
            z_dict1 = deepcopy(prop_dict)
            z_dict1['industry'] = '集成电路'
            prop_return.append(z_dict1)
        if re.findall('新型|装置|设备|装备|太阳能|光伏|核电|叶片|电池|氢', prop_dict['title']):
            z_dict2 = deepcopy(prop_dict)
            z_dict2['industry'] = '新能源'
            prop_return.append(z_dict2)
        if re.findall('新型|装置|设备|设备|制备|加工|工艺', prop_dict['title']):
            z_dict3 = deepcopy(prop_dict)
            z_dict3['industry'] = '新材料'
            prop_return.append(z_dict3)
        return prop_return


class MergeIndex:

    @classmethod
    def merge_result(cls, entry: Entry):
        computed_index_paths = ["kjx_index_pool", "v1",
                                "v2", "v3", "v4",
                                "v6_1", "v6_2",
                                "v13", "v14", "v16",
                                "v17", "v18", "v19",
                                "v21", "v24", "v26",
                                "v27", "v28"]

        need_merge_result_df = cls.cal_province_city_area(entry)
        for index_path in computed_index_paths:
            index_whole_path = os.path.join(ComUtils.get_temp_path(entry),
                                            KJX_MODULE_NAME,
                                            index_path)
            index_df = entry.spark.read.parquet(index_whole_path)
            need_merge_result_df = need_merge_result_df.join(index_df,
                                                             "bbd_qyxx_id",
                                                             "left").fillna(0)

        kjx_index_path = os.path.join(ComUtils.get_temp_path(entry),
                                      KJX_MODULE_NAME,
                                      "merge_index_kjx")
        need_merge_result_df.repartition(1).write.csv(kjx_index_path,
                                                      header=True,
                                                      sep="\t",
                                                      mode="overwrite")
        entry.logger.info(f"finish to calculate the kjx index; "
                          f"and the result is saved in {kjx_index_path}")

    @classmethod
    def cal_province_city_area(cls, entry: Entry) -> DataFrame:
        """
        计算公司 省 市 区
        """
        qyxx_basic_temp_view = "qyxx_basic_view"
        entry.spark.read.parquet(os.path.join(ComUtils.get_source_path(entry),
                                              "industry_divide")
                                 ).createOrReplaceTempView(qyxx_basic_temp_view)
        company_county_temp_view = "company_county_view"
        cls.get_company_county(entry).createOrReplaceTempView(company_county_temp_view)

        sql_str = f"""
                    select 
                        R.bbd_qyxx_id,
                        R.company_name,
                        R.industry,
                        R.sub_industry,
                        L.province,
                        L.city,
                        L.district 
                    from {company_county_temp_view} L 
                    right join {qyxx_basic_temp_view} R 
                    on L.company_county = R.company_county 
                  """
        company_county_df = entry.spark.sql(sql_str)

        entry.logger.info("finish to calculate the province,city and area from qyxx_basic and company_county")
        return company_county_df

    @classmethod
    def get_company_county(cls, entry: Entry) -> DataFrame:
        """
        获取 省 市 区
        """
        sql_str = f"""
                    select company_county, 
                        province, 
                        city, 
                        district
                    from ( 
                        select code as company_county, 
                            province, 
                            city, 
                            district, 
                            (row_number() over(partition by code order by tag )) as row_num
                        from {TEMP_VIEW_COMPANY_COUNTY} 
                        where tag != -1) b 
                    where row_num = 1
                  """
        data_frame = entry.spark.sql(sql_str)

        entry.logger.info("finish to calculate the province,city and area")
        return data_frame
