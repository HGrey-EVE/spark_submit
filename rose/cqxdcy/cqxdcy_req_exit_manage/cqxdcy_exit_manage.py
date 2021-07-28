#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/5/27 : 14:09
@Author: wenhao@bbdservice.com
"""
import os
import time
import json

from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType, MapType

from cqxdcy.proj_common.hive_util import HiveUtil
from whetstone.core.entry import Entry

MODULE_NAME = "cq_xdcy_exit_manage"

VIEW_BASIS_ORGXX = "view_basis_orgxx"
VIEW_MARKET_INVESTMENT = "view_market_investment"
VIEW_MANAGE_FUNDRAISE = "view_manage_fundraise"
VIEW_MANAGE_LISTED = "view_manage_listed"
VIEW_MANAGE_MAXX = "view_manage_maxx"

LAST_FIVE_YEARS = [time.strftime('%Y', time.localtime(time.time() - 3600 * 24 * 365 * num)) for num in range(1, 6)]


class ComUtils:
    @staticmethod
    def get_result_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")

    @staticmethod
    def get_temp_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")


def logger(func):
    def wrapper(*args, **kwargs):
        args[0].logger.info(f"start to execute the function: {func.__name__}...")
        result = func(*args, **kwargs)
        args[0].logger.info(f"executing the function:{func.__name__} has finished!")
        return result
    return wrapper


@logger
def prepare_table_view(entry: Entry):
    table_view_map = {
        "dw.basis_orgxx": VIEW_BASIS_ORGXX,
        "dw.market_investment": VIEW_MARKET_INVESTMENT,
        "dw.manage_fundraise": VIEW_MANAGE_FUNDRAISE,
        "dw.manage_listed": VIEW_MANAGE_LISTED,
        "dw.manage_maxx": VIEW_MANAGE_MAXX
    }
    view_sql_str_template = """select * from {table_name} where dt='{newest_dt}'"""
    for table, view in table_view_map.items():
        table_newest_dt = HiveUtil.newest_partition_by_month(entry.spark,
                                                             table,
                                                             in_month_str=entry.version[:6])
        sql_str = view_sql_str_template.format(table_name=table,
                                               newest_dt=table_newest_dt)
        entry.spark.sql(sql_str).createOrReplaceTempView(view)
        entry.logger.info(f"create temp view ({view}) for table({table}) successfully!")


def mapping_industry_type(entry: Entry, view_name: str) -> DataFrame:
    sql_str = f"""
                select *, 
                    case when industry = '清洁技术' then '节能环保'
                         when industry like '清洁技术 > 环保%' then '节能环保'
                         when industry like '清洁技术 > 新材料%' then '新材料'
                         when industry like '清洁技术 > 新能源%' then '新能源'
                         when industry like '清洁技术 > 其他%' then '生物产业'
                         when industry like '%电信及增值业务%' then  regexp_replace(industry, '电信及增值业务', '新一代信息技术')
                         when industry like '%IT%' then  regexp_replace(industry, 'IT', '新一代信息技术')
                         when industry like '%广播电视及数字电视%' then  regexp_replace(industry, '广播电视及数字电视', '新一代信息技术')
                         when industry like '%生物技术/医疗健康%' then  regexp_replace(industry, '生物技术/医疗健康', '生物产业')
                         when industry like '%机械制造%' then  regexp_replace(industry, '机械制造', '高端装备制造')
                         when industry like '%电子及光电设备%' then  regexp_replace(industry, '电子及光电设备', '高端装备制造')
                         when industry like '%半导体%' then '新一代信息技术 > 集成电路'
                    else industry end transfered_industry
                from {view_name}
               """
    return entry.spark.sql(sql_str)


@logger
def cal_invest_trend(entry: Entry):
    """
    5年内的投资趋势；
    计算 num 和 amount;
    """
    invest_mapping_industry_view = "invest_mapping_industry_view"
    mapping_industry_type(entry, VIEW_MARKET_INVESTMENT).createOrReplaceTempView(invest_mapping_industry_view)
    mapping_amount_sql = f"""
                            select new_industry, 
                                financiers, 
                                finance_date, 
                                transfer_amount * transfer_ratio as new_amount 
                            from (
                                select new_industry, 
                                    financiers, 
                                    finance_date, 
                                    case when unit_amount regexp('.*数百万.*') then 3000000.0
                                         when unit_amount regexp('.*数千万.*') then 30000000.0
                                         when unit_amount regexp('.*数亿.*') then 300000000.0
                                         when unit_amount regexp('.*千万.*') then cast(substr(unit_amount,0,length(unit_amount)-2) as double) * 10000000
                                         when unit_amount regexp('.*百万.*') then cast(substr(unit_amount,0,length(unit_amount)-2) as double) * 1000000
                                         when unit_amount regexp('.*万.*') then cast(substr(unit_amount,0,length(unit_amount)-1) as double) * 10000
                                         when unit_amount regexp('.*亿.*') then cast(substr(unit_amount,0,length(unit_amount)-1) as double) * 100000000
                                         else 0 
                                    end transfer_amount, 
                                    case when bizhong = 'GBP' then 9
                                         when bizhong = 'CHF' then 7
                                         when bizhong = 'CAD' then 5.1
                                         when bizhong = 'EUR' then 7.7
                                         when bizhong = 'AUD' then 5
                                         when bizhong = 'JPY' then 0.06
                                         when bizhong = 'HKD' then 0.83
                                         when bizhong = 'NTD' then 4.3
                                         when bizhong = 'RMB' then 1
                                         when bizhong = 'USD' then 6.496
                                         when bizhong = 'SGD' then 4.841
                                         when bizhong = 'INR' then 0.089
                                         else 1 
                                    end transfer_ratio 
                                from (
                                    select trim(split(transfered_industry,'>')[0]) as new_industry, 
                                        case when amount = '非公开' then 'RMB' 
                                             else substring(amount, 0, 3)
                                        end bizhong, 
                                        case when amount = '非公开' then 0 
                                             else substring(amount, 4)
                                        end unit_amount,
                                        financiers,
                                        finance_date
                                    from {invest_mapping_industry_view}
                                )
                            )
                          """
    invest_mapping_amount_view = "invest_mapping_amount_view"
    entry.spark.sql(mapping_amount_sql).createOrReplaceTempView(invest_mapping_amount_view)
    for year in LAST_FIVE_YEARS:
        end_date = f"{year}-12-31"
        sql_str = f"""
                    select new_industry as invest_trend_industry, 
                        count(distinct financiers) as invest_trend_num, 
                        sum(new_amount) as invest_trend_amount
                    from {invest_mapping_amount_view}
                    where date(finance_date) <= date('{end_date}')
                    group by new_industry
                   """
        invest_trend_df = entry.spark.sql(sql_str)
        path = os.path.join(ComUtils.get_temp_path(entry), MODULE_NAME, year, "invest_trend")
        invest_trend_df.write.parquet(path, mode="overwrite")


@logger
def cal_listed_trend(entry: Entry):
    """
    5年内的上市趋势，只计算num, 不计算amount；
    todo: 暂时保留amount字段，保证后端程序能够正常运行。 后期删除amount字段
    """
    listed_mapping_industry_view = "listed_mapping_industry_view"
    mapping_industry_type(entry, VIEW_MANAGE_LISTED).createOrReplaceTempView(listed_mapping_industry_view)
    for year in LAST_FIVE_YEARS:
        end_date = f"{year}-12-31"
        sql_str = f"""
                    select new_industry as list_trend_industry, 
                        count(distinct company_name) as list_trend_num, 
                        0 as list_trend_amount
                    from (
                        select trim(split(transfered_industry,'>')[0]) as new_industry, 
                            company_name
                        from {listed_mapping_industry_view}
                        where date(market_time) <= date('{end_date}')
                    )
                    group by new_industry
                   """
        list_trend_df = entry.spark.sql(sql_str)
        path = os.path.join(ComUtils.get_temp_path(entry), MODULE_NAME, year, "list_trend")
        list_trend_df.write.parquet(path, mode="overwrite")


@logger
def cal_maxx_trend(entry: Entry):
    """
    5年内的并购趋势，只计算num, 不计算amount；
    todo: 暂时保留amount字段，保证后端程序能够正常运行。 后期删除amount字段
    """
    maxx_mapping_industry_view = "maxx_mapping_industry_view"
    mapping_industry_type(entry, VIEW_MANAGE_MAXX).createOrReplaceTempView(maxx_mapping_industry_view)
    for year in LAST_FIVE_YEARS:
        end_date = f"{year}-12-31"
        sql_str = f"""
                    select new_industry as maxx_trend_industry, 
                        count(distinct acquirer) as maxx_trend_num, 
                        0 as maxx_trend_amount
                    from (
                        select trim(split(transfered_industry,'>')[0]) as new_industry, 
                            acquirer
                        from {maxx_mapping_industry_view}
                        where date(mastrat_date) <= date('{end_date}')
                    )
                    group by new_industry
                   """
        list_trend_df = entry.spark.sql(sql_str)
        path = os.path.join(ComUtils.get_temp_path(entry), MODULE_NAME, year, "maxx_trend")
        list_trend_df.write.parquet(path, mode="overwrite")


@logger
def cal_index_listed_distribution(entry: Entry):
    view_industry_listed_distribution = "view_industry_listed_distribution"
    industry_listed_dt = mapping_industry_type(entry, VIEW_MANAGE_LISTED)
    industry_listed_dt.createOrReplaceTempView(view_industry_listed_distribution)

    for year in LAST_FIVE_YEARS:
        end_date = f"{year}-12-31"
        sql_str = f"""
                    select new_industry as list_distribution_industry, 
                    market_address as list_distribution_market_address, 
                    count(distinct company_name) as list_distribution_num
                    from (
                        select trim(split(transfered_industry,'>')[0]) as new_industry, 
                            market_address, 
                            company_name
                        from {view_industry_listed_distribution}
                        where date(market_time) <= date('{end_date}')
                    )               
                    group by new_industry, market_address
                   """
        listed_distribution_dt = entry.spark.sql(sql_str)

        path = os.path.join(ComUtils.get_temp_path(entry), MODULE_NAME, year, "list_distribution")
        listed_distribution_dt.write.parquet(path, mode="overwrite")
        entry.logger.info(f"save temp result {year}/list_distribution onto {path} successfully!")


def calculate_indexes(entry: Entry):
    # 计算投资趋势
    cal_invest_trend(entry)
    # 计算上市趋势
    cal_listed_trend(entry)
    # 计算并购趋势
    cal_maxx_trend(entry)

    # 计算指标：上市分布
    cal_index_listed_distribution(entry)


@logger
def merge_indexes(entry: Entry):

    def is_empty_str(string: str):
        if string is None:
            return True
        if string == "":
            return True
        if string.strip() == "":
            return True
        return False

    def sql_get_industry(first_industry_str, second_industry_str, third_industry_str, forth_industry_str):
        if not is_empty_str(first_industry_str):
            return first_industry_str
        if not is_empty_str(second_industry_str):
            return second_industry_str
        if not is_empty_str(third_industry_str):
            return third_industry_str
        return forth_industry_str
    entry.spark.udf.register("get_industry", sql_get_industry)

    def market_address_num_to_json(market_address, num):
        data = {market_address: num}
        return json.dumps(data, ensure_ascii=False)
    entry.spark.udf.register("market_address_num_to_json", market_address_num_to_json)

    def num_amount_year_to_json(num, amount, the_year):
        data = dict()
        data["num"] = num if num and num != 'null' and num != '' else 0
        data["amount"] = amount if amount and num != 'null' and num != '' else 0
        data["year"] = the_year
        return json.dumps(data, ensure_ascii=False)
    entry.spark.udf.register("num_amount_year_to_json", num_amount_year_to_json)

    def list_dict_to_single_dict_json(dict_list):
        data = dict()
        for origin_data in dict_list:
            json_origin_data = json.loads(origin_data)
            for k, v in json_origin_data.items():
                if k is not None and k != "" and k != "null":
                    data[k] = json_origin_data[k]
        return json.dumps(data, ensure_ascii=False)
    entry.spark.udf.register("list_dict_to_single_dict_json", list_dict_to_single_dict_json)

    for year in LAST_FIVE_YEARS:
        year_read_path = os.path.join(ComUtils.get_temp_path(entry), MODULE_NAME, year)
        entry.spark.read.parquet(os.path.join(year_read_path, "list_trend"))\
            .createOrReplaceTempView("temp_list_trend_view")
        entry.spark.read.parquet(os.path.join(year_read_path, "invest_trend"))\
            .createOrReplaceTempView("temp_invest_trend_view")
        entry.spark.read.parquet(os.path.join(year_read_path, "maxx_trend"))\
            .createOrReplaceTempView("temp_maxx_trend_view")
        entry.spark.read.parquet(os.path.join(year_read_path, "list_distribution"))\
            .createOrReplaceTempView("temp_list_distribution_view")

        # 先union 所有的industry
        all_industry_sql = """
                            select distinct industry 
                            from (
                                select invest_trend_industry as industry from temp_invest_trend_view
                                union 
                                select list_trend_industry as industry from temp_list_trend_view
                                union 
                                select maxx_trend_industry as industry from temp_maxx_trend_view
                                union 
                                select list_distribution_industry as industry from temp_list_distribution_view    
                            )
                           """
        entry.spark.sql(all_industry_sql).createOrReplaceTempView("all_industry_view")
        sql_str = f"""
                        select v1.industry, 
                            num_amount_year_to_json(invest_trend_num, 
                                                    invest_trend_amount, 
                                                    {year}
                            ) as invest_trend, 
                            num_amount_year_to_json(list_trend_num, 
                                                    list_trend_amount, 
                                                    {year}
                            ) as list_trend, 
                            num_amount_year_to_json(maxx_trend_num, 
                                                    maxx_trend_amount, 
                                                    {year}
                            ) as merger_trend,
                            list_distribution, 
                            concat('{year}', '-01-01') as date, 
                            cast(date_format(now(), 'yyyy-MM-dd HH:mm:ss') as string) as update_time
                        from all_industry_view v1 
                        left join temp_invest_trend_view v2 on v1.industry = v2.invest_trend_industry
                        left join temp_list_trend_view v3 on v1.industry = v3.list_trend_industry 
                        left join temp_maxx_trend_view v4 on v1.industry = v4.maxx_trend_industry 
                        left join (
                            select list_distribution_industry, 
                                list_dict_to_single_dict_json(
                                    collect_list(
                                        market_address_num_to_json(
                                            list_distribution_market_address, list_distribution_num
                                        )
                                    )
                                ) as list_distribution
                            from temp_list_distribution_view
                            group by list_distribution_industry 
                        ) v5 on v1.industry = v5.list_distribution_industry 
                       """
        temp_result_df = entry.spark.sql(sql_str)

        def result_data_to_json(row: Row):
            temp_data = row.asDict(True)
            push_data = dict()
            push_data["tn"] = "cq_xdcy_exit_manage"
            push_data["data"] = temp_data
            return push_data

        result_df_rdd = temp_result_df.rdd.map(result_data_to_json)
        result_df_schemas = [StructField("tn", StringType()),
                             StructField("data", MapType(StringType(), StringType()))]
        result_df = entry.spark.createDataFrame(result_df_rdd, schema=StructType(result_df_schemas))

        path = os.path.join(ComUtils.get_result_path(entry), MODULE_NAME, year)
        result_df.repartition(1).write.json(path, mode="overwrite")
        entry.logger.info(f"save exit_manage index of {year} onto path({path}) successfully!")


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    # step2: 创建需要用到的表的最新分区的 temp_view,
    prepare_table_view(entry)
    # step3: 计算各个指标
    calculate_indexes(entry)
    # step4: 融合指标
    merge_indexes(entry)


def post_check(entry: Entry):
    return True
