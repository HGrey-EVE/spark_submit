#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 产业划分
:Author: weifuwan@bbdservice.com
:Date: 2021-02-03 16:08
"""
import datetime
import json
import os
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, MapType, StringType
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from pyspark.sql import functions as fun
from cqxdcy.proj_common.date_util import DateUtils
from pyspark.sql import Row, functions as F
from cqxdcy.proj_common.log_track_util import LogTrack as T
from cqxdcy.proj_common.json_util import JsonUtils


class CnUtils:
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
    check data
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
    """
    风险偏好
    """
    @classmethod
    def risk_preference(cls, entry):
        bo_max_dt = HiveUtil.newest_partition(entry.spark, 'dw.basis_orgxx')
        _sql = f"""
                select
                    bbd_qyxx_id,
                    case when org_type rlike '天使投资人|VC' then '激进型'
                         when org_type rlike '不限|战略投资者|劵商直投|VCPE' then '稳健型'
                         when org_type rlike 'PE|FOFs' then '保守型'
                         else '保守型' end as risk_preference,
                    dt as date
                from dw.basis_orgxx 
                where dt = '{bo_max_dt}'
            """
        return cls.exec_sql(_sql, entry)

    """
    管理资本量(百万人名币)
    """
    @classmethod
    def regulate_capital(cls, entry):
        mf_max_dt = HiveUtil.newest_partition(entry.spark, 'dw.manage_fundraise')
        bo_max_dt = HiveUtil.newest_partition(entry.spark, 'dw.basis_orgxx')
        _sql = f"""
                select 
                    bbd_qyxx_id,
                    sum(amount) regulate_capital_num,
                    max(amount) regulate_capital_max
                from (
                    select
                        R.bbd_qyxx_id,
                        (L.amount / 1000000) amount
                    from (
                        select 
                            bbd_qyxx_id,
                            org_name,
                            case when amount regexp('.*百万.*') then cast(substr(amount,0,length(amount)-2) as double) * 1000000
                                 when amount regexp('.*千万.*') then cast(substr(amount,0,length(amount)-2) as double) * 10000000
                                 when amount regexp('.*万.*') then cast(substr(amount,0,length(amount)-1) as double) * 10000
                                 when amount regexp('.*亿.*') then cast(substr(amount,0,length(amount)-1) as double) * 100000000
                                 when amount = '非公开' then 0
                            else 0 end amount
                        from dw.manage_fundraise 
                        where dt = '{mf_max_dt}'
                    ) L right join (select * from dw.basis_orgxx where dt = '{bo_max_dt}' and nvl(bbd_qyxx_id,'') != '') R
                    on L.org_name = R.short_name
                ) b
                group by bbd_qyxx_id
            """
        return cls.exec_sql(_sql, entry).fillna(0, subset=['regulate_capital_num', 'regulate_capital_max'])

    @classmethod
    def invest_case(cls, entry):
        return entry.spark.sql("select * from tz_basic").groupBy("bbd_qyxx_id").agg(
            F.count("bbd_qyxx_id").alias("invest_case_num"),
            F.sum("amount").alias("invest_case_amount")
        )


    @classmethod
    @T.log_track
    def jg_basic(cls, entry: Entry):
        mi_max_dt = HiveUtil.newest_partition(entry.spark, 'dw.market_investment')
        bo_max_dt = HiveUtil.newest_partition(entry.spark, 'dw.basis_orgxx')
        _sql = f"""
        select
            R.*,
            L.industry,
            L.investor,
            L.bizhong,
            L.amount2,
            L.bz,
            L.rounds,
            L.company_province,
            round((L.amount2 * bz) / 1000000.0) amount,
            substr(finance_date,0,4) as financies_year
        from (
            select
                bbd_qyxx_id,
                bbd_xgxx_id,
                trim(split(industry,'>')[0]) as industry,
                finance_date,
                bizhong,
                rounds,
                money,
                investor,
                company_province,
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
                    else 1 end bz
                ,
                case when money regexp('.*数百万.*') then 3000000.0
                     when money regexp('.*数千万.*') then 30000000.0
                     when money regexp('.*数亿.*') then 300000000.0
                     when money regexp('.*千万.*') then cast(substr(money,0,length(money)-2) as double) * 10000000
                     when money regexp('.*百万.*') then cast(substr(money,0,length(money)-2) as double) * 1000000
                     when money regexp('.*万.*') then cast(substr(money,0,length(money)-1) as double) * 10000
                     when money regexp('.*亿.*') then cast(substr(money,0,length(money)-1) as double) * 100000000
                    else 0 end amount2
            from (
                select
                    e.*,
                    f.company_province
                from (select 
                    bbd_qyxx_id,
                    bbd_xgxx_id,
                    amount,
                    rounds,
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
                  else industry end industry,
                    investor,
                    finance_date,
                    substr(amount,0,3) bizhong,
                    substr(amount,4,length(amount)) money
                from dw.market_investment
                where dt = '{mi_max_dt}') e left join (select bbd_qyxx_id,company_province from dw.qyxx_basic where dt = '20210621') f
                on e.bbd_qyxx_id = f.bbd_qyxx_id
            ) a
        ) L right join (select * from dw.basis_orgxx where dt = '{bo_max_dt}' and nvl(bbd_qyxx_id,'') != '') R
        on 1=1 where L.investor like concat('%',R.short_name,'%')
        """
        cls.exec_sql(_sql, entry)\
            .fillna(0,subset=['amount'])\
            .fillna('',subset=['industry'])\
            .cache()\
            .createOrReplaceTempView("tz_basic")

    @classmethod
    def invest_case_info(cls, entry):
        """
        数据源: basis_orgxx(投资机构表) && 投资事件(market_investment)
        需求: 统计近5年每年的投资事件数与投资金额总数
        """
        # 获取最近5年
        near5Year = [time.strftime('%Y', time.localtime(time.time() - 3600 * 24 * 365 * num)) for num in range(6)]
        year_2020 = near5Year[1]
        year_2019 = near5Year[2]
        year_2018 = near5Year[3]
        year_2017 = near5Year[4]
        year_2016 = near5Year[5]
        invest_case_info_df = entry.spark.sql(f"""
                select
                    bbd_qyxx_id,
                    financies_year,
                    num,
                    amount
                from (
                    select
                        bbd_qyxx_id,
                        case when nvl(financies_year,'') = '' then '2000'
                            else financies_year end financies_year,
                        num,
                        amount
                    from (  
                        select
                            bbd_qyxx_id,
                            financies_year,
                            count(1) num,
                            sum(amount) amount
                        from tz_basic
                        group by bbd_qyxx_id,financies_year
                    ) a
                ) b
                where financies_year between '{year_2016}' and '{year_2020}'
            """).fillna('', subset=['financies_year']).fillna(0, subset=['num', 'amount'])
        def to_json_data(row: Row) -> Row:
            """
            返回指定json数据
            :param row:
            :return:
            """
            tmp_data = row.asDict(True)
            push_data = dict()
            push_data["bbd_qyxx_id"] = tmp_data['bbd_qyxx_id']
            print(push_data)
            del tmp_data['bbd_qyxx_id']
            financies_year = tmp_data['financies_year']
            del tmp_data['financies_year']
            push_data["invest_case_info"] = {financies_year: {k: str(v) for k, v in tmp_data.items()}}
            return push_data
        json_rdd = invest_case_info_df.rdd.map(to_json_data)
        json_rdd3 = json_rdd.map(lambda x: (x['bbd_qyxx_id'], x['invest_case_info'])).groupByKey().map(
            lambda m: (m[0], list(m[1])))
        invest_case_info_tbl = json_rdd3.toDF(["bbd_qyxx_id", "invest_case_info"])
        return invest_case_info_tbl

    @classmethod
    def invest_industry_case(cls, entry):
        """
        数据源: basis_orgxx(投资机构表) && 投资事件(market_investment)
        需求: 按行业统计投资事件数与投资金额总数
        """
        invest_industry_case_df = entry.spark.sql("""
                select
                    bbd_qyxx_id,
                    industry,
                    case when nvl(industry,'') = '' then '其他'
                         when industry = 'N/A' then '其他'
                    else industry end industry,
                    num,
                    amount
                from (
                    select 
                        bbd_qyxx_id,
                        industry,
                        count(1) num,
                        sum(amount) amount
                    from tz_basic
                    group by bbd_qyxx_id,industry
                ) 
            """)

        def to_json_data(row: Row) -> Row:
            tmp_data = row.asDict(True)
            print(tmp_data)
            push_data = dict()
            push_data["bbd_qyxx_id"] = tmp_data['bbd_qyxx_id']
            print(push_data)
            del tmp_data['bbd_qyxx_id']
            industry = tmp_data['industry']
            del tmp_data['industry']
            push_data["invest_industry_case_info"] = {industry: {k: str(v) for k, v in tmp_data.items()}}
            return push_data

        def to_json_data2(tp):
            push_data = dict()
            push_data['bbd_qyxx_id'] = tp[0]
            push_data['invest_industry_case_info'] = tp[1]
            return JsonUtils.toString(push_data)

        json_rdd = invest_industry_case_df.rdd.map(to_json_data)

        json_rdd3 = json_rdd.map(lambda x: (x['bbd_qyxx_id'], x['invest_industry_case_info'])).groupByKey().map(
            lambda m: (m[0], list(m[1])))
        invest_industry_case_tbl = json_rdd3.toDF(["bbd_qyxx_id", "invest_industry_case_info"])
        return invest_industry_case_tbl

    @classmethod
    def invest_area_case(cls, entry):
        """
        数据源: basis_orgxx(投资机构表)
        需求: 按地域(省) 统计投资事件数与投资金额总数
        """
        invest_case_df = entry.spark.sql("""
                select
                    bbd_qyxx_id,
                    case when nvl(company_province,'') = '' then '其他'
                         when company_province = 'N/A' then '其他'
                    else company_province end company_province,
                    num,
                    amount
                from (
                    select 
                        bbd_qyxx_id,
                        company_province,
                        count(1) num,
                        sum(amount) amount
                    from tz_basic
                    group by bbd_qyxx_id,company_province
                ) a 

            """)

        def to_json_data(row: Row) -> Row:
            """
            返回指定json数据
            :param row:
            :return:
            """
            tmp_data = row.asDict(True)
            print(tmp_data)
            push_data = dict()
            push_data["bbd_qyxx_id"] = tmp_data['bbd_qyxx_id']
            del tmp_data['bbd_qyxx_id']
            area = tmp_data['org_head']
            del tmp_data['org_head']
            push_data["invest_area_case_info"] = {area: {k: str(v) for k, v in tmp_data.items()}}
            return push_data
        json_rdd = invest_case_df.rdd.map(to_json_data)
        json_rdd3 = json_rdd.map(lambda x: (x['bbd_qyxx_id'], x['invest_area_case_info'])).groupByKey().map(
            lambda m: (m[0], list(m[1])))
        invest_area_case_info_tbl = json_rdd3.toDF(["bbd_qyxx_id", "invest_area_case_info"])
        return invest_area_case_info_tbl

    @classmethod
    def new_invest_area_case_info(cls, entry: Entry):

        def area_num_amount_to_json(area, num, amount):
            data = {
                area: {
                    "num": num,
                    "amount": amount
                }
            }
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("area_num_amount_to_json", area_num_amount_to_json)

        def list_dict_to_single_dict_json(dicts_list):
            data = dict()
            for element in dicts_list:
                origin_data = json.loads(element)
                for k, v in origin_data.items():
                    data[f"{k}"] = v
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("list_dict_to_single_dict_json", list_dict_to_single_dict_json)

        def industry_area_to_json(industry, area_invest_json_list):
            area_invest_result = dict()
            for area_invest_json in area_invest_json_list:
                area_invest_data = json.loads(area_invest_json)
                for k, v in area_invest_data.items():
                    area_invest_result[k] = v
            data = {industry: area_invest_result}
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("industry_area_to_json", industry_area_to_json)

        sql_str = f"""
                    select bbd_qyxx_id, industry,
                        case when nvl(org_head,'') = '' then '其他'
                             when org_head = 'N/A' then '其他'
                             else org_head 
                        end org_head,
                        num,
                        amount
                    from (
                        select 
                            bbd_qyxx_id,
                            industry, 
                            org_head,
                            count(1) num,
                            sum(amount) amount
                        from tz_basic
                        group by bbd_qyxx_id, industry, org_head
                    )
                   """
        entry.spark.sql(sql_str).createOrReplaceTempView("invest_case_view")
        sql_str_2 = """
                        select bbd_qyxx_id, 
                            list_dict_to_single_dict_json(
                                collect_list(
                                    industry_area_to_json(
                                        industry, 
                                        area_num_amount_info_list
                                    )
                                )
                            ) as invest_case_area_info
                        from (
                            select bbd_qyxx_id, 
                                industry,
                                collect_list(
                                    area_num_amount_to_json(
                                        org_head,
                                        num,
                                        amount
                                    )
                                ) as area_num_amount_info_list  
                            from invest_case_view
                            group by bbd_qyxx_id, industry
                        )
                        group by bbd_qyxx_id
                    """
        industry_area_invest_df = entry.spark.sql(sql_str_2)
        return industry_area_invest_df

    @classmethod
    def new_invest_round(cls, entry: Entry):
        sql_str = """
                    select
                        bbd_qyxx_id,
                        industry, 
                        case when nvl(rounds,'') = '' then '其他'
                                 when rounds = 'null' then '其他'
                                 when rounds = 'NULL' then '其他'
                             else rounds 
                        end rounds,
                        num
                    from (
                        select 
                            bbd_qyxx_id,
                            industry, 
                            rounds,
                            count(1) num
                        from tz_basic
                        group by bbd_qyxx_id, industry, rounds
                    )  
                  """
        entry.spark.sql(sql_str).createOrReplaceTempView("view_invest_round")

        def rounds_num_to_json(rounds, num):
            data = {rounds: num}
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("rounds_num_to_json", rounds_num_to_json)

        def industry_rounds_num_to_json(industry, rounds_num_json_list):
            rounds_num_result = dict()
            for rounds_num_json in rounds_num_json_list:
                rounds_num_data = json.loads(rounds_num_json)
                for k, v in rounds_num_data.items():
                    rounds_num_result[k] = v
            data = {industry: rounds_num_result}
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("industry_rounds_num_to_json", industry_rounds_num_to_json)

        sql_str_2 = """
                        select bbd_qyxx_id, 
                            list_dict_to_single_dict_json(
                                collect_list(
                                    industry_rounds_num_to_json(
                                        industry, rounds_num_list
                                    )
                                )
                            ) as invest_round_info
                        from (
                           select bbd_qyxx_id, 
                                industry,
                                collect_list(
                                    rounds_num_to_json(
                                        rounds, num
                                    )
                                )as rounds_num_list
                            from view_invest_round
                            group by bbd_qyxx_id, industry 
                        )
                        group by bbd_qyxx_id
                    """
        industry_invest_round_df = entry.spark.sql(sql_str_2)
        return industry_invest_round_df

    @classmethod
    def new_invest_case_info(cls, entry: Entry):
        # 获取最近5年
        near_five_Year = [time.strftime('%Y', time.localtime(time.time() - 3600 * 24 * 365 * num)) for num in range(6)]
        start_year = near_five_Year[5]
        end_year = near_five_Year[1]

        sql_str = f"""
                    select bbd_qyxx_id,
                        industry, 
                        financies_year,
                        num,
                        amount
                    from (
                        select
                            bbd_qyxx_id,
                            industry, 
                            case when nvl(financies_year,'') = '' then '2000'
                                 else financies_year 
                            end financies_year,
                            num,
                            amount
                        from (  
                            select
                                bbd_qyxx_id,
                                industry, 
                                financies_year,
                                count(1) num,
                                sum(amount) amount
                            from tz_basic
                            group by bbd_qyxx_id, industry, financies_year
                        )
                    )
                    where financies_year between '{start_year}' and '{end_year}'
                   """
        entry.spark.sql(sql_str).createOrReplaceTempView("view_invest_case")

        def year_num_amount_to_json(year, num, amount):
            data = {
                year: {
                    "num": num if num and num != 'null' and num != '' else 0,
                    "amount": amount if amount and num != 'null' and num != '' else 0
                }
            }
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("year_num_amount_to_json", year_num_amount_to_json)

        def industry_year_num_amount(industry, year_num_amount_list):
            year_num_amount_result = dict()
            for year_num_amount in year_num_amount_list:
                year_num_amount_data = json.loads(year_num_amount)
                for k, v in year_num_amount_data.items():
                    year_num_amount_result[k] = v
            data = {industry: year_num_amount_result}
            return json.dumps(data, ensure_ascii=False)
        entry.spark.udf.register("industry_year_num_amount", industry_year_num_amount)

        sql_str_2 = """
                        select bbd_qyxx_id, 
                            list_dict_to_single_dict_json(
                                collect_list(
                                    industry_year_num_amount(industry, year_num_amount_list)
                                )
                            ) as invest_case_info
                        from(
                            select bbd_qyxx_id, 
                                industry, 
                                collect_list(
                                    year_num_amount_to_json(
                                        financies_year, num, amount
                                    )
                                ) as year_num_amount_list
                            from view_invest_case
                            group by bbd_qyxx_id, industry
                        )
                        group by bbd_qyxx_id
                    """

        industry_invest_case_info_dt = entry.spark.sql(sql_str_2)
        return industry_invest_case_info_dt

    @classmethod
    def invest_round(cls, entry):
        """
        数据源: basis_orgxx(投资机构表) && 投资事件(market_investment)
        需求: 统计投资事件的各个投资阶段对应的数量(A轮、B轮、天使轮等)
        """
        invest_round_df = entry.spark.sql("""
                select
                    bbd_qyxx_id,
                    case when nvl(rounds,'') = '' then '其他'
                             when rounds = 'null' then '其他'
                             when rounds = 'NULL' then '其他'
                        else rounds end rounds,
                    num
                from (
                    select 
                        bbd_qyxx_id,
                        rounds,
                        count(1) num
                    from tz_basic
                    group by bbd_qyxx_id,rounds
                ) a
            """)

        def to_json_data(row: Row) -> Row:
            tmp_data = row.asDict(True)
            push_data = dict()
            push_data["bbd_qyxx_id"] = tmp_data['bbd_qyxx_id']
            del tmp_data['bbd_qyxx_id']
            rounds = tmp_data['rounds']
            del tmp_data['rounds']
            push_data["invest_round_info"] = {rounds: str(v) for k, v in tmp_data.items()}
            return push_data
        json_rdd = invest_round_df.rdd.map(to_json_data)
        json_rdd3 = json_rdd.map(lambda x: (x['bbd_qyxx_id'], x['invest_round_info'])).groupByKey().map(
            lambda m: (m[0], list(m[1])))
        invest_round_info_tbl = json_rdd3.toDF(["bbd_qyxx_id", "invest_round_info"])
        return invest_round_info_tbl

    @classmethod
    def join_index(cls, entry):
        cls.jg_basic(entry)
        df = cls.risk_preference(entry)
        list = [
                cls.regulate_capital(entry),
                cls.invest_case(entry),
                cls.invest_industry_case(entry),
                cls.new_invest_area_case_info(entry),
                cls.new_invest_round(entry),
                cls.new_invest_case_info(entry)
        ]
        for index_df in list:
            df = df.join(index_df, 'bbd_qyxx_id', 'left')
        CnUtils.save_parquet(df, f"{CnUtils.get_temp_path(entry)}/tjzg_check_data")

        json_df_res = df.withColumn("update_time", fun.lit(CnUtils.now_time())) \
            .withColumn("date", fun.lit(f"{entry.version[:4]}-{entry.version[4:6]}-{entry.version[6:]}"))

        def to_push_data(row: Row):
            """
            返回订阅推送结构的数据
            :param row:
            :return:
            """
            tmp_data = row.asDict(True)
            print(tmp_data)
            push_data = {}
            tn = "cq_xdcy_invest_list"
            push_data["tn"] = tn
            push_data["data"] = {
                k: json.dumps(v, ensure_ascii=False) if k in ['invest_industry_case_info'] else v
                for k, v in tmp_data.items()
            }
            return push_data

        rdd_res = json_df_res.rdd.map(to_push_data)

        schema = StructType([
            StructField("tn", StringType(), True),
            StructField("data", MapType(StringType(), StringType()), True)])

        ## 数据保存路径
        entry.spark.createDataFrame(rdd_res, schema).repartition(20).write.json(f"{CnUtils.get_final_path(entry)}/cq_xdcy_invest_list", mode="overwrite")

        return json_df_res


def pre_check(entry: Entry):
    return True


def main(entry: Entry):

    ## 投资机构
    entry, CalIndex.join_index(entry)


def post_check(entry: Entry):
    return True
