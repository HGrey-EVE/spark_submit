#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 投资价值
:Author: luzihao@bbdservice.com
:Date: 2021-04-28 16:08
"""
import datetime
import os

from pyspark import Row
from whetstone.core.entry import Entry
from cqxdcy.proj_common.hive_util import HiveUtil
from cqxdcy.proj_common.json_util import JsonUtils
from pyspark.sql import functions as fun, DataFrame

from cqxdcy.proj_common.date_util import DateUtils
from cqxdcy.proj_common.log_track_util import LogTrack

COMPANY_KEY = '高级|资深|工程师|科学家|大数据|算法|人工智能|AI|识别|机器学习|神经网络|区块链|架构|研发|开发'

CAFE_COMPANY = '/user/cqxdcyfzyjy/temp/cafe_temp'

# class LogTrack:
#     @staticmethod
#     def log_track():
#         def actrual_decorator(function):
#             def wrapper(*args, **kwargs):
#                 args[1].logger.info(f" (°ο°)~~ START EXEC method {function.__name__} , waiting ~~(°ο°)")
#                 result = function(*args, **kwargs)
#                 args[1].logger.info(f" O(∩_∩)O  ENG  EXEC method {function.__name__} ,  success ~~*@ο@*~~")
#                 return result
#             return wrapper
#         return actrual_decorator

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

class PushData:


    @classmethod
    @LogTrack.log_track
    def push_data(cls, entry: Entry, data: DataFrame):
        # CnUtils.saveToHdfs(data.rdd.map(cls.to_push_data),f"{CnUtils.get_temp_path(entry)}/cq_xdcy_tzjz_invest")
        data.distinct().repartition(1).write.csv(CnUtils.get_temp_path(entry) + "/cq_xdcy_tzjz_invest", header=True,sep='\t',mode='overwrite')

    # @classmethod
    # @LogTrack.log_track()
    # def save_to_csv(cls, entry: Entry, module, module_name, partition_num=100):
    #     os.system("hadoop fs -rm -r -skipTrash " + CnUtils.get_final_path(entry) + module_name)
    #     module.distinct().repartition(partition_num).write.csv(CnUtils.get_final_path(entry) + module_name, header=True, sep='\t')
    #     return


class CalIndex:

    @staticmethod
    @LogTrack.log_track
    def exec_sql(sql: str, entry: Entry):
        entry.logger.info(sql)
        return entry.spark.sql(sql)

    @staticmethod
    def get_company_county(entry: Entry):
        """
        获取省市区
        :return: DataFrame
        """
        # dt = HiveUtil.newest_partition(entry.spark, 'dw.company_county')
        dt = HiveUtil.newest_partition_by_month(entry.spark, 'dw.company_county', in_month_str=entry.version[:6])
        df = entry.spark.sql("""                                                                            
                      select company_county, province, city, region                                    
                      from                                                                             
                          (select code as company_county, province, city, district as region,          
                                      (row_number() over(partition by code order by tag )) as row_num  
                          from dw.company_county                                                       
                          where dt = '{dt}' and tag != -1) b                                           
                      where row_num = 1                                                                
                 """.format(dt=dt))
        return df

    @staticmethod
    def get_IT_worker_index(entry: Entry):
        # company_list = entry.spark.read.parquet(CnUtils.get_source_path(entry) + "/industry_divide" )
        company_list = entry.spark.read.parquet(f"/user/cqxdcyfzyjy/{entry.version}/input/cqxdcy/industry_divide")
        company_list.createOrReplaceTempView('company_tmp_v')

        # manage_recruit_dt = HiveUtil.newest_partition(entry.spark, 'dw.manage_recruit')
        manage_recruit_dt = HiveUtil.newest_partition_by_month(entry.spark, 'dw.manage_recruit', in_month_str=entry.version[:6])

        # 研发人员总数
        it_worker_count = entry.spark.sql(
            '''
            select a.bbd_qyxx_id, sum(coalesce(bbd_recruit_num, 0)) as T64 
            from 
            (select distinct bbd_recruit_num, bbd_qyxx_id, pubdate from dw.manage_recruit where dt={a_dt} and (job_title rlike '{COMPANY_KEY}' 
            or job_functions rlike '{COMPANY_KEY}'
            or job_descriptions rlike '{COMPANY_KEY}')) a
            join company_tmp_v b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
            '''.format(a_dt=manage_recruit_dt, COMPANY_KEY=COMPANY_KEY)
        )

        it_worker_count.createOrReplaceTempView('it_worker_count')

        # 招聘人员总数
        worker_count = entry.spark.sql(
            '''
            select a.bbd_qyxx_id, sum(coalesce(bbd_recruit_num, 0)) as worker_count 
            from 
            (select distinct bbd_recruit_num, bbd_qyxx_id, pubdate FROM dw.manage_recruit where dt={a_dt}) a
            join company_tmp_v b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
            '''.format(a_dt=manage_recruit_dt)
        )

        worker_count.createOrReplaceTempView('worker_count')

        # 研发人员占比
        it_worker_present = entry.spark.sql(
            '''
            select a.bbd_qyxx_id, round(T64/worker_count , 3) as T65
            from
            it_worker_count a
            join worker_count b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            '''
        )

        # qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition(entry.spark, "dw.qyxx_jqka_cwzb_zhsy")
        qyxx_jqka_cwzb_zhsy_dt = HiveUtil.newest_partition_by_month(entry.spark, "dw.qyxx_jqka_cwzb_zhsy", in_month_str=entry.version[:6])

        # 研发投入
        it_money = entry.spark.sql(
            '''
            select a.bbd_qyxx_id, 
            sum(case a.r_and_d_cost when null then 0 when '--' then 0 else cast(a.r_and_d_cost as int) end) as T66 
            from (SELECT * FROM (SELECT r_and_d_cost, bbd_qyxx_id, row_number() over (partition by bbd_qyxx_id order by report_date desc) as rn 
            FROM dw.qyxx_jqka_cwzb_zhsy where dt={a_dt} and date_format(report_date, "MM") = 12) WHERE rn = 1) a
            join company_tmp_v b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
            '''.format(a_dt=qyxx_jqka_cwzb_zhsy_dt)
        )

        it_money.createOrReplaceTempView('it_money')

        # 研发投入与收入比
        it_money_cost = entry.spark.sql(
            '''
            select a.bbd_qyxx_id, 
            sum(case a.total_revenue when null then 0 when '--' then 0 else cast(a.total_revenue as int) end) as revenue 
            from (SELECT * FROM (SELECT total_revenue, bbd_qyxx_id, row_number() over (partition by bbd_qyxx_id order by report_date desc) as rn 
            FROM dw.qyxx_jqka_cwzb_zhsy where dt={a_dt} and date_format(report_date, "MM") = 12) WHERE rn = 1) a
            join company_tmp_v b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by a.bbd_qyxx_id
            '''.format(a_dt=qyxx_jqka_cwzb_zhsy_dt)
        )

        it_money_cost.createOrReplaceTempView('it_money_cost')

        it_money_persent = entry.spark.sql(
            '''
            select a.bbd_qyxx_id , round(a.T66/b.revenue, 3) as T67
            from 
            it_money a
            join it_money_cost b
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            '''
        )

        # cafe
        ids_df = entry.spark.read.csv(CAFE_COMPANY, header=True, sep='\t')
        ids_df.createOrReplaceTempView('cafe_temp')

        cafe_count = entry.spark.sql(
            '''                                                                                                                        
            select b.bbd_qyxx_id,`毛利率` as T1,`净利率` as T2,`净资产收益率` as T3,`资产负债率` as T4,`年末短期债务比率` as T5,                                
            `经营活动净现金流` as T6,`投资活动净现金流` as T7,`筹资活动净现金流` as T8,`经营活动净现金流权益比` as T9,`投资活动净现金流权益比` as T10,                               
            `筹资活动净现金流权益比` as T11,`营业总收入增长率` as T12,`归属母公司股东权益合计增长率` as T13,`资产总计增长率` as T14,                                           
            `营业利润增长率` as T15,`净利润增长率` as T16,`应收账款周转率` as T17,`存货周转率` as T18,`固定资产周转率` as T19,`总资产周转率` as T20,                         
            `货币资金占流动资产比率` as T21,`应收账款占流动资产比率` as T22,`预付款项占流动资产比率` as T23,`存货占流动资产比率` as T24,                                         
            `流动资产合计 / 总资产` as T25,`长期投资占非流动资产比率` as T26,`固定资产占非流动资产比率` as T27,`无形资产占非流动资产比率` as T28,               
            `非流动资产合计 / 总资产` as T29 from                                                                                                
            cafe_temp a                                                                                                                
            right join company_tmp_v b                                                                                                 
            on a.bbd_qyxx_id = b.bbd_qyxx_id                                                                                           
            '''
        )

        # index_pool_test
        index_pool_1 = entry.spark.sql(
            '''                                                                                                                                             
            select b.bbd_qyxx_id, qy_ssgs_is as T30, qy_sfwgyyq_is as T31, qy_zrrgddwtz_num as T32, qy_djgdwtz_num as                                       
            T33, qy_frdwtz_num as T34, qy_gqfrgd_num as T35, qy_xzxk_is as T36, qy_zl_num as T38, qy_fmzl_num_dim as T39,                                   
            qy_zprs_num as T40, qy_ssjyszp_num as T41, qy_deg1_glf_num as T42, qy_txjsqyzz_is as T_18Y, qy_jzlqyzz_is as T_19Y,                             
            qy_gcjlzz_is as T_20Y, qy_xxxtgcjlzz_is as T_21Y, qy_srrz_is as T_22Y, qy_gycpscxkz_is as T_23Y, qy_nyscxkz_is as T_24Y,                        
            qy_spscxkz_is as T_25Y, qy_ypscxkz_is as T_26Y, qy_ypjyxkz_is as T_27Y, qy_hzpscxkz_is as T_28Y, qy_GMPrz_is as T_29Y,                          
            qy_hgdj_is as T_30Y, qy_cktkxk_is as T_31Y, qy_deg1_zrr_rate as T46, qy_deg2_glf_num as T47, qy_deg2_zrr_rate as T48,                           
            qy_deg1_fmzl_num_avg as T49, qy_deg1_zz_num_avg as T50, qy_deg1_zprs_num_avg as T51, qy_deg1_zl_num_avg as T53, qy_deg1_ssjyszp_num_avg as T54, 
            qy_deg1_dwtz_avg as T55, qy_deg1_fzjg_avg as T56, qy_deg1_gqfr_rate as T57, qy_deg1_ssfr_avg as T58, qy_zl_num as T59,                          
            qy_zprs_num as T62_1, qy_ssjyszp_num as T62_2, qy_rz_num as T63                                                                                 
            from dw.index_pool a                                                                                                                            
            right join company_tmp_v b                                                                                                                      
            on a.bbd_qyxx_id = b.bbd_qyxx_id                                                                                                                
            '''
        )

        # 添加省份
        CalIndex.get_company_county(entry).createOrReplaceTempView('company_country')
        company_province = entry.spark.sql(
            '''                                                         
            select b.bbd_qyxx_id, a.province, a.city, a.region          
            from company_country a                                      
            right join company_tmp_v b                                  
            on a.company_county = b.company_county                      
            '''
        )

        # 合并指标
        index_result = company_list.select('bbd_qyxx_id', 'industry', 'sub_industry')
        for index_df in [company_province, it_worker_count, it_worker_present, it_money, it_money_persent,
                         cafe_count, index_pool_1]:
            index_result = index_result.join(index_df, ['bbd_qyxx_id'], 'left')
        return index_result


def pre_check(entry: Entry):
    return True


def main(entry: Entry):

    # PushData.save_to_csv(entry, CalIndex.get_IT_worker_index(entry), 'tzjz', 1)

    PushData.push_data(entry, CalIndex.get_IT_worker_index(entry))

def post_check(entry: Entry):
    return True