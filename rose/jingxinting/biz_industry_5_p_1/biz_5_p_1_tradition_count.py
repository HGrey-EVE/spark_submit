#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 经信厅5+1产业下的传统产业的指标计算
"""
from pyspark.sql import SparkSession, DataFrame
import os
import re

from whetstone.core.entry import Entry
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy
from jingxinting.proj_common.data_output_util import ResultOutputUtil

'''
    提供执行并打印sql，将结果生成临时表和写入hdfs的方法
'''


def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        paquest_path=None,
        repartition_number=500,
        is_cache=False
):
    entry.logger.info(f'当前执行sql为:\n{sql}')

    df: DataFrame = entry.spark.sql(sql)

    if is_cache:
        df.cache()

    if newtablename:
        df.createOrReplaceTempView(f'{newtablename}')
        entry.logger.info(f'生成临时表名为:{newtablename}')

    if paquest_path:
        df.repartition(repartition_number).write.parquet(paquest_path, mode='overwrite')
        # save_text_to_hdfs(df=df, repartition_number=repartition_number, path=paquest_path)
        entry.logger.info(f'写入hdfs地址为{paquest_path}')
        return paquest_path

    return df


class Ttadition_5_1:

    def __init__(self, entry: Entry):
        savepath = entry.cfg_mgr.hdfs.get_tmp_path("hdfs-biz", "path_5p1_tmp_data")
        self.savepath1 = os.path.join(savepath, "data_tradition_qys")
        self.savepath2 = os.path.join(savepath, "data_tradition_zps")
        self.savepath3 = os.path.join(savepath, "data_tradition_zls")
        self.savepath4 = os.path.join(savepath, "data_tradition_tzje")
        self.version = entry.version[:4] + '-' + entry.version[4:6]

    def get_basic(self, entry: Entry):
        # 先整理计算基础表
        sql = f'''
            select 
                distinct substring (esdate,1,7) as static_month,company_type,company_enterprise_status,
                company_industry,bbd_qyxx_id,city_code,key_enterprise,regcap_amount
            from qyxx_basic a 
            where substring (esdate,1,7)  <= '{self.version}'
            '''

        sql_excute_newtable_tohdfs(sql=sql, newtablename='basic_info', is_cache=True, entry=entry)

    def exec_index_1(self, entry: Entry):
        # 计算投资金额（注册资本）
        sql = f'''
                   select 
                       static_month,
                       company_industry as industry_type,
                       a.key_enterprise as company_type,
                       a.city_code as city,
                       a.company_type as company_nature,
                       sum(regcap_amount) as investment_amount
                   from basic_info a 
                   where company_enterprise_status like '%%存续%%'
                   group by 
                       static_month,
                       company_industry,
                       key_enterprise,
                       city_code,
                       company_type
               '''
        sql_excute_newtable_tohdfs(sql=sql, paquest_path=self.savepath4, entry=entry)

    def exec_index_2(self, entry: Entry):
        # 计算专利数
        sql = f'''
                select 
                static_month,industry_type,company_type,city,company_nature,
                sum(patent_num) over(partition by substring(static_month,1,4),industry_type,company_type,city,company_nature order by static_month) as patent_num
                from
                    (select 
                       substring(b.publidate,1,7) as static_month,
                       company_industry as industry_type,
                       a.key_enterprise as company_type,
                       a.city_code as city,
                       a.company_type as company_nature,
                       count(distinct b.patent_code) as patent_num
                   from basic_info a 
                   inner join 
                   patent_info b 
                   on a.bbd_qyxx_id = b.bbd_qyxx_id 
                   and a.company_enterprise_status like '%%存续%%'
                   group by 
                   substring(b.publidate,1,7),
                   company_industry,
                   a.city_code,
                   a.key_enterprise,
                   a.company_type
                   ) a 
               '''

        sql_excute_newtable_tohdfs(sql=sql, paquest_path=self.savepath3, entry=entry)

    def exec_index_3(self, entry: Entry):
        # 计算招聘数
        sql = f'''
                   select
                   substring(b.pubdate,1,7) as static_month,
                   a.company_industry as industry_type,
                   a.key_enterprise as company_type,
                   a.city_code as city,
                   a.company_type as company_nature,
                   sum(b.bbd_recruit_num) as recruit_num
               from basic_info a 
               inner join 
               manage_recruit b 
               on a.bbd_qyxx_id = b.bbd_qyxx_id 
               and a.company_enterprise_status like '%%存续%%'
               group by 
                   substring(b.pubdate,1,7),
                   company_industry,
                   a.city_code,
                   a.key_enterprise,
                   company_type
               '''
        sql_excute_newtable_tohdfs(sql=sql, paquest_path=self.savepath2, entry=entry)

    def exec_index_4(self, entry: Entry):
        # 存续企业数/注销企业数/外迁企业数/计算企业性质分布（数量分布）
        sql = f'''
                       select 
                            static_month,industry_type,company_type,city,company_nature,
                            survival_num,cancellation_num,out_num,
                            cast(survival_num as int)+cast(num2 as int)+cast(num3 as int) as company_num
                       from 
                           (select 
                               static_month,industry_type,company_type,city,company_nature,
                               sum(survival_num) over(partition by industry_type,company_type,city,company_nature order by static_month) as survival_num,
                               sum(cancellation_num) over(partition by industry_type,company_type,city,company_nature order by static_month) as num2,
                               sum(out_num) over(partition by industry_type,company_type,city,company_nature order by static_month) as num3,
                               cancellation_num,out_num
                           from 
                               (select 
                                   static_month,
                                   a.company_industry as industry_type,
                                   a.key_enterprise as company_type,
                                   a.city_code as city,
                                   a.company_type as company_nature,
                                   count(distinct case when company_enterprise_status like '%%存续%%' then bbd_qyxx_id end) as survival_num,
                                   count(distinct case when company_enterprise_status like '%%注销%%' then bbd_qyxx_id end) as cancellation_num,
                                   count(distinct case when company_enterprise_status like '%%迁出%%' then bbd_qyxx_id end) as out_num
                               from basic_info a 
                               group by 
                                   static_month,
                                   company_industry,
                                   key_enterprise,
                                   city_code,
                                   company_type
                               ) m 
                           ) n 
                   '''

        # sql_excute_newtable_tohdfs(sql=sql, paquest_path=self.savepath1, entry=entry)

        sql1 = f'''
            select 
               static_month,industry_type,company_type,city,company_nature,
               sum(survival_num) over(partition by industry_type,company_type,city,company_nature order by static_month) as survival_num,
               sum(cancellation_num) over(partition by industry_type,company_type,city,company_nature order by static_month) as num2,
               sum(out_num) over(partition by industry_type,company_type,city,company_nature order by static_month) as num3,
               cancellation_num,out_num
           from 
               (select 
                   static_month,
                   a.company_industry as industry_type,
                   a.key_enterprise as company_type,
                   a.city_code as city,
                   a.company_type as company_nature,
                   count(distinct case when company_enterprise_status like '%%存续%%' then bbd_qyxx_id end) as survival_num,
                   count(distinct case when company_enterprise_status like '%%注销%%' then bbd_qyxx_id end) as cancellation_num,
                   count(distinct case when company_enterprise_status like '%%迁出%%' then bbd_qyxx_id end) as out_num
               from basic_info a 
               group by 
                   static_month,
                   company_industry,
                   key_enterprise,
                   city_code,
                   company_type
               ) m  
        '''
        sql_excute_newtable_tohdfs(sql=sql1, entry=entry, newtablename='tmp1')

        sql2 = f'''
            select 
            *
            from 
            (select 
            row_number() over(partition by industry_type,company_type,city,company_nature order by static_month desc) as row_id,a.*
            from tmp1 a ) m 
            where row_id = 1
        '''
        sql_excute_newtable_tohdfs(sql=sql2, entry=entry, newtablename='tmp2')

        sql3 = '''
            select 
            a.static_month,b.industry_type,b.company_type,b.city,b.company_nature,b.survival_num,
            b.num2,b.num3,b.cancellation_num,b.out_num
            from 
                (select distinct static_month from tmp1) a ,tmp2 b 
                where a.static_month > b.static_month
        '''
        sql_excute_newtable_tohdfs(sql=sql3, entry=entry, newtablename='tmp3')

        sql4 = '''
        select 
            static_month,industry_type,company_type,city,company_nature,
            survival_num,cancellation_num,out_num,
            cast(survival_num as int)+cast(num2 as int)+cast(num3 as int) as company_num
        from tmp1
        union all
        select 
            static_month,industry_type,company_type,city,company_nature,
            survival_num,cancellation_num,out_num,
            cast(survival_num as int)+cast(num2 as int)+cast(num3 as int) as company_num
        from tmp3
        '''
        sql_excute_newtable_tohdfs(sql=sql4, entry=entry, paquest_path=self.savepath1)


class Merge_data:

    def get_basic_info(self, entry: Entry):
        s = BasicDataProxy(entry=entry)

        # 基础表
        basic_df = s.get_qyxx_basic_df()
        basic_df.createOrReplaceTempView('qyxx_basic')

        # 招聘表
        recruit_df = s.get_recruit_info_df()
        recruit_df.cache().createOrReplaceTempView('manage_recruit')

        # 专利表
        patent_df = s.get_patent_info_df()
        patent_df.cache().createOrReplaceTempView('patent_info')

    def exec_merge(self, entry: Entry):
        S = Ttadition_5_1(entry=entry)
        S.get_basic(entry=entry)
        S.exec_index_1(entry=entry)
        S.exec_index_2(entry=entry)
        S.exec_index_3(entry=entry)
        S.exec_index_4(entry=entry)

        df1: DataFrame = entry.spark.read.parquet(S.savepath1)
        df2: DataFrame = entry.spark.read.parquet(S.savepath2)
        df3: DataFrame = entry.spark.read.parquet(S.savepath3)
        df4: DataFrame = entry.spark.read.parquet(S.savepath4)

        join_cond = ['static_month', 'industry_type', 'company_type', 'city', 'company_nature']

        result = df1.join(df2, join_cond, 'left').join(df3, join_cond, 'left').join(df4, join_cond, 'left').fillna(0)

        return result


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    m = Merge_data()
    m.get_basic_info(entry=entry)
    result = m.exec_merge(entry=entry)

    # 处理结果数据
    r = ResultOutputUtil(entry=entry, table_name='monthly_info_industry_old', month_value_fmt='%Y-%m')
    r.save(result)
