#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 经信厅国内对标
"""
from pyspark.sql import SparkSession, DataFrame
import datetime
import os

from whetstone.core.entry import Entry
from jingxinting.biz_city_development.city_count import hive_table_init, sql_excute_newtable_tohdfs
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy
from jingxinting.proj_common.data_output_util import ResultOutputUtil
from jingxinting.proj_common.hive_util import HiveUtil


class Demestic_compare:

    def __init__(self, entry: Entry):
        self.database = entry.cfg_mgr.get('hive-dw', 'database')

    # 处理基础数据
    def get_basic_data(self, entry: Entry):
        # 得到表的最新分区
        table1 = f'{self.database}.qyxx_basic'
        basic_dt = HiveUtil.newest_partition(entry.spark, table1)

        table2 = f'{self.database}.company_county'
        company_dt = HiveUtil.newest_partition(entry.spark, table2)

        sql = f"""
            select 
                distinct a.bbd_qyxx_id,a.esdate,b.province,a.regcap_amount/10000 as regcap_amount
            from 
            {self.database}.qyxx_basic a 
            inner join  
            (
            select *
            from 
            (SELECT *,
                row_number() over (partition by code order by tag) as rank 
                FROM {self.database}.company_county
                WHERE dt = '{company_dt}' and tag!=-1
                ) a 
            WHERE rank=1
            )b 
            on a.company_county = b.code
            and  a.dt = '{basic_dt}'
            and a.bbd_qyxx_id is not null
            and a.bbd_qyxx_id != 'null'
            and substr(a.company_companytype, 1, 2) not in ('91', '92', '93')
            and a.company_enterprise_status like '%%存续%%'
        """
        sql_excute_newtable_tohdfs(entry=entry, newtablename='qyxx_basic', is_cache=True, sql=sql)

    # 计算各省份企业数量和投资金额
    def get_company_index_1(self, entry: Entry):
        sql = '''
         select 
                substring(esdate,1,7) as static_month,
                province,
                count(distinct bbd_qyxx_id) as company_number,
                sum(regcap_amount) as investment_amount
            from qyxx_basic a 
            group by 
                substring(esdate,1,7),
                province
        '''
        df = sql_excute_newtable_tohdfs(sql=sql, entry=entry)

        return df

    # 计算各省招聘数量
    def get_company_index_2(self, entry: Entry):
        sql = '''
            select
                substring(b.pubdate,1,7) as static_month,
                a.province,
                sum(b.bbd_recruit_num) as recruit_num
            from qyxx_basic a 
            inner join 
            manage_recruit b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by 
                substring(b.pubdate,1,7),
                a.province
        '''

        return sql_excute_newtable_tohdfs(sql=sql, entry=entry)

    # 计算各省的专利数
    def get_company_index_3(self, entry: Entry):
        sql = '''
            select 
            static_month,province,
            sum(patent_num) over(partition by substring(static_month,1,4),province order by static_month) as patent_num
            from 
            (select 
                substring(b.publidate,1,7) as static_month,
                a.province,
                count(distinct patent_code) as patent_num
            from qyxx_basic a 
            inner join 
            patent_info b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by 
            substring(b.publidate,1,7),
            a.province
            ) a 
        '''

        rd = sql_excute_newtable_tohdfs(sql=sql, entry=entry)

        return rd


class Get_merge_data:

    def __init__(self, entry: Entry):
        self.database = entry.cfg_mgr.get('hive-dw', 'database')

    # 获取基础数据源
    def get_basic_data(self, entry: Entry):
        # 招聘表
        table_name = f'{self.database}.manage_recruit'
        new_dt = HiveUtil.newest_partition(spark=entry.spark, table_name=f'{table_name}')
        recruit_df = entry.spark.sql(f'''
            select * 
            from {table_name}
            where dt = '{new_dt}'
                and bbd_qyxx_id is not null
                and bbd_qyxx_id != 'null'
        ''')
        recruit_df.createOrReplaceTempView('manage_recruit')

        # 专利表
        table_name = f'{self.database}.prop_patent_data'
        new_dt = HiveUtil.newest_partition(spark=entry.spark, table_name=f'{table_name}')
        columns = """
                    L.bbd_qyxx_id, L.publidate, L.patent_type,
                    L.title, L.public_code as patent_code, L.application_date
                """

        patent_df = entry.spark.sql(f'''
            select 
             {columns}
            from {table_name} L
            where dt = '{new_dt}'
                and bbd_qyxx_id is not null
                and bbd_qyxx_id != 'null'
        ''')
        patent_df.createOrReplaceTempView('patent_info')

    # 将多个结果merge
    def merge_index(self, entry: Entry):
        s = Demestic_compare(entry=entry)
        s.get_basic_data(entry=entry)
        rdd1: DataFrame = s.get_company_index_1(entry=entry)
        rdd2: DataFrame = s.get_company_index_2(entry=entry)
        rdd3: DataFrame = s.get_company_index_3(entry=entry)

        join_list = ['static_month', 'province']
        rdd1 = rdd1.join(rdd2, join_list, 'left').fillna(0)
        df = rdd1.join(rdd3, join_list, 'left').fillna(0)

        return df


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    g = Get_merge_data(entry=entry)

    g.get_basic_data(entry=entry)

    result = g.merge_index(entry=entry)

    # 将结果写入结果表
    r = ResultOutputUtil(entry=entry, table_name='monthly_info_province', month_value_fmt='%Y-%m')
    r.save(result)

    # entry.logger.info(result.take(10))
