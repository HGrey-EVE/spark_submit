#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 经信厅市州发展
"""
from pyspark.sql import SparkSession, DataFrame
import datetime
import os

from whetstone.core.entry import Entry
from jingxinting.proj_common.hive_util import HiveUtil
from jingxinting.proj_common.common_util import save_text_to_hdfs
from jingxinting.proj_common.date_util import DateUtils
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy
from jingxinting.proj_common.data_output_util import ResultOutputUtil


def hive_table_init(
        spark,
        tablename: str,
        new_table=None,
        database='dw',
        dateNo=None
):
    """
    初始化hive表
    :param spark:
    :param tablename:需要初始化的hive表
    :param new_table:新建的临时表名(默认和原hive表同名)
    :param database:
    :param dateNo: 在某个月的最新分区,注意日期格式要和分区格式一致，eg:201901,2019-01
    """

    if new_table is None:
        new_table = tablename

    if dateNo is None:
        spark.sql('''
            select a.* from {table} a  where dt = {new_par}
        '''.format(new_par=HiveUtil.newest_partition(spark, f'{database}.{tablename}'),
                   table=f'{database}.{tablename}')) \
            .createOrReplaceTempView(f'{new_table}')
    else:
        spark.sql('''
             select a.* from {table} a  where dt = {new_par}
        '''.format(new_par=HiveUtil.newest_partition_by_month(spark, table=f'{database}.{tablename}',
                                                              in_month_str=str(dateNo)),
                   table=f'{database}.{tablename}')).createOrReplaceTempView(f'{new_table}')


'''
    提供执行并打印sql，将结果生成临时表和写入hdfs的方法
'''


def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        paquest_path=None,
        repartition_number=100,
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


class City_development:

    def get_sichuan_basic(self, entry: Entry):
        '''
           添加经济区划分
           四川省五大经济区是指：
            （1）成都平原经济区包括8个市州：成都市、德阳市、绵阳市、遂宁市、乐山市、雅安市、眉山市、资阳市。
            （2）川南经济区包括4个市州：自贡市、泸州市、内江市、宜宾市.
            （3）川东经济区包括5个市州：广元市、南充市、广安市、达州市、巴中市.
            （4）攀西经济区包括2个市州：攀枝花市、凉山市.
            （5）川西北生态示范区包括2个市州：阿坝州、甘孜州。
        '''

        sql = '''
            select 
                a.*,
                case when company_city rlike'成都|德阳|绵阳|遂宁|乐山|雅安|眉山|资阳' then 1
                     when company_city rlike '自贡|泸州|内江市|宜宾' then 2 
                     when company_city rlike '广元|南充|广安|达州|巴中' then 3
                     when company_city rlike '攀枝花|凉山' then 4
                     when company_city rlike '阿坝|甘孜' then 5 
                end sc_econo_region_type
            from qyxx_basic a
            where company_province = '四川'
            and company_enterprise_status like '%%存续%%'
        '''
        sql_excute_newtable_tohdfs(sql=sql, newtablename='qyxx_basic_2', entry=entry)

    # 按照市州划分,计算企业数量和投资金额（注册金额）
    def get_sc_area_development_1(self, entry: Entry):
        sql = f'''
            select
                substring(esdate,1,7) as static_month,
                '1' as city_or_zone,
                city_code as city_eco_zone,
                a.sc_econo_region_type,
                count(distinct bbd_qyxx_id) as company_number,
                sum(regcap_amount) as investment_amount
            from
                qyxx_basic_2 a 
            group by 
                substring(esdate,1,7),
                city_code,
                a.sc_econo_region_type
        '''
        df = sql_excute_newtable_tohdfs(sql=sql, newtablename='sc_area_development_1', entry=entry)

        return df

    # 按照经济区进行划分
    def get_sc_region_devolopment_1(self, entry: Entry):
        sql2 = '''
            select 
                static_month,
                '2' as city_or_zone,
                sc_econo_region_type as city_eco_zone,
                sum(company_number) as company_number,
                sum(investment_amount) as investment_amount
            from sc_area_development_1 a 
            group by 
            static_month,
            sc_econo_region_type
        '''
        df = sql_excute_newtable_tohdfs(sql=sql2, entry=entry)
        return df

    # 各市州月度新增招聘数量
    def get_sc_area_development_2(self, entry: Entry):
        sql = '''
            select
                substring(b.pubdate,1,7) as static_month,
                a.city_code as city_eco_zone,
                a.sc_econo_region_type,
                sum(b.bbd_recruit_num) as recruit_num
            from qyxx_basic_2 a 
            inner join 
            manage_recruit b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by 
                substring(b.pubdate,1,7),
                a.city_code,
                a.sc_econo_region_type
        '''
        df = sql_excute_newtable_tohdfs(sql=sql, newtablename='sc_area_development_2', entry=entry)

        return df

    # 按照经济区进行划分
    def get_sc_region_devolopment_2(self, entry: Entry):
        sql2 = '''
            select 
                static_month,
                sc_econo_region_type as city_eco_zone,
                sum(recruit_num) as recruit_num
            from sc_area_development_2  a 
            group by 
            static_month,
            sc_econo_region_type
        '''
        df = sql_excute_newtable_tohdfs(sql=sql2, entry=entry)

        return df

    # 各市州专利数量
    def get_sc_area_development_3(self, entry: Entry):
        sql = '''
        select 
            static_month,city_eco_zone,sc_econo_region_type,
            sum(patent_num) over(partition by substring(static_month,1,4),city_eco_zone,sc_econo_region_type order by static_month) as patent_num
        from             
            (select 
                substring(b.publidate,1,7) as static_month,
                a.city_code as city_eco_zone,
                a.sc_econo_region_type,
                count(distinct patent_code) as patent_num
            from qyxx_basic_2 a 
            inner join 
            patent_info b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            group by 
            substring(b.publidate,1,7),
            a.city_code,
            a.sc_econo_region_type
            ) a 
        '''
        df = sql_excute_newtable_tohdfs(sql=sql, newtablename='sc_area_development_3', entry=entry)

        return df

    # 按照经济区划分
    def get_sc_region_devolopment_3(self, entry: Entry):
        sql2 = '''
            select 
                static_month,
                sc_econo_region_type as city_eco_zone,
                sum(patent_num) as patent_num
            from sc_area_development_3 
            group by 
            static_month,
            sc_econo_region_type
        '''
        df = sql_excute_newtable_tohdfs(sql=sql2, entry=entry)

        return df


class Get_merge_data:

    # 获取基础数据源
    def get_basic_data(self, entry: Entry):
        s = BasicDataProxy(entry=entry)

        # 基础表
        basic_df = s.get_qyxx_basic_df()
        basic_df.cache().createOrReplaceTempView('qyxx_basic')

        # 招聘表
        recruit_df = s.get_recruit_info_df()
        recruit_df.createOrReplaceTempView('manage_recruit')

        # 专利表
        patent_df = s.get_patent_info_df()
        patent_df.createOrReplaceTempView('patent_info')

    def merge_index(self, entry: Entry):
        s = City_development()
        s.get_sichuan_basic(entry=entry)

        df1: DataFrame = s.get_sc_area_development_1(entry)
        df4: DataFrame = s.get_sc_region_devolopment_1(entry)

        df2: DataFrame = s.get_sc_area_development_2(entry)
        df5: DataFrame = s.get_sc_region_devolopment_2(entry)

        df3: DataFrame = s.get_sc_area_development_3(entry)
        df6: DataFrame = s.get_sc_region_devolopment_3(entry)

        join_list_1 = ['static_month', 'city_eco_zone', 'sc_econo_region_type']
        join_list_2 = ['static_month', 'city_eco_zone']

        result1 = df1.join(df2, join_list_1, 'left').fillna(0). \
            select('static_month', 'city_or_zone', 'city_eco_zone', 'investment_amount', 'company_number',
                   'recruit_num')
        result2 = result1.join(df3, join_list_2, 'left').fillna(0). \
            select('static_month', 'city_or_zone', 'city_eco_zone', 'investment_amount', 'company_number',
                   'recruit_num', 'patent_num')

        result3 = df4.join(df5, join_list_2, 'left').fillna(0). \
            select('static_month', 'city_or_zone', 'city_eco_zone', 'investment_amount', 'company_number',
                   'recruit_num')
        result4 = result3.join(df6, join_list_2, 'left').fillna(0). \
            select('static_month', 'city_or_zone', 'city_eco_zone', 'investment_amount', 'company_number',
                   'recruit_num', 'patent_num')

        result = result2.union(result4)

        return result


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):
    s = Get_merge_data()

    s.get_basic_data(entry=entry)

    result = s.merge_index(entry=entry)

    ##处理结果,存入对应结果表
    r = ResultOutputUtil(entry=entry, table_name='monthly_info_city', month_value_fmt='%Y-%m')
    r.save(result)
    # entry.logger.info(result.take(10))
