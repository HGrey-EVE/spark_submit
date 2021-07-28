#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--经济类型（内部数据）
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame
from scwgj.proj_common.hive_util import HiveUtil

'''
1.【企业结汇】取人民币账户（剔除意愿结汇）
2.【人民币交易数据】以第1步人民币账户支出对手计数
【企业结汇】交易编码=‘121010’and 结汇用途代码!='022'
'''


@register(name='business_type',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['all_off_line_relations_degree2_v', 'all_company_v',
                        'target_company_v', 'qyxx_basic_v'])
def business_type(param: Parameter, entry: Entry):

    # 找出目标企业的二度及二度以内关联方
    sql1 = '''
        select 
        a.corp_code,a.company_name,
        b.relation_corp_code,b.relation_company_name,
        case when b.corp_code is not null then 1 end has_degree2
        from target_company_v a 
        left join 
        (select a.corp_code,
            b.corp_code as relation_corp_code,
            b.company_name as relation_company_name
        from all_off_line_relations_degree2_v a 
        join all_company_v b 
        on a.company_rel_degree_2 = b.company_name
        ) b  
        on a.corp_code = b.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql1, entry=entry, newtablename='business_type_tmp1')

    # 公司以及二度关联方都匹配出对应的company_type
    sql3 = '''
        select 
        distinct m1.corp_code,m1.company_type_1,m2.company_type as company_type_2
        from 
        (select 
            a.*,b.company_type as company_type_1
            from 
            business_type_tmp1 a 
            left join 
            qyxx_basic_v b 
            on a.company_name = b.company_name
        ) m1 
        left join 
        qyxx_basic_v m2 
        on m1.relation_company_name = m2.company_name and m1.has_degree2 = 1
    '''
    sql_excute_newtable_tohdfs(sql=sql3, entry=entry, newtablename='business_type_tmp3')

    sql4 = '''
        select 
        a.corp_code,a.company_name,
        case when b.corp_code is not null then 1 else 0 end business_type
        from 
        target_company_v a 
        left join 
        (select 
        distinct corp_code
        from business_type_tmp3
        where (company_type_1 rlike '国有|国资|财政' and company_type_1 not rlike '非国有|非国资')
        or (company_type_2 rlike '国有|国资|财政' and company_type_2 not rlike '非国有|非国资')
        ) b 
        on a.corp_code = b.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql4, entry=entry, index_info='出口骗税--经济类型（内部数据）计算完成')


def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        is_cache=False,
        index_info: str = None
):
    entry.logger.info(f'*********************\n当前执行sql为:\n{sql}\n*********************')
    df: DataFrame = entry.spark.sql(sql)
    if index_info:
        entry.logger.info(f"*********************\n{index_info}\n********************")
    if is_cache:
        df.cache()
    if newtablename:
        df.createOrReplaceTempView(f'{newtablename}')
        entry.logger.info(f'*********************\n生成临时表名为:{newtablename}\n********************')

    return df
