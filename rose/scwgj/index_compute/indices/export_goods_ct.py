#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--出口商品计数
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
出口商品计数
【出口报关单明细变】出口商品大类（取出口商品代码前2位）不同计数
'''


@register(name='export_goods_ct',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_expf_v', 'target_company_v'])
def export_goods_ct(param: Parameter, entry: Entry):
    t_date_dict = param.get("tmp_date_dict")
    pboc_expf_end_date = t_date_dict.get("pboc_expf")

    sql = f"""
        select
            corp_code,
            count(distinct substring(merch_code,1,2)) as export_goods_ct
        from pboc_expf_v
        where exp_date between add_months('{pboc_expf_end_date}', -12) and date('{pboc_expf_end_date}')
        and corp_code is not null 
        and merch_code is not null 
        group by corp_code
    """
    sql_excute_newtable_tohdfs(sql=sql, entry=entry, newtablename='export_goods_ct_tmp')

    sql2 = '''
        select 
        a.corp_code, 
        a.company_name,  
        coalesce(export_goods_ct,0) as export_goods_ct
        from target_company_v a 
        left join 
        export_goods_ct_tmp b 
        on a.corp_code = b.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql2, entry=entry, index_info='出口骗税的出口商品计数计算完成')


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
