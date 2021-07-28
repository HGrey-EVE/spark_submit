#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--出口收汇金额（一般贸易）
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
【对公收入】出口收汇金额汇总（折美元）
【对公收入】货物贸易项下 交易编码=’121010‘
'''


@register(name='corp_inc_amt',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_rcv_jn_v', 'target_company_v'])
def corp_inc_amt(param: Parameter, entry: Entry):
    t_date_dict = param.get('tmp_date_dict')
    pboc_corp_rcv_jn_end_date = t_date_dict.get('pboc_corp_rcv_jn')

    sql = f'''
        select 
        corp_code,
        sum(tx_amt_usd) as corp_inc_amt
        from pboc_corp_rcv_jn_v
        where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
        and tx_code = '121010'
        group by corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql, newtablename='corp_inc_amt_tmp', entry=entry)

    sql2 = '''
        select 
        a.corp_code, 
        a.company_name,  
        coalesce(corp_inc_amt,0) as corp_inc_amt
        from target_company_v a 
        left join 
        corp_inc_amt_tmp b 
        on a.corp_code = b.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql2, entry=entry, index_info='出口骗税--出口收汇金额（一般贸易）计算完成')


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
