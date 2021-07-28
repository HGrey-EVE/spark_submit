#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--出口收汇金额变异系数（按月）
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
1、【对公收入】货物贸易收入金额按月汇总
2、基于12个月的月度汇总值，计算变异系数
【对公收入】货物贸易收入金额按月汇总
【对公收入】货物贸易项下 交易编码=’121010‘
'''


@register(name='corp_inc_amt_varicoef',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_rcv_jn_v', 'target_company_v'])
def corp_inc_amt_varicoef(param: Parameter, entry: Entry):
    t_date_dict = param.get('tmp_date_dict')
    pboc_corp_rcv_jn_end_date = t_date_dict.get('pboc_corp_rcv_jn')

    # 出口收汇笔数变异系数（按月） 分子标准差 【指标计算当天过去一年】
    sql1 = f"""
       select corp_code, 
           var_pop(salefx_amt_usd) as c39
       from (
           select corp_code, 
               rcv_date_tmp, 
               sum(salefx_amt_usd) as salefx_amt_usd
           from (
               select corp_code, 
                   salefx_amt_usd, 
                   substring(rcv_date, 1, 7) as rcv_date_tmp
               from pboc_corp_rcv_jn_v 
               where tx_code in ('121010', '121020', '121040', '121080', '121090', '121990')
               and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
           )
           group by corp_code, rcv_date_tmp
       )
       group by corp_code
    """
    sql_excute_newtable_tohdfs(sql=sql1, newtablename='corp_inc_amt_varicoef_tmp1')

    # 出口收汇笔数变异系数（按月） 分母平均值 【指标计算当天过去一年】
    sql2 = f"""
       select corp_code, 
           avg(salefx_amt_usd) as c40
       from (
           select corp_code, 
               rcv_date_tmp, 
               sum(salefx_amt_usd) as salefx_amt_usd
           from (
               select corp_code, 
                   salefx_amt_usd, 
                   substring(rcv_date, 1, 7) as rcv_date_tmp
               from pboc_corp_rcv_jn_v 
               where tx_code in ('121010', '121020', '121040', '121080', '121090', '121990')
               and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
           )
           group by corp_code, rcv_date_tmp
       )
       group by corp_code
    """
    sql_excute_newtable_tohdfs(sql=sql2, newtablename='corp_inc_amt_varicoef_tmp2')

    sql3 = """
       select company.corp_code,
              company.company_name,
           case when v1.c39 is null and v2.c40 is null then null
                else coalesce(v1.c39, 1)/coalesce(v2.c40, 1) 
           end corp_inc_amt_varicoef
       from target_company_v company
       left join corp_inc_amt_varicoef_tmp1 v1  
       on company.corp_code = v1.corp_code
       left join corp_inc_amt_varicoef_tmp2 v2 
       on company.corp_code = v2.corp_code
     """
    return sql_excute_newtable_tohdfs(sql=sql3, index_info='出口收汇金额变异系数（按月）计算完成', entry=entry)


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
