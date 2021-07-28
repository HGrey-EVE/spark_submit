#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--境内多个企业仅与同一境外交易对手发生货物贸易收入
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
1、【对公收入】结算方式=’电汇‘的交易，所有境外交易对手的境内交易企业（去重）计数>阈值（算黑样本的分布）的交易详细信息。
2、【对公收入】判断目标企业是否在1中，且计算目标企业与1中的境外交易对手的贸易收入/目标企业的所有贸易收入>阈值（算黑样本的分布），标记目标企业
'''


@register(name='same_foreign_partner',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_rcv_jn_v', 'target_company_v'])
def same_foreign_partner(param: Parameter, entry: Entry):
    dict_date = param.get('tmp_date_dict')
    pboc_corp_rcv_jn_v_end_date = dict_date.get('pboc_corp_rcv_jn')
    # 境外交易对手的境内交易企业（去重）计数>阈值
    num1 = 10
    # 目标企业与1中的境外交易对手的贸易收入/目标企业的所有贸易收入>阈值
    num2 = 0.5

    # 对公收入境外对手的交易明细
    sql = f'''
        select 
        substring(a.cp_payer_name,1,15) as cp_payer_name,
        a.corp_code,
        a.tx_amt_usd
        from pboc_corp_rcv_jn_v a 
        where a.rcv_date between add_months('{pboc_corp_rcv_jn_v_end_date}',-12) and date('{pboc_corp_rcv_jn_v_end_date}')
        and a.settle_method_code = 'T'
    '''
    sql_excute_newtable_tohdfs(sql=sql, entry=entry, newtablename='same_foreign_partner_tmp1')

    # 所有境外交易对手的境内交易企业（去重）计数>阈值的境外企业
    sql1 = f'''
        select 
        cp_payer_name,deal_nums
        from 
        (select 
        cp_payer_name,
        count(distinct corp_code) as deal_nums
        from same_foreign_partner_tmp1 
        group by cp_payer_name
        ) a 
        where deal_nums > {num1}
    '''
    sql_excute_newtable_tohdfs(sql=sql1, entry=entry, newtablename='same_foreign_partner_tmp1_1')

    # 目标企业的全部境外企业的明细(一年内的记录)
    sql2 = f'''
        select 
        a.corp_code,
        substring(b.cp_payer_name,1,15) as cp_payer_name,
        b.tx_amt_usd
        from target_company_v a 
        join 
        pboc_corp_rcv_jn_v b 
        on a.corp_code = b.corp_code
        and b.rcv_date between add_months('{pboc_corp_rcv_jn_v_end_date}',-12) and date('{pboc_corp_rcv_jn_v_end_date}')
    '''
    sql_excute_newtable_tohdfs(sql=sql2, entry=entry, newtablename='same_foreign_partner_tmp2')

    # 目标企业和境外企业的对公收入总额
    sql3 = '''
        select 
        a.corp_code,
        sum(a.tx_amt_usd) as tx_1
        from same_foreign_partner_tmp2 a 
        join same_foreign_partner_tmp1_1 b 
        on a.cp_payer_name = b.cp_payer_name
        group by a.corp_code
    '''

    sql_excute_newtable_tohdfs(sql=sql3, entry=entry, newtablename='same_foreign_partner_tmp3')

    # 目标企业的所有对公收入总额
    sql4 = '''
        select 
        corp_code,
        sum(tx_amt_usd) as tx_2
        from same_foreign_partner_tmp2
        group by corp_code
    '''

    sql_excute_newtable_tohdfs(sql=sql4, entry=entry, newtablename='same_foreign_partner_tmp4')

    # 【对公收入】判断目标企业是否在1中，且计算目标企业与1中的境外交易对手的贸易收入/目标企业的所有贸易收入>阈值（算黑样本的分布），标记目标企业
    sql5 = f'''
        select 
            a.corp_code,
            a.company_name,
            coalesce(b.tx_1,0)/coalesce(c.tx_2,1) as same_foreign_partner_value,
            case when coalesce(b.tx_1,0)/coalesce(c.tx_2,1) > {num2} then 1 else 0 end same_foreign_partner
        from target_company_v a 
        left join 
        same_foreign_partner_tmp3 b 
        on a.corp_code = b.corp_code
        left join 
        same_foreign_partner_tmp4 c 
        on a.corp_code = c.corp_code
    '''

    return sql_excute_newtable_tohdfs(sql=sql5, entry=entry, index_info='出口骗税--境内多个企业仅与同一境外交易对手发生货物贸易收入计算完毕')


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
