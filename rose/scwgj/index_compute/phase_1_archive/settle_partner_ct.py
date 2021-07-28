#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--结汇当日或次日支付对手次数

人民币数据暂时不看
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
1.【企业结汇】取人民币账户（剔除意愿结汇）
2.【人民币交易数据】以第1步人民币账户支出对手计数
【企业结汇】交易编码=‘121010’and 结汇用途代码!='022'
'''


@register(name='settle_partner_ct',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_rcv_jn_v'])
def settle_partner_ct(param: Parameter, entry: Entry):
    dict_date = param.get('tmp_date_dict')
    pboc_corp_rcv_jn_end_date = dict_date.get("pboc_corp_rcv_jn")

    sql = f'''
        select
        distinct corp_code
        from pboc_corp_lcy_v
        where deal_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
        and tx_code = '121010' 
        and salefx_use_code <> '022'
    '''
    sql_excute_newtable_tohdfs(sql=sql, entry=entry, newtablename='settle_partner_ct_tmp1')

    sql2 = '''
        from settle_partner_ct_tmp1 a 
        join pboc_cncc_beps_v b 
        on a.corp_code = b.
    '''


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