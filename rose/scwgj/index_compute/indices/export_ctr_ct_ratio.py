#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--出口抵运国数量与收汇交易对手国别数量的比例
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
【出口报关单基本信息】抵运国不同计数/
【对公收入】交易对方国别不同计数
'''


@register(name='export_ctr_ct_ratio',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_exp_custom_rpt_v', 'pboc_corp_rcv_jn_v', 'target_company_v'])
def export_ctr_ct_ratio(param: Parameter, entry: Entry):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_exp_custom_rpt_end_date = t_date_dict.get("pboc_exp_custom_rpt")

    sql = f"""
        select
            a.corp_code,
            a.num1/b.num2 as export_ctr_ct_ratio
        from 
            (select 
                corp_code,
                count(distinct trade_country_code) num1
            from pboc_exp_custom_rpt_v
            where exp_date between add_months('{pboc_exp_custom_rpt_end_date}',-12) and date('{pboc_exp_custom_rpt_end_date}')
                and corp_code is not null 
                and trade_country_code is not null
                group by corp_code
            ) a
            join 
            (select 
                corp_code,
                count(distinct payer_country_code) as num2
            from pboc_corp_rcv_jn_v
            where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
                and corp_code is not null
                and payer_country_code is not null
                group by corp_code
            ) b
            on a.corp_code = b.corp_code
    """
    sql_excute_newtable_tohdfs(sql=sql, entry=entry, newtablename='export_ctr_ct_ratio_tmp')

    sql2 = '''
        select 
        a.corp_code, 
        a.company_name,  
        coalesce(b.export_ctr_ct_ratio,0) as export_ctr_ct_ratio
        from target_company_v a 
        left join 
        export_ctr_ct_ratio_tmp b 
        on a.corp_code = b.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql2, entry=entry, index_info='出口抵运国数量与收汇交易对手国别数量的比例执行完成')


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
