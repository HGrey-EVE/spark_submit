#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--出口商品退税率
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
商品成交总价（折美元）最高的商品（商品编码看全位）的出口商品退税率

商品退税率数据需要导入
定义表名为 commodity_tax_rebate_rate_v
字段 
终止日期（end_date） 起始日期(start_date) 退税率(tax_rebate_rate) 
海关商品码(goods_code)
'''


@register(name='export_goods_taxrate',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_expf_v', 'target_company_v',
                        "commodity_tax_rebate_rate_v"])
def export_goods_taxrate(param: Parameter, entry: Entry):
    t_date_dict = param.get("tmp_date_dict")
    pboc_expf_end_date = t_date_dict.get("pboc_expf")
    commodity_tax_rebate_rate_end_date = t_date_dict.get("commodity_tax_rebate_rate")

    sql1 = f'''
        select 
        corp_code,merch_code
        from 
        (select
        corp_code,
        merch_code,
        row_number() over(partition by corp_code order by prod_deal_amt_usd desc) as row
        from pboc_expf_v a 
        where exp_date between add_months('{pboc_expf_end_date}', -12) and date('{pboc_expf_end_date}')
        ) a 
        where row = 1
    '''

    sql_excute_newtable_tohdfs(sql=sql1, entry=entry, newtablename='export_goods_taxrate_tmp1')

    sql2 = f'''
        select 
        a.corp_code,a.company_name,
        b.tax_rebate_rate as export_goods_taxrate
        from 
        target_company_v a 
        left join 
        (select 
        a.corp_code,b.tax_rebate_rate
        from export_goods_taxrate_tmp1 a  
        join 
        commodity_tax_rebate_rate_v b 
        on a.merch_code = b.goods_code
        and b.start_date <= add_months('{commodity_tax_rebate_rate_end_date}', -12)  
        and end_date >= date('{commodity_tax_rebate_rate_end_date}')
        ) b 
        on a.corp_code = b.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql2, entry=entry, index_info='出口骗税--出口商品退税率计算完成')


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
