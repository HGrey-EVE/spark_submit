#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--出口骗税--出口金额与境内关联方进口金额比例
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
【出口报关单基本情况表】出口成交总价（折美元）/
【进口报关单进本情况表】二度及二度以内关联方进口成交总价（折美元）
'''


@register(name='export_rel_import_ratio',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_exp_custom_rpt_v', 'target_company_v',
                        'all_off_line_relations_degree2_v', 'all_company_v', 'pboc_imp_custom_rpt_v'])
def export_rel_import_ratio(param: Parameter, entry: Entry):
    t_date_dict = param.get("tmp_date_dict")
    pboc_exp_custom_rpt_end_date = t_date_dict.get("pboc_exp_custom_rpt")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")

    # 计算出【出口报关单基本情况表】出口成交总价（折美元）
    sql = f"""
        select 
            a.corp_code,
            sum(a.deal_amt_usd) as deal_amt_1
        from pboc_exp_custom_rpt_v a 
        join target_company_v b 
        on a.corp_code = b.corp_code
        and a.exp_date between add_months('{pboc_exp_custom_rpt_end_date}', -12) and date('{pboc_exp_custom_rpt_end_date}')
        and a.custom_trade_mode_code like '%10' 
        and a.custom_trade_mode_code not in ('1210','9610')
        and a.corp_code is not null 
        group by a.corp_code 
    """
    sql_excute_newtable_tohdfs(sql=sql, newtablename='export_rel_import_ratio_tmp1', entry=entry)

    # 二度关联方的进口成交总价
    sql2 = f'''
        select 
            n.corp_code,
            sum(m.deal_amt_usd) as deal_amt_2
        from pboc_imp_custom_rpt_v m 
        join 
        (select distinct a.corp_code, 
        b.corp_code as relation_corp_code
        from all_off_line_relations_degree2_v a 
        join all_company_v b 
        on a.company_rel_degree_2 = b.company_name
        ) n 
        on m.corp_code = n.relation_corp_code
        and m.imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12) and date('{pboc_imp_custom_rpt_end_date}')
        group by n.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql, newtablename='export_rel_import_ratio_tmp2', entry=entry)

    # 计算比例
    sql3 = f'''
        select 
        m1.corp_code,
        m1.company_name,
        coalesce(m2.deal_amt_1,0)/coalesce(m3.deal_amt_2,1) as export_rel_import_ratio
        from 
        target_company_v m1
        left join 
        export_rel_import_ratio_tmp1 m2 
        on m1.corp_code = m2.corp_code
        left join 
        export_rel_import_ratio_tmp2 m3 
        on m1.corp_code = m3.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql3, entry=entry, index_info='出口骗税--出口金额与境内关联方进口金额比例计算完成')


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
