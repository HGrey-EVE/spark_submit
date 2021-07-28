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

'''
1、【对公收入】交易编码=‘一般贸易收汇’（代码）金额汇总
2、企业境内二度及以内关联方
3、【对公付款】企业自身付款总额+【对公付款】上述关联方付款总额
4、第一步结果/第三步结果
'''


@register(name='collect_relpay_ratio',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_rcv_jn_v', 'target_company_v', 'pboc_corp_pay_jn_v',
                        'all_off_line_relations_degree2_v', 'all_company_v'])
def collect_relpay_ratio(param: Parameter, entry: Entry):
    dict_date = param.get('tmp_date_dict')
    pboc_corp_rcv_jn_end_date = dict_date.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = dict_date.get("pboc_corp_pay_jn")

    # 【对公收入】交易编码=‘一般贸易收汇’（代码）金额汇总
    sql = f'''
        select 
        a.corp_code,
        sum(a.tx_amt_usd) as tx_amt_usd_1        
        from pboc_corp_rcv_jn_v a 
        join target_company_v b 
        on a.corp_code = b.corp_code
        and a.tx_code = '121010'
        and a.rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
        group by a.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql, entry=entry, newtablename='collect_relpay_ratio_tmp1')

    # 企业自身对公付款的总额
    sql2 = f'''
        select 
        a.corp_code,
        sum(tx_amt_usd) as tx_amt_usd_2
        from pboc_corp_pay_jn_v a 
        join target_company_v b 
        on a.corp_code = b.corp_code
        and a.pay_date between add_months('{pboc_corp_pay_jn_end_date}',-12) and date('{pboc_corp_pay_jn_end_date}')
        group by a.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql2, entry=entry, newtablename='collect_relpay_ratio_tmp2')

    # 企业境内二度及以内关联方的对公付款的付款总额
    sql3 = f'''
    select 
        m1.corp_code,
        sum(m2.tx_amt_usd) as tx_amt_usd_3
    from 
        (select 
            distinct a.corp_code, 
            b.corp_code as relation_corp_code
        from all_off_line_relations_degree2_v a 
        join all_company_v b 
        on a.company_rel_degree_2 = b.company_name
        ) m1 
        join 
        pboc_corp_pay_jn_v m2 
        on m1.relation_corp_code = m2.corp_code
        and m2.pay_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
        group by m1.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql3, entry=entry, newtablename='collect_relpay_ratio_tmp3')

    sql4 = '''
        select
         a.corp_code,a.company_name,tx_amt_usd_1/all_num as collect_relpay_ratio
        from 
            (select 
                a.corp_code,
                a.company_name,
                coalesce(b.tx_amt_usd_1,0) as tx_amt_usd_1,
                case when(coalesce(c.tx_amt_usd_2,0)+coalesce(d.tx_amt_usd_3,0)) = 0 then 1 
                    else (coalesce(c.tx_amt_usd_2,0)+coalesce(d.tx_amt_usd_3,0)) end all_num
            from target_company_v a 
            left join 
            collect_relpay_ratio_tmp1 b on a.corp_code = b.corp_code
            left join 
            collect_relpay_ratio_tmp2 c on a.corp_code = c.corp_code
            left join 
            collect_relpay_ratio_tmp3 d on a.corp_code = d.corp_code
            ) a 
    '''

    return sql_excute_newtable_tohdfs(sql=sql4, entry=entry, index_info='一般贸易收汇（企业本身）与境内关联方（一二度关联、国外合作伙伴、）付汇金额比例计算完成')


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
