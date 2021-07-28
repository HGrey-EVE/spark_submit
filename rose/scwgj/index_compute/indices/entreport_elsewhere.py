#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 转口贸易--异地办理转口贸易收支比例

Date: 2021/6/28 10:01
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
（【对公收入】转口贸易收入【企业所属外汇局代码】与经办银行【所属外汇局代码】不同（用前两位外汇局代码比较）的笔数+
【对公付款】转口贸易支出企业所属外汇局代码与经办银行所属外汇局代码不同（用前两位外汇局代码比较）的笔数）/
（【对公收入】转口贸易收入笔数+【对公付款】转口贸易支出笔数）
'''


@register(name='entreport_elsewhere',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_pay_jn_v', 'pboc_corp_rcv_jn_v',
                        'target_company_v'])
def entreport_elsewhere(param: Parameter, entry: Entry):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    sql = f'''
        select
         corp_code,
         count(distinct case when substring (safecode,1,2) <> substring(bank_code,1,2) then rptno end) as num1,
         count(distinct rptno) all_1
        from pboc_corp_rcv_jn_v
        where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
            and tx_code in ('122010','121030')
            group by corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql, newtablename='entreport_elsewhere_tmp1', entry=entry)

    sql2 = f'''
        select
         corp_code,
         count(distinct substring (safecode,1,2) <> substring(bank_code,1,2) then rptno end) as num2,
         count(distinct rptno) as all_2
        from pboc_corp_pay_jn_v
        where pay_date between add_months('{pboc_corp_pay_jn_end_date}',-12) and date('{pboc_corp_pay_jn_end_date}')
            and tx_code in ('122010','121030')
            group by corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql2, newtablename='entreport_elsewhere_tmp2', entry=entry)

    sql3 = f'''
        select
        a.corp_code,
        a.company_name,
        case when coalesce (b.num1,0) = 0 and coalesce (c.num2,0) = 0  then 0 
            else (b.num1+c.num2)/(b.all_1+c.all_2) 
            end entreport_elsewhere
        from target_company_v a 
        left join 
        entreport_elsewhere_tmp1 b 
        on a.corp_code = b.corp_code
        left join 
        entreport_elsewhere_tmp2 c 
        on a.corp_code = c.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql3, index_info='转口贸易--异地办理转口贸易收支比例已经计算完成', entry=entry)


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
