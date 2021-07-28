#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 

Date: 2021/6/29 9:16
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
【收支余】贷记和借记字段（折美元）
- 如果当天有借贷进出，且一致（绝对值<=1000），记为有一天风险；
- 如果当天借或贷只有一项，或借贷不一致，但当天和当天+1的借贷累计值一致（绝对值<=1000），记为有2天风险；
- 都不符合，记为无风险。
有风险的天数/收支余内当年的总计数（即发生交易天数）,
'''


@register(name='equal_in_out_ratio',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['t_acct_rpb_v', 'target_company_v'])
def equal_in_out_ratio(param: Parameter, entry: Entry):
    t_date_dict = param.get("tmp_date_dict")
    t_acct_rpb_end_date = t_date_dict.get("t_acct_rpb")

    # 有一天风险的
    sql1 = f'''
        select
        corp_code,deal_date,credit_amt_usd,debit_amt_usd,
        case when credit_amt_usd > 0 and debit_amt_usd > 0 and abs(credit_amt_usd - debit_amt_usd) <= 1000 
        then 1 end risk_type
        from 
        (select 
            corp_code,
            deal_date,
            sum(abs(coalesce(credit_amt_usd,0))) as credit_amt_usd,
            sum(abs(coalesce(debit_amt_usd,0))) as debit_amt_usd
        from t_acct_rpb_v 
        where deal_date between add_months('{t_acct_rpb_end_date}',-12) and date('{t_acct_rpb_end_date}')
        group by corp_code,deal_date
        ) a 
    '''

    sql_excute_newtable_tohdfs(sql=sql1, entry=entry, newtablename='equal_in_out_ratio_tpm1')

    # 计算两天风险的(不剔除掉一天风险的),先把所有连着的两天的情况都列出来，并且满足两天累计起来绝对值<=1000
    sql2 = '''
        select
        a.corp_code,a.deal_date as deal_date_1,b.deal_date as deal_date_2,
        a.credit_amt_usd as credit_amt_usd_1,a.debit_amt_usd as debit_amt_usd_1,
        b.credit_amt_usd as credit_amt_usd_2,b.debit_amt_usd as debit_amt_usd_2
        from 
        (select * from equal_in_out_ratio_tpm1 where risk_type <> 1) a 
        inner join 
        equal_in_out_ratio_tpm1 b 
        on a.corp_code = b.corp_code
        and datediff(a.deal_date,b.deal_date) = 1
        and abs(a.credit_amt_usd+b.credit_amt_usd-a.debit_amt_usd-b.debit_amt_usd) <= 1000 
    '''
    sql_excute_newtable_tohdfs(sql=sql2, entry=entry, newtablename='equal_in_out_ratio_tpm2')

    # 把已经计算为两天风险的date和date+1，date又作为上一个两天风险的date+1，这种情况的记录给排除了。
    sql3 = '''
        select
        a.corp_code,a.deal_date_1,a.deal_date_2
        from equal_in_out_ratio_tpm2 a
        where 
        not exists(
        select 1 from equal_in_out_ratio_tpm2 b 
        where a.corp_code = b.corp_code 
        and a.deal_date_1 = b.deal_date_2
        )
    '''
    sql_excute_newtable_tohdfs(sql=sql3, entry=entry, newtablename='equal_in_out_ratio_tpm3')

    # 结果统计
    sql4 = '''
        select  
        m1.corp_code,m1.company_name,
        coalesce (m2.equal_in_out_ratio,0) as equal_in_out_ratio
        from 
        target_company_v m1
        left join 
        (select 
        corp_code,
        count(distinct deal_date) as equal_in_out_ratio
        from 
            (select 
            corp_code,deal_date_1 as deal_date
            from equal_in_out_ratio_tpm3
            union all 
            select 
            corp_code,deal_date_2 as deal_date
            from equal_in_out_ratio_tpm3
            union all 
            select 
            corp_code,deal_date
            from equal_in_out_ratio_tpm1
            where risk_type = 1
            ) a 
            group by corp_code
        ) m2 
        on m1.corp_code = m2.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql4, entry=entry, index_info='出口骗税--收汇当日或次日账户贷借相等天数占比计算完成')


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
