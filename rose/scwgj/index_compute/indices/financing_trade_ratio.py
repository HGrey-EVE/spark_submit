#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouchao@bbdservice.com
:Date: 2021-06-22 17:44
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="financing_trade_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_dfxloan_sign_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_exp_custom_rpt_v",
                        "pboc_imp_custom_rpt_v"
                        ])
def financing_trade_ratio(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_df_date = t_date_dict.get('pboc_dfxloan_sign')
    t_imp_date = t_date_dict.get('pboc_imp_custom_rpt')
    t_exp_date = t_date_dict.get('pboc_exp_custom_rpt')
    t_rcv_date = t_date_dict.get('pboc_corp_rcv_jn')
    # 贸易融资金额与贸易背景比例 境外贸易融资金额（折美元）
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c1
          from (
                select corp_code, tx_amt_usd, rptno
                  from pboc_corp_pay_jn_v
                 where tx_code = '822020'
                 and (regno = '' or regno = 'N/A')
                 and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 境内贸易融资金额（折美元）
    sql = f'''
           select corp_code, sum(sign_amt_usd) as c2
             from (
                   select corp_code, sign_amt_usd, rptno
                     from pboc_dfxloan_sign_v
                    where domfx_loan_type_code = '1102'
                    and interest_start_date between add_months('{t_imp_date}', -12) and date('{t_df_date}')
                   ) 
            group by corp_code
           '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 进口成交总价
    sql = f'''
               select corp_code, sum(deal_amt) as c3
                 from (
                       select corp_code, deal_amt
                         from pboc_imp_custom_rpt_v
                        where imp_date between add_months('{t_imp_date}', -12) and date('{t_imp_date}')
                       ) 
                group by corp_code
               '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view3")
    # 出口成交总价
    sql = f'''
                   select corp_code, sum(deal_amt) as c4
                     from (
                           select corp_code, deal_amt
                             from pboc_exp_custom_rpt_v
                            where exp_date between add_months('{t_exp_date}', -12) and date('{t_exp_date}')
                           ) 
                    group by corp_code
                   '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view4")
    # 【对公付款】离岸转口买卖
    sql = f'''
                       select corp_code, sum(tx_amt_usd) as c5
                         from (
                               select corp_code, tx_amt_usd
                                 from pboc_corp_pay_jn_v
                                where pay_date between add_months('{t_date}', -12) and date('{t_date}')
                                and tx_code = '122020'
                               ) 
                        group by corp_code
                       '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view5")
    # 【对公收入】离岸转口买卖
    sql = f'''
                           select corp_code, sum(tx_amt_usd) as c6
                             from (
                                   select corp_code, tx_amt_usd
                                     from pboc_corp_rcv_jn_v
                                    where rcv_date between add_months('{t_rcv_date}', -12) and date('{t_rcv_date}')
                                    and tx_code = '122020'
                                   ) 
                            group by corp_code
                           '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view6")
    # 贸易融资金额与贸易背景比例
    sql = """
        select 
            company.corp_code 
            ,company.company_name 
            ,case when v1.c1 is null and v2.c2 is null 
                       and v3.c3 is null and v4.c4 is null 
                       and v5.c5 is null and v6.c6 is null then null 
            else coalesce(v1.c1, 1) + coalesce(v2.c2, 1)/coalesce(v3.c3, 1) + 
                 coalesce(v4.c4, 1) + coalesce(v5.c5, 1) + coalesce(v6.c6, 1)
            end financing_trade_ratio
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        left join view3 v3 on v1.corp_code = company.corp_code
        left join view4 v4 on v2.corp_code = company.corp_code
        left join view5 v5 on v1.corp_code = company.corp_code
        left join view6 v6 on v2.corp_code = company.corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
