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


@register(name="import_exp_trade_prop_2y",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_imp_custom_rpt_v",
                        "pboc_corp_pay_jn_v"])
def import_exp_trade_prop_2y(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_imp_date = t_date_dict.get('pboc_imp_custom_rpt')
    # 【对公付款】货物贸易付款金额折美元（汇总）
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c5
          from (
                select corp_code, tx_amt_usd, rptno
                  from pboc_corp_pay_jn_v
                 where tx_code in ('121010', '121020', '121040', '121080', '121090', '121990', '121110', '121100')
                 and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【进口报关单基本信息表】成交总价（汇总）【指标计算当天所属年度与上一个自然年度】
    sql = f'''
        select corp_code, sum(deal_amt_usd) as c6 
          from (
                select corp_code, deal_amt_usd
                  from pboc_imp_custom_rpt_v 
                 where imp_date between add_months('{t_imp_date}', -12) and date('{t_imp_date}')
                 and (custom_trade_mode_code like '%10' 
                      or custom_trade_mode_code like '%15' 
                      or custom_trade_mode_code like '%33' 
                      or custom_trade_mode_code like '%34' 
                      or custom_trade_mode_code like '%19'
                      or custom_trade_mode_code like '%22' 
                      or custom_trade_mode_code like '%25' 
                      or custom_trade_mode_code like '%20' 
                      or custom_trade_mode_code like '%35' 
                      or custom_trade_mode_code like '%13'
                      or custom_trade_mode_code like '%16'
                      or custom_trade_mode_code like '%30'
                      or custom_trade_mode_code like '%39'
                      or custom_trade_mode_code like '%31'
                      or custom_trade_mode_code like '%41')
                and (custom_trade_mode_code != 1210
                    and custom_trade_mode_code != 9610
                    and custom_trade_mode_code != 1239)
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 货物贸易资金流与进口货物流比例
    sql = """
        select 
            company.corp_code 
            ,company.company_name 
            ,case when v1.c5 is null and v2.c6 is null then null 
            else coalesce(v1.c5, 1)/coalesce(v2.c6, 1) end import_exp_trade_prop_2y
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
