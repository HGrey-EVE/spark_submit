#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-25 17:44
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="import_trade_prepay_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_imp_custom_rpt_v", "pboc_corp_pay_jn_v"])
def import_trade_prepay_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_imp_custom_rp')
    t_pay_date = t_date_dict.get('pboc_corp_pay_jn')
    # 【对公付款】预付货款交易金额(汇总) 【所有存量数据】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c9 
          from (
                select corp_code, tx_amt_usd
                  from pboc_corp_pay_jn_v
                 where rcvpay_attr_code = 'A'
                 and tx_code like '1%'
                 and pay_date between add_months('{t_pay_date}', -12) and date('{t_pay_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【进口报关单基本信息】成交总价(汇总) 【所有存量数据】
    sql = f'''
        select corp_code, sum(deal_amt_usd) as c10
          from (
                select corp_code, deal_amt_usd
                  from pboc_imp_custom_rpt_v
                  where pay_date between add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 预付货款与进口货物流比例
    sql = """
        select 
            company.corp_code 
            ,company.company_name 
            ,case when v1.c9 is null and v2.c10 is null then null 
            when v2.c10 = 0 then coalesce(v1.c9, 1)/1
            else coalesce(v1.c9, 1)/coalesce(v2.c10, 1) end import_trade_prepay_prop
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
