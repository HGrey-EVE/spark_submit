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


@register(name="yf_rptamt_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_corp_pay_jn_v"])
def yf_rptamt_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    # 【对公付款】预付交易笔数 【指标计算当天所属年度与上一个自然年度】
    sql = f'''
        select corp_code, count(distinct rptno) as c15
          from (
                select corp_code, rptno
                  from pboc_corp_pay_jn_v 
                 where rcvpay_attr_code = 'A'
                 and tx_code like '1%'
                 and pay_date add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公付款】货物贸易交易笔数【指标计算当天所属年度与上一个自然年度】
    sql = f'''
        select corp_code, count(distinct rptno) as c36
          from (
                select corp_code, tx_amt_usd, rptno
                  from pboc_corp_pay_jn_v
                 where tx_code in ('121010','121020','121040','121080','121090','121990'，'121110','121100')
                 and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 预付货款笔数占货物贸易付款笔数的比例
    sql = """
        select 
            company.corp_code 
            ,company.company_name 
            ,case when v1.c15 is null and v2.c36 is null then null else coalesce(v1.c15, 1)/coalesce(v2.c36, 1) end yf_rptamt_prop
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df