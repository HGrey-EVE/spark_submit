#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouchao@bbdservice.com
:Date: 2021-06-24 17:44
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="prepay_elsewhere_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_corp_pay_jn_v"])
def prepay_elsewhere_ratio(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    # 【对公付款】预付交易金额 【指标计算当天所属年度与上一个自然年度】
    sql = f'''
            select corp_code, rptno, safecode, bank_safecode 
              from pboc_corp_pay_jn_v 
             where rcvpay_attr_code = 'A'
             and pay_date between add_months('{t_date}', -12) and date('{t_date}')
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公付款】货物贸易交易金额（汇总）【指标计算当天所属年度与上一个自然年度】
    sql = '''
        select company.corp_code ,company.bbd_company_name,  
            case when v1.c1 is null and v2.c2 is null then null 
            when v2.c2 = 0 then coalesce(v1.c1, 1)/1
            else coalesce(v1.c1, 1)/coalesce(v2.c2, 1) end prepay_elsewhere_ratio
            from target_company_v company
            left join 
           (select corp_code, count(*) as c2
           from view1
           group by corp_code) v1 on v1.corp_code = company.corp_code
           left join 
           (select corp_code, count(*) as c1
           from view1 
           where substring(safecode, 1, 2) != substring(bank_safecode, 1, 2)
           group by corp_code) v2 v2.corp_code = company.corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
