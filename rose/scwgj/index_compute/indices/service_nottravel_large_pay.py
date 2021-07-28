#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouchao@bbdservice.com
:Date: 2021-06-23 14:44
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="service_nottravel_large_pay",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_corp_pay_jn_v"])
def service_nottravel_large_pay(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    # 服贸（非旅游）向周边涉赌国家大额付款
    sql = f'''
        select company.corp_code, company.bbd_company_name, service_nottravel_large_pay_value, 
        case when service_notravel_freq_pay_value is null then null
             when service_notravel_freq_pay_value > 1000 then 1 
            else 0
            end service_notravel_freq_pay 
        from target_company_v company
        left join
            (
            select tx_amt_usd as service_nottravel_large_pay_value, corp_code
              from pboc_corp_pay_jn_v
             where tx_code like '2%'
             and tx_code not like '223%'
             and rcver_country_code in ('SGP','VNM','KHM','MYS','LAO','MMR','PHL','MAC')
             and pay_date between add_months('{t_date}', -12)  
                                and date('{t_date}')
            ) pay
        on company.corp_code = pay.corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
