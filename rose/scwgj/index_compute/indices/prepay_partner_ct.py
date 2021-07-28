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


@register(name="prepay_partner_ct",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_corp_pay_jn_v"])
def prepay_partner_ct(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    # 预付交易对手不同计数
    sql = f'''
        select company.corp_code, company.company_name, prepay_partner_ct
        from target_company_v company
        left join 
            (select corp_code, count(distinct cp_rcver_name) as prepay_partner_ct 
              from (
                    select corp_code, cp_rcver_name
                      from pboc_corp_pay_jn_v
                     where rcvpay_attr_code = 'A'
                     and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                    )
             group by corp_code) rever
        on company.corp_code = rever.corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
