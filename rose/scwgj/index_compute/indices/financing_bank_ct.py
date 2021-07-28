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


@register(name="financing_bank_ct",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_dfxloan_sign_v"
                        ])
def financing_bank_ct(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_df_date = t_date_dict.get('pboc_dfxloan_sign')
    # 贸易融资经办银行计数
    sql = f'''
        select  company.corp_code, company.company_name, financing_bank_ct
        from target_company_v company
        left join
            (select corp_code, count(distinct bank_attr_code) as financing_bank_ct
              from (
                    select corp_code, bank_attr_code
                      from pboc_dfxloan_sign_v
                     where domfx_loan_type_code = '1102'
                     and interest_start_date between add_months('{t_df_date}', -12) and date('{t_df_date}')
                    )
             group by corp_code) bank
        on company.corp_code = bank.corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)

    return df
