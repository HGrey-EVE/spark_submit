#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_entreport_cp_isneg",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["black_list_v",
                        "negative_list_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_corp_pay_jn_v",
                        "target_company_v"])
def corp_entreport_cp_isneg(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    # 转口贸易境外交易对手是否为负面清单主体的境外交易对手
    entry.spark.sql(
        '''
        select distinct cp_payer_name
          from (
                select distinct corp_code
                  from (
                        select corp_code
                          from black_list_v
                        union all
                        select corp_code
                          from negative_list_v
                        )
                )a
          join (
                select distinct corp_code, substring(cp_name, 5, 20) as cp_payer_name
                  from (
                        select corp_code, cp_payer_name as cp_name
                          from pboc_corp_rcv_jn_v
                        union all
                        select corp_code, cp_rcver_name as cp_name
                          from pboc_corp_pay_jn_v
                        )
                )b
         on a.corp_code = b.corp_code
        ''').createOrReplaceTempView('tmp_v')
    entry.spark.sql(
        f'''
        select corp_code, substring(cp_payer_name, 5, 20) as cp_name
          from pboc_corp_rcv_jn_v
         where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
        union all
        select corp_code, substring(cp_rcver_name, 5, 20) as cp_name
          from pboc_corp_pay_jn_v
         where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
        ''').createOrReplaceTempView('tmp_1_v')

    sql = '''
        select a.bbd_company_name as company_name, a.corp_code, 
               case when b.black_corp_code is not null then 1
                    else 0 end corp_entreport_cp_isneg
          from target_company_v a 
          left join (
                    select distinct b.corp_code as black_corp_code  
                      from tmp_v a 
                      join (
                            select corp_code, cp_name
                              from tmp_1_v
                            )b 
                     on a.cp_payer_name = b.cp_name
                    )b 
         on a.corp_code = b.black_corp_code
        '''

    logger.info(sql)
    df = entry.spark.sql(sql)

    return df


