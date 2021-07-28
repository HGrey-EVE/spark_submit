#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rmb_exp_pay_circle",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["pboc_corp_pay_jn_v",
                        "target_company_v",
                        "pboc_cncc_v"])
def rmb_exp_pay_circle(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    exchange_rate = 1.0 / 0.14316802

    entry.spark.sql(
        f'''
        select a.bbd_company_name as company_name, a.corp_code,
               b.new_cp_rcver_name, sum(tx_amt_usd * {exchange_rate}) as tx_amt_usd_sum
          from target_company_v a 
          join (
                select corp_code, substring(cp_rcver_name, 5, 20) as new_cp_rcver_name, tx_amt_usd
                  from pboc_corp_pay_jn_v
                 where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                 group by corp_code, new_cp_rcver_name, tx_amt_usd
                )b 
         on a.corp_code = b.corp_code
         group by a.bbd_company_name, a.corp_code, b.new_cp_rcver_name
        ''').createOrReplaceTempView('part_1_v')


    sql = '''
        select company_name, corp_code,
               case when tmp_prop > 0.7 and tmp_prop < 1.3 then 1
                    else 0 end rmb_exp_pay_circle
          from (
                select a.company_name, a.corp_code,
                       a.new_cp_rcver_name / b.AMT_sum as tmp_prop
                  from part_1_v a 
                  join (
                        select CRDTORNAME, sum(AMT) as AMT_sum
                          from pboc_cncc_v 
                         group by CRDTORNAME
                        )b 
                 on a.new_cp_rcver_name = b.CRDTORNAME
                )
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)

    return df





