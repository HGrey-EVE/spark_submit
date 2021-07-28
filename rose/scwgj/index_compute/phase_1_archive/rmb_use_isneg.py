#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rmb_use_isneg",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["pboc_corp_pay_jn_v",
                        "target_company_v"])
def rmb_use_isneg(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    entry.spark.sql(
        f'''
        select a.bbd_company_name as company_name, a.corp_code, substring(cp_rcver_name, 5, 20) as cp_name 
          from target_company_v a 
          join (
                select corp_code, cp_rcver_name
                  from pboc_corp_pay_jn_v
                 where tx_code = '929070'
                 and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                )b 
         on a.corp_code = b.corp_code
        ''') \
        .createOrReplaceTempView('part_1_v')

    entry.spark.sql(
        f'''
        select corp_code, substring(cp_rcver_name, 5, 20) as cp_name 
          from pboc_corp_pay_jn_v
        where (cp_rcver_name like '%信托%' 
              or cp_rcver_name like '%证券%' 
              or cp_rcver_name like '%保险%' 
              or cp_rcver_name like '%股票%' 
              or cp_rcver_name like '%房%'
              or tx_rem like '%信托%' 
              or tx_rem like '%证券%' 
              or tx_rem like '%保险%' 
              or tx_rem like '%股票%' 
              or tx_rem like '%房%'
             )
         and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
        ''') \
        .createOrReplaceTempView('part_2_v')

    sql = '''
            select company_name, corp_code,
              case when rmb_use_isneg > 1 then 1 else 0 end as rmb_use_isneg
              from (
                 select a.company_name, a.corp_code,
                     count(b.corp_code) as rmb_use_isneg
                     from part_1_v a
                     join part_2_v b
                       on a.cp_name = b.cp_name
                     group by a.company_name, a.corp_code
              )
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df


