#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rmb_exp_cp_isneg_mark",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["pboc_corp_pay_jn_v",
                        "target_company_v",
                        "pboc_cncc_v"])
def rmb_exp_cp_isneg_mark(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 当月和后1个月人民币支出属于负面清单的企业进行标注，资金是否回流
    entry.spark.sql(
        f'''
        select corp_code, substring(cp_rcver_name, 5, 20) as cp_name 
          from pboc_corp_pay_jn_v
         where tx_code = '929070'
         and (cp_rcver_name like '%信托%' 
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
        ''').createOrReplaceTempView('part1')
    entry.spark.sql(
        '''
        select a.corp_code, b.CRDTORNAME
          from part1 a 
          join pboc_cncc_v b 
         on a.cp_name = b.DEBTORNAME
        ''').createOrReplaceTempView('part2')
    entry.spark.sql(
        '''
        select corp_code, cp_name 
          from part1
        union all 
        select corp_code, CRDTORNAME as cp_name
          from part2 
        ''').distinct().createOrReplaceTempView('tmp')

    sql = '''
        select a.bbd_company_name as company_name, a.corp_code, 
               case when count(distinct cp_name) > 0 then 1
                    else 0 end rmb_exp_cp_isneg_mark
          from target_company_v a  
          left join tmp b 
         on a.corp_code = b.corp_code
         group by a.bbd_company_name, a.corp_code
        '''


    logger.info(sql)
    df = entry.spark.sql(sql)

    return df


