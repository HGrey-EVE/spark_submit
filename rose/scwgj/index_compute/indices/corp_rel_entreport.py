#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_rel_entreport",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["pboc_corp_rcv_jn_v",
                        "target_company_v",
                        "pboc_corp_pay_jn_v",
                        "all_off_line_relations_v",
                        "all_company_v"])
def corp_rel_entreport (entry: Entry, param: Parameter, logger):

    # 是否多个关联企业进行转口贸易交易
    entry.spark.sql(
        '''
        select a.corp_code as a_corp_code, b.corp_code as b_corp_code
          from (
                select corp_code, rpt_tel 
                  from pboc_corp_pay_jn_v 
                 where tx_code in ('122010','121030')
                )a 
          join (
                select corp_code, rpt_tel 
                  from pboc_corp_rcv_jn_v 
                 where tx_code in ('122010','121030')
                )b 
         on a.rpt_tel = b.rpt_tel
        ''').createOrReplaceTempView('part1')
    entry.spark.sql(
        '''
        select distinct corp_code
          from (
                select corp_code 
                  from pboc_corp_pay_jn_v 
                 where tx_code in ('122010','121030')
                union all 
                select corp_code 
                  from pboc_corp_rcv_jn_v 
                 where tx_code in ('122010','121030')
                )
        ''').createOrReplaceTempView('part2')
    entry.spark.sql(
        '''
        select b.relation_corp_code
          from part2 a 
          join (
                select a.corp_code, b.corp_code as relation_corp_code
                  from all_off_line_relations_v a 
                  join all_company_v b 
                 on a.company_rel_degree_1 = b.company_name
                ) b 
         on a.corp_code = b.corp_code
        ''').createOrReplaceTempView('part3')
    entry.spark.sql(
        '''
        select a_corp_code as corp_code 
          from part1
        union all 
        select b_corp_code as corp_code
          from part1
        union all 
        select relation_corp_code as corp_code
          from part3
        ''').distinct().createOrReplaceTempView('tmp')

    sql = '''
        select a.bbd_company_name as company_name, a.corp_code, 
               case when b.corp_code is not null then 1
                    else 0 end corp_rel_entreport
          from target_company_v a 
          left join tmp b 
         on a.corp_code = b.corp_code
        '''


    logger.info(sql)
    df = entry.spark.sql(sql)

    return df


