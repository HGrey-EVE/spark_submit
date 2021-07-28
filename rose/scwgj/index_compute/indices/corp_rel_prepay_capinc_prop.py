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


@register(name="corp_rel_prepay_capinc_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_corp_pay_jn_v",
                        "all_off_line_relations_v",
                        "all_company_v"])
def corp_rel_prepay_capinc_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_rcv_date = t_date_dict.get('pboc_corp_rcv_jn')
    # 【对公收入】资本金收入汇总 【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c23
          from (
                select corp_code, tx_amt_usd
                  from pboc_corp_rcv_jn_v
                 where tx_code in ('622011','622012','622013')
                 and rcv_date between add_months('{t_rcv_date}', -12) and date('{t_rcv_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公付款】预付货款交易金额(汇总) 【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c11 
          from (
                select corp_code, tx_amt_usd
                  from pboc_corp_pay_jn_v
                 where rcvpay_attr_code = 'A'
                 and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 预付金额与关联企业资本金收入金额比例
    sql = """
        select 
            company.corp_code 
            ,company.company_name 
            ,c23
            ,c11
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("pre_process_v")

    sql = """
          select a.corp_code, a.company_name, 
            case when a.c11 is null and b.tmp_col is null then null else coalesce(a.c11, 1)/coalesce(b.tmp_col, 1) end corp_rel_prepay_capinc_prop
          from pre_process_v a
          left join (
                select a.corp_code, sum(b.c23) as tmp_col
                  from (
                        select a.corp_code, b.corp_code as relation_corp_code
                          from all_offline_relations_v a 
                          join all_company_v b 
                         on a.company_rel_degree_1 = b.company_name
                      ) a 
                  join pre_process_v b 
                 on a.relation_corp_code = b.corp_code
                 group by a.corp_code
                )b
          on a.corp_code = b.corp_code
            """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df