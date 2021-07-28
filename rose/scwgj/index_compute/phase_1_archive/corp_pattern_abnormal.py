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


@register(name="corp_pattern_abnormal",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_dfxloan_sign_v",
                        "pboc_exp_custom_rpt_v",
                        "pboc_imp_custom_rpt_v"])
def corp_pattern_abnormal(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_dfxloan_sign_end_date = t_date_dict.get("pboc_dfxloan_sign")
    pboc_exp_custom_rpt_end_date = t_date_dict.get("pboc_exp_custom_rpt")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")
    # 【对公收入】一般贸易收款笔数 【指标计算当天过去一年】
    sql = f'''
        select corp_code, count(distinct rptno) as c24, sum(tx_amt_usd) as c28
          from (
                select corp_code, rptno, tx_amt_usd
                  from pboc_corp_rcv_jn_v
                 where tx_code in ('121010', '121020', '121040')
                 and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公收入】一般贸易预收笔数 【指标计算当天过去一年】
    sql = f'''
        select corp_code, count(distinct rptno) as c25
          from (
                select corp_code, rptno
                  from pboc_corp_rcv_jn_v
                 where tx_code in ('121010', '121020', '121040')
                 and rcvpay_attr_code = 'A'
                 and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 【对公收入】一般贸易非电汇收款笔数 【指标计算当天过去一年】
    sql = f'''
        select corp_code, count(distinct rptno) as c26
          from (
                select corp_code, rptno
                  from pboc_corp_rcv_jn_v
                 where where tx_code in ('121010', '121020', '121040')
                 and settle_method_code = 'T'
                 and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view3")
    # 【国内外汇贷款签约信息】贸易融资笔数 【指标计算当天过去一年】
    sql = f'''
        select corp_code, count(distinct dfxloan_no) as c27
          from (
                select corp_code, dfxloan_no
                  from pboc_dfxloan_sign_v
                 where interest_start_date between add_months('{pboc_dfxloan_sign_end_date}', -12) and date('{pboc_dfxloan_sign_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view4")
    # 【出口报关单基本信息】成交总价（折美元）【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(deal_amt_usd) as c29
          from (
                select corp_code, deal_amt_usd
                  from pboc_exp_custom_rpt_v
                 where exp_datebetween add_months('{pboc_exp_custom_rpt_end_date}', -12) and date('{pboc_exp_custom_rpt_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view5")

    # 【进口报关】进口成交总价（折美元）汇总 【指标计算当天过去一年】
    sql = f'''
            select corp_code, sum(deal_amt_usd) as c8 
          from (
                select corp_code, deal_amt_usd
                  from pboc_imp_custom_rpt_v 
                 where imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12) and date('{pboc_imp_custom_rpt_end_date}')
                )
         group by corp_code
            '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view6")
    # 是否四零一低企业
    sql = """
    select 
        corp_code, 
        company_name,
        case when c24>=10 and c25=0 and c26=0 and c27=0 and c8=0 and tmp_index < 0.2 then 1 else 0 end corp_pattern_abnormal
    FROM 
        (select 
            company.corp_code 
            ,company.company_name 
            ,c24
            ,coalesce(c25, 0) as c25
            ,coalesce(c26, 0) as c26
            ,coalesce(c27, 0) as c27
            ,coalesce(c8, 0) as c8
            ,case when c29 is null and (coalesce(c29, 0) - coalesce(c28, 0)) = 0 then null else abs(coalesce(c29, 1) / if(coalesce(c29, 0) - coalesce(c28, 0)=0, 1, coalesce(c29, 0) - coalesce(c28, 0))) end tmp_index 
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        left join view3 v3 on v3.corp_code = company.corp_code
        left join view4 v4 on v4.corp_code = company.corp_code
        left join view5 v5 on v5.corp_code = company.corp_code
        left join view6 v6 on v6.corp_code = company.corp_code
        ) 
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
