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


@register(name="corp_signamt_txamt_entreport_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_dfxloan_sign_v"])
def corp_signamt_txamt_entreport_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_dfxloan_sign_end_date = t_date_dict.get("pboc_dfxloan_sign")

    # 【对公付款】转口贸易支出交易金额（折美元） 【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c19 
          from (
                select corp_code, tx_amt_usd
                  from pboc_corp_pay_jn_v
                 where tx_code in ('122010','121030')
                 and pay_date between add_months('{pboc_corp_pay_jn_end_date}',-12) and date('{pboc_corp_pay_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 【对公收入】境外融资金额折美元 【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(tx_amt_usd) as c20
          from (
                select corp_code, tx_amt_usd
                  from pboc_corp_rcv_jn_v
                 where tx_code = '822020'
                 and (regno = '' or regno = 'N/A')
                 and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}',-12) and date('{pboc_corp_rcv_jn_end_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")
    # 【国内外汇贷款签约信息表】贸易融资签约金额折美元）【指标计算当天过去一年】
    sql = f'''
        select corp_code, sum(sign_amt_usd) as c21
          from (
                select corp_code, sign_amt_usd
                  from pboc_dfxloan_sign_v
                 where interest_start_date between add_months('{pboc_dfxloan_sign_end_date}',-12) and date('{pboc_dfxloan_sign_end_date}')
                    and substring (domfx_loan_type_code,1,4) = '1102'
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view3")
    # 转口贸易付汇额与贸易融资的比例
    sql = """
    select 
        corp_code, 
        company_name,
        case when c19 is null and c20_21 = 0 then null 
             else coalesce(c19, 1) / if(c20_21=0, 1, c20_21) 
        end corp_signamt_txamt_entreport_prop
    from (
        select 
            company.corp_code, 
            company.company_name, 
            v1.c19, 
            coalesce(v2.c20, 0) + coalesce(v3.c21, 0) as c20_21
        from target_company_v company
        left join view1 v1 on v1.corp_code = company.corp_code
        left join view2 v2 on v2.corp_code = company.corp_code
        left join view3 v3 on v3.corp_code = company.corp_code
    ) 
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
