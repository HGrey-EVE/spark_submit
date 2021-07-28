#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/3 : 17:25
@Author: wenhao@bbdservice.com
"""
from pyspark.sql import DataFrame

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="yf_rptamt_sum_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", 
                        "pboc_imp_custom_rpt_v",
                        "pboc_adv_pay_v", 
                        "pboc_corp_pay_jn_v", 
                        "pboc_delay_pay_v"])
def yf_rptamt_sum_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")
    pboc_adv_pay_end_date = t_date_dict.get("pboc_adv_pay")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_delay_pay_end_date = t_date_dict.get("pboc_delay_pay")

    # 【进口报关】进口成交总价（折美元）汇总 【指标计算当天过去一年】
    c8_sql = f"""
                 select corp_code, sum(deal_amt_usd) as c8 
                 from (
                     select corp_code, deal_amt_usd
                     from pboc_imp_custom_rpt_v 
                     where imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12) and date('{pboc_imp_custom_rpt_end_date}')
                 )
                 group by corp_code
             """
    execute_index_sql(entry, c8_sql, logger).createOrReplaceTempView("view_c8")

    # 【预付货款】报告金额（折美元）汇总 【指标计算当天过去一年】
    c30_sql = f"""
                  select corp_code, sum(rpt_amt_usd) as c30
                  from (
                        select corp_code, rpt_amt_usd
                        from pboc_adv_pay_v
                        where expt_imp_date between add_months('{pboc_adv_pay_end_date}', -12) and date('{pboc_adv_pay_end_date}')
                  )
                  group by corp_code
               """
    execute_index_sql(entry, c30_sql, logger).createOrReplaceTempView("view_c30")

    # 【对公付款】货到付款交易金额（折美元）汇总 + 其他付汇交易金额（折美元）汇总 【指标计算当天过去一年】
    c31_sql = f"""
                    select corp_code, sum(tx_amt_usd) as c31
                    from (
                        select corp_code, tx_amt_usd
                        from pboc_corp_pay_jn_v
                        where rcvpay_attr_code = 'P'
                        and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                        union all 
                        select corp_code, tx_amt_usd
                        from pboc_corp_pay_jn_v
                        where rcvpay_attr_code = 'O'
                        and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c31_sql, logger).createOrReplaceTempView("view_c31")

    # 【延期付款】报告金额（折美元）汇总【指标计算当天过去一年】
    c32_sql = f"""
                    select corp_code, sum(rpt_amt_usd) as c32
                    from (
                        select corp_code, rpt_amt_usd
                        from pboc_delay_pay_v
                        where expt_pay_date between add_months('{pboc_delay_pay_end_date}', -12) and date('{pboc_delay_pay_end_date}')
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c32_sql, logger).createOrReplaceTempView("view_c32")

    # 预计预付到货的资金流与进口货物流比例
    sql_str = """
                select company_name, corp_code, 
                    case when t1=0 then c30
                         else coalesce(c30, 1)/t1 
                    end yf_rptamt_sum_prop
                from (
                    select company.company_name, 
                        company.corp_code, 
                        v2.c30, 
                        (coalesce(v1.c8, 0)-coalesce(v3.c31, 0)-coalesce(v4.c32, 0)) as t1
                    from target_company_v company
                    left join view_c8 v1 on company.corp_code = v1.corp_code
                    left join view_c30 v2 on company.corp_code = v2.corp_code
                    left join view_c31 v3 on company.corp_code = v3.corp_code
                    left join view_c32 v4 on company.corp_code = v4.corp_code
                )
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df

    
def execute_index_sql(entry: Entry, sql_str: str, logger) -> DataFrame:
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df