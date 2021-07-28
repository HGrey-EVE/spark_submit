#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 13:38
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="import_fxpay_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_imp_custom_rpt_v",
                        "pboc_corp_pay_jn_v"])
def import_fxpay_ratio(entry: Entry, param: Parameter, logger):
    """
    指标计算：进口与付汇比例。
        1. 【进口报关单基本情况表】进口成交总价/【对公付款】货物贸易付款（折美元）总价；
        2. 指标计算当天过去一年；
        3. 计算规则内有货物贸易的，【对公付款】【对公收款】交易编码='121010,121020,121040,121080,121090,121990，121110,121100';
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 【进口报关单基本情况表】进口成交总价(非折美元)
    imp_sum_sql = f"""
                    select corp_code, sum(deal_amt) as sum_imp_amt
                    from pboc_imp_custom_rpt_v 
                    where imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12)
                                     and date('{pboc_imp_custom_rpt_end_date}')
                    group by corp_code
                   """
    logger.info(imp_sum_sql)
    entry.spark.sql(imp_sum_sql).createOrReplaceTempView("imp_sum_view")

    # 【对公付款】货物贸易付款（折美元）总价
    pay_sum_sql = f"""
                    select corp_code, sum(pay_tamt_usd) as sum_pay_amt
                    from pboc_corp_pay_jn_v
                    where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12)
                                     and date('{pboc_corp_pay_jn_end_date}')
                        and tx_code in ('121010', '121020', '121040', '121080', 
                                        '121090', '121990', '121110', '121100')
                    group by corp_code
                   """
    logger.info(pay_sum_sql)
    entry.spark.sql(pay_sum_sql).createOrReplaceTempView("pay_sum_view")

    sql_str = """
                select corp_code, 
                    company_name,
                    sum_imp_amt/sum_pay_amt as import_fxpay_ratio 
                from (
                    select company.corp_code,
                        company.company_name, 
                        coalesce(v1.sum_imp_amt, 0) as sum_imp_amt, 
                        case when v2.sum_pay_amt is null or v2.sum_pay_amt = 0 then 1 
                             else v2.sum_pay_amt
                        end sum_pay_amt
                    from target_company_v company
                    left join imp_sum_view v1 on company.corp_code = v1.corp_code
                    left join pay_sum_view v2 on company.corp_code = v2.corp_code
                )
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df



