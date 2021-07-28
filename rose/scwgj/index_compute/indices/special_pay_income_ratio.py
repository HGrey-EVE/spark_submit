#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/23 : 15:38
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="special_pay_income_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def special_pay_income_ratio(entry: Entry, param: Parameter, logger):
    """
    指标计算：非特殊监管区企业或非成都地区企业特殊监管区付汇与收汇比例
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")

    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 【境内非金融机构】非特殊监管区企业或非成都地区企业特殊监管区
    temp_company_sql = """
                    select distinct corp_code
                    from pboc_corp_v
                    where is_taxfree = 0 or safecode in ('5100', '5101')
                 """
    logger.info(temp_company_sql)
    entry.spark.sql(temp_company_sql).createOrReplaceTempView("temp_company_view")

    # 【对公付款】非特殊监管区付款金额（折美元）
    pay_sql = f"""
                select L.corp_code, R.sum_pay_usd
                from temp_company_view L
                join (
                    select corp_code, sum(pay_tamt_usd) as sum_pay_usd
                    from pboc_corp_pay_jn_v
                    where tx_code = '121030'
                        and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12)
                                        and date('{pboc_corp_pay_jn_end_date}')
                    group by corp_code
                ) R
                on L.corp_code = R.corp_code
               """
    logger.info(pay_sql)
    entry.spark.sql(pay_sql).createOrReplaceTempView("pay_view")

    # 【对公收入】非特殊监管区收款金额（折美元）
    rcv_sql = f"""
                select L.corp_code, R.sum_rcv_usd
                from temp_company_view L
                join (
                    select corp_code, sum(rcv_tamt_usd) as sum_rcv_usd
                    from pboc_corp_rcv_jn_v
                    where tx_code = '121030' 
                        and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12)
                                        and date('{pboc_corp_rcv_jn_end_date}')
                    group by corp_code
                ) R
                on L.corp_code = R.corp_code
               """
    logger.info(rcv_sql)
    entry.spark.sql(rcv_sql).createOrReplaceTempView("rcv_view")

    # 非特殊监管区企业或非成都地区企业特殊监管区付汇与收汇比例
    sql_str = """
                select corp_code, 
                    company_name, 
                    sum_pay_usd/sum_rcv_usd as special_pay_income_ratio
                from (
                    select company.corp_code, 
                        company.company_name, 
                        coalesce(v1.sum_pay_usd, 0) as sum_pay_usd, 
                        case when v2.sum_rcv_usd is null or v2.sum_rcv_usd = 0 then 1
                             else v2.sum_rcv_usd
                        end sum_rcv_usd
                    from target_company_v company
                    left join pay_view v1 on company.corp_code = v1.corp_code
                    left join rcv_view v2 on company.corp_code = v2.corp_code
                )
                
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
