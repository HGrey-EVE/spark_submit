#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/3 : 17:46
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_settle_prepay_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_lcy_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def corp_settle_prepay_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_lcy_end_date = t_date_dict.get("pboc_corp_lcy")

    # 【企业结汇】发生日期当月资本金结汇 【只计算当月】
    c33_sql = f"""
                   select corp_code, sum(salefx_amt_usd) as c33
                   from (
                        select corp_code, salefx_amt_usd
                        from pboc_corp_lcy_v
                        where tx_code in ('622011','622012','622013')
                        and deal_date between add_months('{pboc_corp_lcy_end_date}', -1) and date('{pboc_corp_lcy_end_date}')
                   )
                   group by corp_code 
               """
    execute_index_sql(entry, c33_sql, logger).createOrReplaceTempView("view_c33")

    # 【对公付款】当月预付金额（折美元）【只计算当月】
    c34_sql = f"""
                  select corp_code, sum(tx_amt_usd) as c34
                  from (
                        select corp_code, tx_amt_usd
                        from pboc_corp_pay_jn_v
                        where rcvpay_attr_code = 'A'
                        and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -1) and date('{[pboc_corp_pay_jn_end_date]}')
                  )
                  group by corp_code
               """
    execute_index_sql(entry, c34_sql, logger).createOrReplaceTempView("view_c34")

    # 【对公收入】资本金收入 【只计算当月】
    c35_sql = f"""
                  select corp_code, sum(tx_amt_usd) as c35
                  from (
                        select corp_code, tx_amt_usd
                        from pboc_corp_rcv_jn_v
                        where tx_code in ('622011','622012','622013')
                        and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -1) and date('{pboc_corp_rcv_jn_end_date}')
                  )
                  group by corp_code
               """
    execute_index_sql(entry, c35_sql, logger).createOrReplaceTempView("view_c35")

    # 资本金当月全额结汇或预付比例
    sql_str = """
                select company_name, 
                    corp_code, 
                    case when c35 is null and tmp_col = 0 then null 
                         else coalesce(c35, 0) / if(tmp_col=0, 1, tmp_col) 
                    end corp_settle_prepay_prop
                from (
                    select company.company_name, 
                        company.corp_code, 
                        v3.c35, 
                        coalesce(v1.c33, 0) + coalesce(v2.c34, 0) as tmp_col
                    from target_company_v company
                    left join view_c33 v1 on company.corp_code = v1.corp_code
                    left join view_c34 v2 on company.corp_code = v2.corp_code
                    left join view_c35 v3 on company.corp_code = v3.corp_code
                )
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
