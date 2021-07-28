#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/3 : 16:41
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_prepay_capinc_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def corp_prepay_capinc_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_rcv_date = t_date_dict.get('pboc_corp_rcv_jn')

    # 【对公付款】预付货款交易金额(汇总) 【指标计算当天过去一年】
    c11_sql = f"""
                 select corp_code, sum(tx_amt_usd) as c11 
                 from (
                     select corp_code, tx_amt_usd
                         from pboc_corp_pay_jn_v
                         where rcvpay_attr_code = 'A'
                         and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                     )
                 group by corp_code
               """
    execute_index_sql(entry, c11_sql, logger).createOrReplaceTempView("view_c11")

    # 【对公收入】资本金收入汇总 【指标计算当天过去一年】
    c23_sql = f"""
                 select corp_code, sum(tx_amt_usd) as c23
                 from (
                     select corp_code, tx_amt_usd
                         from pboc_corp_rcv_jn_v
                         where tx_code in ('622011','622012','622013')
                         and rcv_date between add_months('{t_rcv_date}', -12) and date('{t_rcv_date}')
                     )
                 group by corp_code
              """
    execute_index_sql(entry, c23_sql, logger).createOrReplaceTempView("view_c23")

    # 【虚假欺骗交易】预付金额与自身资本金收入比例
    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c11 is null and v2.c23 is null then null
                         else coalesce(v1.c11, 1)/coalesce(v2.c23, 1) 
                    end corp_prepay_capinc_prop
                from target_company_v company
                left join view_c11 v1 on company.corp_code = v1.corp_code
                left join view_c23 v2 on company.corp_code = v2.corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

