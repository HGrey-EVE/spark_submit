#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 14:54
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_entreport_exp_amt_varicoef",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v"])
def corp_entreport_exp_amt_varicoef(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 转口贸易付款月金额变异系数（按月）分子标准差 【指标计算当天过去一年】
    c49_sql = f"""
                    select corp_code, 
                        var_pop(tx_amt_usd_sum) as c49
                    from (
                        select corp_code, 
                            pay_date_tmp, 
                            sum(tx_amt_usd) as tx_amt_usd_sum
                        from (
                            select corp_code, 
                                tx_amt_usd, 
                                substring(pay_date, 1, 7) as pay_date_tmp
                            from pboc_corp_pay_jn_v 
                            where tx_code in ('122010','121030')
                                and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                        )
                        group by corp_code, pay_date_tmp
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c49_sql, logger).createOrReplaceTempView("view_c49")

    # 转口贸易付款月金额变异系数（按月）分母平均值 【指标计算当天过去一年】
    c50_sql = f"""
                    select corp_code, 
                        avg(tx_amt_usd_sum) as c50
                    from (
                        select corp_code, 
                            pay_date_tmp,  
                            sum(tx_amt_usd) as tx_amt_usd_sum
                        from (
                            select corp_code, 
                                tx_amt_usd, 
                                substring(pay_date, 1, 7) as pay_date_tmp
                            from pboc_corp_pay_jn_v 
                            where tx_code in ('122010','121030')
                                and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                        )
                        group by corp_code, pay_date_tmp
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c50_sql, logger).createOrReplaceTempView("view_c50")

    # 转口贸易付款月金额变异系数（按月）
    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c49 is null and v2.c50 is null then null
                         else coalesce(v1.c49, 1)/coalesce(v2.c50, 1) 
                    end corp_entreport_exp_amt_varicoef
                from target_company_v company
                left join view_c49 v1 on company.corp_code = v1.corp_code
                left join view_c50 v2 on company.corp_code = v2.corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

