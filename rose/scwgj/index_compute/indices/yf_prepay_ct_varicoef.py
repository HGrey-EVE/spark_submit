#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 11:23
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="yf_prepay_ct_varicoef",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v"])
def yf_prepay_ct_varicoef(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')

    # 预付货款笔数变异系数（按月）分子标准差 【指标计算当天过去一年】
    c41_sql = f"""
                  select corp_code, 
                        var_pop(rptno_count) as c41 
                  from (
                          select corp_code, 
                                pay_date_tmp, 
                                count(distinct rptno) as rptno_count
                          from (
                                select corp_code, 
                                    rptno, 
                                    substring(pay_date, 1, 7) as pay_date_tmp
                                from pboc_corp_pay_jn_v 
                                where rcvpay_attr_code = 'A'
                                    and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                                )
                          group by corp_code, pay_date_tmp
                        )
                  group by corp_code
               """
    execute_index_sql(entry, c41_sql, logger).createOrReplaceTempView("view_c41")

    # 预付货款笔数, 金额变异系数（按月）分母平均值 【指标计算当天过去一年】
    c42_sql = f"""
                select corp_code, 
                    avg(rptno_count) as c42
                from (
                    select corp_code, 
                        pay_date_tmp, 
                        count(distinct rptno) as rptno_count
                    from (
                        select corp_code, 
                            rptno, 
                            substring(pay_date, 1, 7) as pay_date_tmp
                        from pboc_corp_pay_jn_v 
                        where rcvpay_attr_code = 'A'
                            and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                    )
                    group by corp_code, pay_date_tmp
                )
                group by corp_code
               """
    execute_index_sql(entry, c42_sql, logger).createOrReplaceTempView("view_c42")

    # 预付货款笔数变异系数（按月）
    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c41 is null and v2.c42 is null then null
                         else coalesce(v1.c41, 1)/coalesce(v2.c42, 1) 
                    end yf_prepay_ct_varicoef
                from target_company_v company
                left join view_c41 v1 on company.corp_code = v1.corp_code
                left join view_c42 v2 on company.corp_code = v2.corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

