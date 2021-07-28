#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 13:40
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="ys_preinc_ct_varicoef",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def ys_preinc_ct_varicoef(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 预收货款笔数, 金额变异系数（按月）分子标准差 【指标计算当天过去一年】
    c45_sql = f"""
                select corp_code, 
                    var_pop(rptno_count) as c45, 
                    var_pop(tx_amt_usd_sum) as c46
                from (
                    select corp_code, 
                        rcv_date_tmp, 
                        count(distinct rptno) as rptno_count, 
                        sum(tx_amt_usd) as tx_amt_usd_sum
                    from (
                        select corp_code, 
                            rptno, 
                            tx_amt_usd, 
                            substring(rcv_date, 1, 7) as rcv_date_tmp
                        from pboc_corp_rcv_jn_v 
                        where rcvpay_attr_code = 'A'
                            and rcv_date between and_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                    )
                    group by corp_code, rcv_date_tmp
                )
                group by corp_code
               """
    execute_index_sql(entry, c45_sql, logger).createOrReplaceTempView("view_c45")

    # 预收货款笔数, 金额变异系数（按月）分母平均值 【指标计算当天过去一年】
    c47_sql = f"""
                select corp_code, 
                    avg(rptno_count) as c47, 
                    avg(tx_amt_usd_sum) as c48
                from (
                    select corp_code, 
                        rcv_date_tmp, 
                        count(distinct rptno) as rptno_count, 
                        sum(tx_amt_usd) as tx_amt_usd_sum
                    from (
                        select corp_code, 
                            rptno, 
                            tx_amt_usd, 
                            substring(rcv_date, 1, 7) as rcv_date_tmp
                        from pboc_corp_rcv_jn_v 
                        where rcvpay_attr_code = 'A'
                            and rcv_date between and_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                    )
                    group by corp_code, rcv_date_tmp
                )
                group by corp_code
               """
    execute_index_sql(entry, c47_sql, logger).createOrReplaceTempView("view_c47")

    # 预收货款笔数变异系数（按月）
    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c45 is null and v2.c47 is null then null
                         else coalesce(v1.c45, 1)/coalesce(v2.c47, 1) 
                    end ys_preinc_ct_varicoef
                from target_company_v company
                left join view_c45 v1 on company.corp_code = v1.corp_code
                left join view_c47 v2 on company.corp_code = v2.corp_code
              """
    result_df = entry.spark.sql(sql_str)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df

