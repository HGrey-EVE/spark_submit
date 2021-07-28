#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 10:15
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_inc_ct_varicoef",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_inc_ct_varicoef(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 出口收汇笔数变异系数（按月） 分子标准差 【指标计算当天过去一年】
    c39_sql = f"""
                select corp_code, 
                    var_pop(rptno_count) as c39
                from (
                    select corp_code, 
                        rcv_date_tmp, 
                        count(distinct rptno) as rptno_count
                    from (
                        select corp_code, 
                            rptno, 
                            substring(rcv_date, 1, 7) as rcv_date_tmp
                        from pboc_corp_rcv_jn_v 
                        where tx_code in ('121010', '121020', '121040', '121080', '121090', '121990')
                        and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                    )
                    group by corp_code, rcv_date_tmp
                )
                group by corp_code
               """
    execute_index_sql(entry, c39_sql, logger).createOrReplaceTempView("view_c39")

    # 出口收汇笔数变异系数（按月） 分母平均值 【指标计算当天过去一年】
    c40_sql = f"""
                select corp_code, 
                    avg(rptno_count) as c40
                from (
                    select corp_code, 
                        rcv_date_tmp, 
                        count(distinct rptno) as rptno_count
                    from (
                        select corp_code, 
                            rptno, 
                            substring(rcv_date, 1, 7) as rcv_date_tmp
                        from pboc_corp_rcv_jn_v 
                        where tx_code in ('121010', '121020', '121040', '121080', '121090', '121990')
                        and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                    )
                    group by corp_code, rcv_date_tmp
                )
                group by corp_code
               """
    execute_index_sql(entry, c40_sql, logger).createOrReplaceTempView("view_c40")

    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c39 is null and v2.c40 is null then null
                         else coalesce(v1.c39, 1)/coalesce(v2.c40, 1) 
                    end corp_inc_ct_varicoef
                from target_company_v company
                left join view_c39 v1 on company.corp_code = v1.corp_code
                left join view_c40 v2 on company.corp_code = v2.corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
