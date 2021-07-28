#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 10:15
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="export_ct_varicoef",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_exp_custom_rpt_v"])
def export_ct_varicoef(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_exp_custom_rpt_end_date = t_date_dict.get("pboc_exp_custom_rpt")

    # 出口贸易笔数变异系数（按月）分子标准差 【指标计算当天过去一年】
    c37_sql = f"""
                    select corp_code, 
                        var_pop(custom_rpt_no_count) as c37
                    from (
                        select corp_code, 
                            exp_date_tmp, 
                            count(distinct custom_rpt_no) as custom_rpt_no_count
                        from (
                            select corp_code, 
                                custom_rpt_no, 
                                substring(exp_date, 1, 7) as exp_date_tmp
                            from pboc_exp_custom_rpt_v 
                            where exp_date between add_months('{pboc_exp_custom_rpt_end_date}', -12) and date('{pboc_exp_custom_rpt_end_date}')
                        )
                        group by corp_code, exp_date_tmp
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c37_sql, logger).createOrReplaceTempView("view_c37")

    # 出口贸易笔数变异系数（按月）分母平均值 【指标计算当天过去一年】
    c38_sql = f"""
                    select corp_code, 
                        avg(custom_rpt_no_count) as c38
                    from (
                        select corp_code, 
                            exp_date_tmp, 
                            count(distinct custom_rpt_no) as custom_rpt_no_count
                        from (
                            select corp_code, 
                                custom_rpt_no, 
                                substring(exp_date, 1, 7) as exp_date_tmp
                            from pboc_exp_custom_rpt_v 
                            where exp_date between add_months('{pboc_exp_custom_rpt_end_date}', -12) and date('{pboc_exp_custom_rpt_end_date}')
                        )
                        group by corp_code, exp_date_tmp
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c38_sql, logger).createOrReplaceTempView("view_c38")

    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c37 is null and v2.c38 is null then null
                         else coalesce(v1.c37, 1)/coalesce(v2.c38, 1) 
                    end export_ct_varicoef
                from target_company_v company
                left join view_c37 v1 on company.corp_code = v1.corp_code
                left join view_c38 v2 on company.corp_code = v2.corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
