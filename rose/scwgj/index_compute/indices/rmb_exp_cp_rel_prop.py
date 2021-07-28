#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 15:33
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rmb_exp_cp_rel_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "all_off_line_relations_v",
                        "pboc_corp_pay_jn_v",
                        "all_company_v"])
def rmb_exp_cp_rel_prop(entry: Entry, param: Parameter, logger):
    """
    二期有更新。
    待支付账户付款的交易对手是关联企业（非跨国公司集中运营）的占比
        1. 【对公付款】交易对手是关联企业数量/交易对手数量；
        2. 分子取 【对公付款】交易编码=929070；
        3. 指标计算当天过去一年；
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 【对公付款】待支付账户交易中关联企业数量 【指标计算当天过去一年】
    c55_sql = f"""
                    select a.corp_code, 
                        count(a.relation_corp_code) as c55 
                    from (
                        select a.corp_code, 
                            b.corp_code as relation_corp_code
                        from all_off_line_relations_v a 
                        join all_company_v b 
                        on a.company_rel_degree_1 = b.company_name
                        ) a 
                    join (
                        select corp_code
                        from pboc_corp_pay_jn_v
                        where tx_code = '929070'
                            and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) 
                                            and date('{pboc_corp_pay_jn_end_date}')
                    ) b 
                    on a.relation_corp_code = b.corp_code
                    group by a.corp_code
               """
    execute_index_sql(entry, c55_sql, logger).createOrReplaceTempView("view_c55")

    # 【对公付款】待支付账户交易的企业数量 【指标计算当天过去一年】
    c56_sql = f"""
                    select corp_code, 
                        count(1) as c56
                    from (
                        select corp_code
                        from pboc_corp_pay_jn_v
                        where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) 
                                            and date('{pboc_corp_pay_jn_end_date}')
                    )
                    group by corp_code
               """
    execute_index_sql(entry, c56_sql, logger).createOrReplaceTempView("view_c56")

    # 待支付账户付款的交易对手是关联企业的占比
    sql_str = """
                        select company.corp_code, 
                            company.company_name,  
                            case when v1.c55 is null and v2.c56 is null then null
                                 else coalesce(v1.c55, 1)/coalesce(v2.c56, 1) 
                            end rmb_exp_cp_rel_prop
                        from target_company_v company
                        left join view_c55 v1 on company.corp_code = v1.corp_code
                        left join view_c56 v2 on company.corp_code = v2.corp_code
                       """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
