#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 16:03
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="black_isneg_rel",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "negative_list_v",
                        "black_list_v",
                        "pboc_corp_rcv_jn_v", 
                        "pboc_corp_pay_jn_v"])
def black_isneg_rel(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_dict_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    
    # 疑似负面主体交易相关联境内企业
    temp_sql = """
                select distinct cp_payer_name
                from (
                    select distinct corp_code
                    from (
                        select corp_code
                        from black_list_v
                        union all
                        select corp_code
                        from negative_list_v
                    )
                ) a
                join (
                    select distinct corp_code, 
                        substring(cp_name, 5, 20) as cp_payer_name
                    from (
                        select corp_code, cp_payer_name as cp_name
                        from pboc_corp_rcv_jn_v
                        union all
                        select corp_code, cp_rcver_name as cp_name
                        from pboc_corp_pay_jn_v
                    )
                ) b
                on a.corp_code = b.corp_code
               """
    execute_index_sql(entry, temp_sql, logger).createOrReplaceTempView("tmp_v")
    
    temp_1_sql = f"""
                    select corp_code, substring(cp_payer_name, 5, 20) as cp_name
                    from pboc_corp_rcv_jn_v
                    where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}') and date('{pboc_corp_rcv_jn_end_date}')
                    union all
                    select corp_code, substring(cp_rcver_name, 5, 20) as cp_name
                    from pboc_corp_pay_jn_v
                    where pay_date between add_months('{pboc_corp_pay_jn_end_date}') and date('{pboc_corp_pay_jn_end_date}')
                  """
    execute_index_sql(entry, temp_1_sql, logger).createOrReplaceTempView("tmp_1_v")

    sql_str = """
                select a.bbd_company_name as company_name, a.corp_code, 
                    case when b.black_corp_code is not null then 1
                         else 0 
                    end black_isneg_rel
                from target_company_v a 
                left join (
                    select distinct b.corp_code as black_corp_code 
                    from tmp_v a 
                    join (
                        select corp_code, cp_name
                        from tmp_1_v
                    ) b 
                    on a.cp_payer_name = b.cp_name
                ) b 
                on a.corp_code = b.black_corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
