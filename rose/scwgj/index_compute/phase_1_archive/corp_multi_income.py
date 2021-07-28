#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/3 : 18:06
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_multi_income",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_multi_income(entry: Entry, param: Parameter, logger):

    # 资本金多行业多区县多企业汇入
    sql_str = """
                select a.bbd_company_name as company_name, 
                    a.corp_code,
                    case when b.black_corp_code is not null then 1
                         else 0 
                    end corp_multi_income
                from target_company_v a
                left join (
                    select distinct black_corp_code
                    from (
                        select explode(corp_codes) as black_corp_code
                        from (
                            select cp_payer_name,
                                    count(distinct payer_country_code) as payer_country_code_count,
                                    count(distinct safecode) as safecode_count,
                                    count(distinct indu_attr_code) as indu_attr_code_count,
                                    collect_set(corp_code) as corp_codes
                            from pboc_corp_rcv_jn_v
                            group by cp_payer_name
                        )
                        where payer_country_code_count+safecode_count+indu_attr_code_count>=3
                    )
                )b
                on a.corp_code = b.black_corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
