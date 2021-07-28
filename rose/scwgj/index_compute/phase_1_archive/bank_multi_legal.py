#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 10:44
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="bank_multi_legal",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_v"])
def bank_multi_legal(entry: Entry, param: Parameter, logger):

    # 涉及多个公司的法人代表
    sql_str = """
                    select a.bbd_company_name as company_name, a.corp_code,
                        case when b.black_corp_code is not null then 1
                        else 0 end bank_multi_legal
                    from target_company_v a
                    left join (
                        select distinct black_corp_code
                        from (
                            select explode(corp_codes) as black_corp_code
                            from (
                                select frname, collect_set(corp_code) as corp_codes
                                from (
                                    select frname, corp_code
                                    from pboc_corp_v
                                    where frname is not null
                                )
                                group by frname
                            )
                            where size(corp_codes) >= 2
                        )
                    ) b
                    on a.corp_code = b.black_corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
