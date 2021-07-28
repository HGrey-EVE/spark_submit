#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/3 : 18:11
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="basic_esage",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "qyxx_basic_v"])
def basic_esage(entry: Entry, param: Parameter, logger):

    # 成立年限
    sql_str = """
                select a.bbd_company_name as company_name, 
                    a.corp_code,
                    floor(datediff(current_date(), esdate) / 365) as basic_esage
                from target_company_v a
                join (
                    select company_name, esdate, bbd_qyxx_id
                    from qyxx_basic_v
                )b
                on a.bbd_qyxx_id = b.bbd_qyxx_id
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
