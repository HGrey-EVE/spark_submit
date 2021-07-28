#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 14:54
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rmb_inc_ct",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_cncc_v"])
def rmb_inc_ct(entry: Entry, param: Parameter, logger):
    # 人民币收入次数计数
    sql_str = f"""
                    select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        count(*) as rmb_inc_ct
                    from target_company_v a
                    join (
                        select CRDTORNAME
                        from pboc_cncc_v
                    )b
                    on a.company_name = b.CRDTORNAME
                    group by a.bbd_company_name, a.corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
