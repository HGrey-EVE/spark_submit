#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 14:07
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="rmb_exp_same_cp",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_cncc_v"])
def rmb_exp_same_cp(entry: Entry, param: Parameter, logger):
    # 多家收汇企业人民币支出是否支付给同一境内主体
    sql_str = f"""
                    select a.bbd_company_name as company_name, 
                        corp_code, 
                        count(distinct CRDTORNAME) as rmb_exp_same_cp
                    from target_company_v a 
                    join (
                        select DEBTORNAME, CRDTORNAME
                        from pboc_cncc_v
                    )b 
                    on a.bbd_company_name = b.DEBTORNAME
                    group by a.bbd_company_name, corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
