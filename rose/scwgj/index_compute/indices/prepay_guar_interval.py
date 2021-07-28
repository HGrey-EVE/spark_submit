#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:44
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="prepay_guar_interval",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_con_exguaran_new_v",
                        "pboc_corp_pay_jn_v"])
def prepay_guar_interval(entry: Entry, param: Parameter, logger):

    # 预付与自身内保外贷到期的时间间隔
    sql_str = f"""
                    select bbd_company_name as company_name, 
                        corp_code, 
                        min(tmp_gap) as prepay_guar_interval
                    from (
                        select a.bbd_company_name, 
                            a.corp_code, 
                            abs(datediff(maturity, pay_date)) as tmp_gap
                        from target_company_v a 
                        join (
                            select corp_code, 
                                maturity
                            from pboc_con_exguaran_new_v
                        ) b
                        join (
                            select corp_code, 
                                pay_date
                            from pboc_corp_pay_jn_v
                            where rcvpay_attr_code = 'A'
                        ) c 
                        on a.corp_code = b.corp_code 
                            and a.corp_code = c.corp_code 
                            and b.corp_code = c.corp_code
                    )
                    group by bbd_company_name, corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
