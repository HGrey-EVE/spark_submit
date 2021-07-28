#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:59
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="prepay_guar_capinc_interval",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_corp_pay_jn_v"])
def prepay_guar_capinc_interval(entry: Entry, param: Parameter, logger):

    # 预付与自身资本金收入时间间隔
    sql_str = """
                select bbd_company_name as company_name, 
                    corp_code, 
                    min(tmp_gap) as prepay_guar_capinc_interval
                from (
                    select a.bbd_company_name, 
                        a.corp_code, 
                        abs(datediff(rcv_date, pay_date)) as tmp_gap
                    from target_company_v a 
                    join (
                        select corp_code, rcv_date
                        from pboc_corp_rcv_jn_v
                        where tx_code in ('622011','622012','622013')
                    ) b
                    join (
                        select corp_code, pay_date
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
