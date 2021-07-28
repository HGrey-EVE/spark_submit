#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 14:18
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_entreport_multi_location",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_entreport_multi_location(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 转口贸易多地或异地发生
    sql_str = f"""
                select a.bbd_company_name as company_name, 
                    a.corp_code, 
                    count(distinct bank_code_tmp) as corp_entreport_multi_location
                from target_company_v a 
                join (
                    select corp_code, 
                        substring(bank_code, 1, 4) as bank_code_tmp
                    from pboc_corp_rcv_jn_v 
                    where tx_code in ('122010','121030')
                        and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                ) b 
                on a.corp_code = b.corp_code
                group by a.bbd_company_name, a.corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df


