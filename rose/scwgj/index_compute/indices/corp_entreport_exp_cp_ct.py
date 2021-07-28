#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 9:53
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_entreport_exp_cp_ct",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v"])
def corp_entreport_exp_cp_ct(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 转口贸易付汇交易对手计数
    sql_str = f"""
                select a.bbd_company_name as company_name, 
                    a.corp_code, 
                    count(distinct new_cp_rcver_name) as corp_entreport_exp_cp_ct
                from target_company_v a 
                join (
                    select corp_code, 
                        substring(cp_rcver_name, 5, 20) as new_cp_rcver_name
                    from (
                        select corp_code, 
                            cp_rcver_name
                        from pboc_corp_pay_jn_v 
                        where tx_code in ('122010','121030')
                            and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                    )
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
