#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 14:21
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_exp_proper_use",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v"])
def corp_exp_proper_use(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 结汇待支付账户支付是否符合使用范围
    sql_str = f"""
                    select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        case when b.corp_code is not null then 1
                             else 0 
                        end corp_exp_proper_use
                    from target_company_v a 
                    left join (
                        select distinct corp_code
                        from (
                            select corp_code, 
                                substring(cp_rcver_name, 5, 20) as new_cp_rcver_name
                            from pboc_corp_pay_jn_v
                            where pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                        )
                        where new_cp_rcver_name like '%证券%'
                    )b 
                     on a.corp_code = b.corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
