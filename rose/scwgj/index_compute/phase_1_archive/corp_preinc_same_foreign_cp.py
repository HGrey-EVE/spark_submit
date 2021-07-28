#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 16:09
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_preinc_same_foreign_cp",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_preinc_same_foreign_cp(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 【对公收入】 对境外同一交易对手向境内多家预付的企业进行标注
    sql_str = f"""
                select a.bbd_company_name as company_name, 
                    corp_code, 
                    case when b.black_corp_code is not null then 1
                         else 0 
                    end corp_preinc_same_foreign_cp
                from target_company_v a 
                left join (
                    select distinct black_corp_code
                    from (
                        select explode(corp_codes) as black_corp_code 
                        from (
                            select new_cp_payer_name, 
                                collect_set(corp_code) as corp_codes 
                            from (
                                select distinct corp_code, 
                                    substring(cp_payer_name, 5, 20) as new_cp_payer_name
                                from pboc_corp_rcv_jn_v
                                where rcvpay_attr_code = 'A'
                                    and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                            )
                            group by new_cp_payer_name
                        )
                        where size(corp_codes) > 1
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

