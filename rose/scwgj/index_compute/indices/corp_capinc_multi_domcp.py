#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 10:34
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_capinc_multi_domcp",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_capinc_multi_domcp(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_rcv_jn')

    # 资本金收入交易对手是否为多个境内企业汇入资本金(对境内企业进行标记）
    sql_str = f"""
                select a.bbd_company_name as company_name, 
                    a.corp_code, 
                    case when b.black_corp_code is not null then 1 
                         else 0 
                    end corp_capinc_multi_domcp
                from target_company_v a 
                left join (
                    select distinct explode(corp_codes) as black_corp_code 
                    from (
                        select collect_set(corp_code) as corp_codes 
                        from (
                            select corp_code, 
                                substring(cp_payer_name, 1, 15) as new_cp_payer_name
                            from pboc_corp_rcv_jn_v 
                            where tx_code in ('622011','622012','622013')
                            and rcv_date between add_months('{t_date}', -12) and date('{t_date}')
                        )
                        group by new_cp_payer_name
                    )
                    where size(corp_codes) > 3
                ) b 
                on a.corp_code = b.black_corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
