#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_capinc_multi_domcp_rel",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["all_off_line_relations_v",
                        "all_company_v",
                        "pboc_corp_rcv_jn_v",
                        "target_company_v"])
def corp_capinc_multi_domcp_rel(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")

    # 资本金收入交易对手是否为多个境内企业汇入资本金(对境内企业进行标记，对上述境内企业的关联企业进行标注）

    sql = f'''
        select a.bbd_company_name as company_name, a.corp_code, 
               case when b.relation_corp_code is not null then 1 
                    else 0 end corp_capinc_multi_domcp_rel
          from target_company_v a 
          left join (
                    select distinct relation_corp_code
                      from (
                            select distinct explode(corp_codes) as black_corp_code 
                              from (
                                    select collect_set(corp_code) as corp_codes 
                                      from (
                                            select corp_code, substring(cp_payer_name, 5, 20) as new_cp_payer_name
                                              from pboc_corp_rcv_jn_v 
                                             where tx_code in ('622011','622012','622013')
                                             and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                                            )
                                     group by new_cp_payer_name
                                    )
                             where size(corp_codes) > 3
                            )a 
                      join (
                            select a.corp_code, b.corp_code as relation_corp_code
                              from all_off_line_relations_v a 
                              join all_company_v b 
                             on a.company_rel_degree_1 = b.company_name
                           )b 
                     on a.black_corp_code = b.corp_code
                    )b 
         on a.corp_code = b.relation_corp_code
        '''


    logger.info(sql)
    df = entry.spark.sql(sql)

    return df


