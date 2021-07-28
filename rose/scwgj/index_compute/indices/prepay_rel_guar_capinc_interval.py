#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 14:13
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="prepay_rel_guar_capinc_interval",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "all_off_line_relations_v",
                        "all_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_corp_pay_jn_v"])
def prepay_rel_guar_capinc_interval(entry: Entry, param: Parameter, logger):

    # 预付与关联方资本金收入时间间隔最小值
    sql_str = """
                    select company_name, 
                        corp_code, 
                        min(tmp_gap) as prepay_rel_guar_capinc_interval
                    from (
                        select a.bbd_company_name as company_name, 
                            a.corp_code, 
                            abs(datediff(rcv_date, pay_date)) as tmp_gap
                        from target_company_v a 
                        join (
                            select a.corp_code, 
                                rcv_date
                            from (
                                select a.corp_code, 
                                    b.corp_code as relation_corp_code
                                from all_off_line_relations_v a 
                                join all_company_v b 
                                on a.company_rel_degree_1 = b.company_name
                            ) a 
                            join (
                                select corp_code, 
                                    rcv_date
                                from pboc_corp_rcv_jn_v
                                where tx_code in ('622011','622012','622013')
                            ) b 
                            on a.relation_corp_code = b.corp_code
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
                    group by company_name, corp_code 
              """
    result_df = execute_index_sql(entry, sql_str, logger)
    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
