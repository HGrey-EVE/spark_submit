#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 14:20
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_rmbpay_genrcv_gap",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_corp_pay_jn_v"])
def corp_rmbpay_genrcv_gap(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    # 人民币付汇时间与货物贸易收入的时间间隔
    sql_str = f"""
                select company_name, 
                    corp_code,
                    avg(min_tmp_day) as corp_rmbpay_genrcv_gap
                from (
                    select company_name, 
                        corp_code, 
                        rcv_date,
                        min(tmp_day) as min_tmp_day
                    from (
                        select company_name, 
                            corp_code, 
                            rcv_date, 
                            tmp_day
                        from (
                            select a.bbd_company_name as company_name, 
                                a.corp_code, 
                                b.rcv_date, 
                                datediff(b.rcv_date, c.pay_date) as tmp_day
                            from target_company_v a 
                            join (
                                select corp_code, rcv_date  
                                from pboc_corp_rcv_jn_v
                                where tx_code in ('121010', '121020', '121040')
                                    and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                            ) b 
                            join (
                                select corp_code, pay_date
                                from pboc_corp_pay_jn_v
                                where tx_ccy_code = 'CNY'
                                    and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                            ) c 
                            on a.corp_code = b.corp_code 
                                and a.corp_code = c.corp_code 
                                and b.corp_code = c.corp_code
                        )
                        where tmp_day > 0
                    )
                    group by company_name, corp_code, rcv_date
                )
                group by company_name, corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)
    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
