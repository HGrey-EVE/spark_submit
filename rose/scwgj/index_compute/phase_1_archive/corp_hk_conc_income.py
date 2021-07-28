#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/4 : 10:58
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_hk_conc_income",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_hk_conc_income(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    # 境内多个企业仅与同一境外交易（香港）对手发生贸易收入

    # 找到与某个香港交易对手交易数量大于等于5的企业，并作为黑名单企业
    sql_black_list = f"""
                select distinct black_corp_code
                from (
                    select explode(corp_codes) as black_corp_code
                    from (
                        select cp_payer_name, collect_set(corp_code) as corp_codes
                        from (
                            select cp_payer_name, corp_code
                            from pboc_corp_rcv_jn_v
                            where payer_country_code = 'HKG'
                                and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                            union all 
                            select cp_payer_name, corp_code
                            from pboc_corp_rcv_jn_v
                            where payer_country_code = 'SGP'
                                and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                            union all 
                            select cp_payer_name, corp_code
                            from pboc_corp_rcv_jn_v
                            where payer_country_code = 'MAC'
                                and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                        )
                        group by cp_payer_name
                    )
                where size(corp_codes) >= 5
                        )
             """
    execute_index_sql(entry, sql_black_list, logger).createOrReplaceTempView("black_list_tmp_v")

    # 黑名单企业与香港、新加坡、澳门的交易金额
    sql_c1_v = f"""
                   select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        sum(tx_amt_usd) as c1
                   from (
                         select a.bbd_company_name, 
                            a.corp_code
                         from target_company_v a
                         join black_list_tmp_v b
                         on a.corp_code = b.black_corp_code
                   ) a
                   join (
                         select corp_code, 
                            tx_amt_usd
                         from pboc_corp_rcv_jn_v
                         where payer_country_code = 'HKG'
                         and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                         union all 
                         select corp_code, 
                            tx_amt_usd
                         from pboc_corp_rcv_jn_v
                         where payer_country_code = 'SGP'
                         and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                         union all 
                         select corp_code, 
                            tx_amt_usd
                         from pboc_corp_rcv_jn_v
                         where payer_country_code = 'MAC'
                         and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                   ) b
                   on a.corp_code = b.corp_code
                   group by a.bbd_company_name, a.corp_code 
                """
    execute_index_sql(entry, sql_c1_v, logger).createOrReplaceTempView("c1_v")

    # 黑名单企业的交易总额
    sql_c2_v = f"""
                  select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        sum(tx_amt_usd) as c2
                  from (
                        select a.bbd_company_name, a.corp_code
                        from target_company_v a
                        join black_list_tmp_v b
                        on a.corp_code = b.black_corp_code
                  )a
                  join (
                        select corp_code, tx_amt_usd
                        from pboc_corp_rcv_jn_v
                        where rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                  )b
                  on a.corp_code = b.corp_code
                  group by a.bbd_company_name, a.corp_code
                """
    execute_index_sql(entry, sql_c2_v, logger).createOrReplaceTempView("c2_v")

    sql_part1 = """
                    select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        c1
                    from target_company_v a
                    left join c1_v b
                    on a.corp_code = b.corp_code
                 """
    execute_index_sql(entry, sql_part1, logger).createOrReplaceTempView("part1")

    sql_final_tmp_v = """
                        select a.company_name, 
                            a.corp_code, 
                            c1, 
                            c2
                        from part1 a
                        left join c2_v b
                        on a.corp_code = b.corp_code
                      """
    execute_index_sql(entry, sql_final_tmp_v, logger).createOrReplaceTempView("final_tmp_v")

    result_sql_str = """
                        select company_name, 
                            corp_code,
                            case when tmp_prop > 0.9 then 1
                                 else 0 
                            end corp_hk_conc_income
                        from (
                            select company_name, 
                                corp_code,
                                case when c1 is null and c2 is null then null
                                     else coalesce(c1, 1) / coalesce(c2, 1) 
                                end tmp_prop
                            from final_tmp_v
                        )
                     """
    result_df = execute_index_sql(entry, result_sql_str, logger)
    
    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df


