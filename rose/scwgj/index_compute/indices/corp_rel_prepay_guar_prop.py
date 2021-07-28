#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 11:22
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_rel_prepay_guar_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "all_off_line_relations_v",
                        "all_company_v",
                        "pboc_con_exguaran_new_v"])
def corp_rel_prepay_guar_prop(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_con_date = t_date_dict.get('pboc_con_exguaran_new')
    part1_sql = f"""
                    select corp_code, 
                        sum(tx_amt_usd) as c1, 
                        concat(pay_date_tmp, '-01') as new_pay_date
                    from (
                           select corp_code, 
                            tx_amt_usd, 
                            substring(pay_date, 1, 7) as pay_date_tmp
                        from pboc_corp_pay_jn_v 
                        where rcvpay_attr_code = 'A'
                        and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                    )
                    group by corp_code, pay_date_tmp
                 """
    execute_index_sql(entry, part1_sql, logger).createOrReplaceTempView("part1")

    part2_sql = f"""
                    select a.corp_code, 
                        sum(usdguaranamount) as c2, 
                        concat(maturity_tmp, '-01') as new_maturity
                    from (
                        select a.corp_code, 
                            b.corp_code as relation_corp_code
                        from all_off_line_relations_v a 
                        join all_company_v b 
                        on a.company_rel_degree_1 = b.company_name
                    ) a 
                    join (
                        select corp_code, 
                            usdguaranamount, 
                            substring(maturity, 1, 7) as maturity_tmp
                        from pboc_con_exguaran_new_v
                        where (guar_type_name = '其他融资性担保' 
                                or guar_type_name = '授信额度担保' 
                                or '自身生产经营需要担保')
                        and upper(countryname0) != 'CNH' 
                        and contractdate between add_months('{t_con_date}', -12) and date('{t_con_date}')
                    ) b 
                    on a.relation_corp_code = b.corp_code
                    group by a.corp_code, maturity_tmp
                 """
    execute_index_sql(entry, part2_sql, logger).createOrReplaceTempView("part2")

    tmp1_sql = """
                    select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        b.new_pay_date, b.c1 
                    from target_company_v a 
                    left join part1 b 
                    on a.corp_code = b.corp_code
               """
    execute_index_sql(entry, tmp1_sql, logger).createOrReplaceTempView("tmp1")

    tmp_v_sql = """
                    select a.*, b.c2, b.new_maturity
                    from tmp1 a 
                    left join part2 b 
                    on a.corp_code = b.corp_code
                """
    execute_index_sql(entry, tmp_v_sql, logger).createOrReplaceTempView("tmp_v")

    # 预付金额与关联方内保外贷签约金额的比例
    sql_str = """
                    select company_name, 
                        corp_code, 
                        count(1) as corp_rel_prepay_guar_prop
                    from (
                        select company_name, 
                            corp_code, 
                            tmp_prop 
                        from (
                            select company_name, 
                                corp_code, 
                                case when c1 is null and c2 is null then null 
                                     else coalesce(c1, 1) / coalesce(c2, 1) 
                                end tmp_prop
                            from tmp_v 
                            where datediff(new_pay_date, new_maturity) between 28 and 31
                        )
                        where tmp_prop > 0.7 and tmp_prop < 1.3
                    )
                    group by company_name, corp_code
               """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df