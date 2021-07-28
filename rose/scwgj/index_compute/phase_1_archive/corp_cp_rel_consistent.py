#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 17:50
@Author: wenhao@bbdservice.com
"""
from pyspark.sql.types import IntegerType

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_cp_rel_consistent",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "all_off_line_relations_v",
                        "all_company_v",
                        "pboc_corp_rcv_jn_v"])
def corp_cp_rel_consistent(entry: Entry, param: Parameter, logger):
    # 预付交易对手是否与关联企业资本金收入交易对手一致
    t_date_dict = param.get("tmp_date_dict")
    pboc_corp_rcv_jn_end_date = t_date_dict.get("pboc_corp_rcv_jn")
    pboc_corp_pay_jn_end_date = t_date_dict.get("pboc_corp_pay_jn")

    part1_sql = f"""
                    select corp_code, collect_set(new_cp_rcver_name) as cp_rcver_names
                    from (
                        select distinct corp_code, substring(cp_rcver_name, 5, 20) as new_cp_rcver_name
                        from pboc_corp_pay_jn_v 
                        where rcvpay_attr_code = 'A'
                            and pay_date between add_months('{pboc_corp_pay_jn_end_date}', -12) and date('{pboc_corp_pay_jn_end_date}')
                    )
                    group by corp_code
                 """
    execute_index_sql(entry, part1_sql, logger).createOrReplaceTempView("part1")

    part2_sql = f"""
                    select a.corp_code, collect_set(new_cp_payer_name) as cp_payer_names
                    from (
                        select a.corp_code, b.corp_code as relation_corp_code
                        from all_off_line_relations_v a 
                        join all_company_v b 
                        on a.company_rel_degree_1 = b.company_name
                    ) a 
                    join (
                        select distinct corp_code, substring(cp_payer_name, 5, 20) as new_cp_payer_name
                        from pboc_corp_rcv_jn_v 
                        where tx_code in ('622011','622012','622013')
                            and rcv_date between add_months('{pboc_corp_rcv_jn_end_date}', -12) and date('{pboc_corp_rcv_jn_end_date}')
                    ) b 
                    on a.relation_corp_code = b.corp_code
                    group by a.corp_code
                 """
    execute_index_sql(entry, part2_sql, logger).createOrReplaceTempView("part2")

    tmp1_sql = """
                    select a.bbd_company_name as company_name, 
                        a.corp_code, 
                        cp_rcver_names
                    from target_company_v a 
                    left join part1 b 
                    on a.corp_code = b.corp_code
               """
    execute_index_sql(entry, tmp1_sql, logger).createOrReplaceTempView("tmp1")

    tmp_sql = """
                select a.*, b.cp_payer_names
                from tmp1 a 
                left join part2 b 
                on a.corp_code = b.corp_code
              """
    execute_index_sql(entry, tmp_sql, logger).createOrReplaceTempView("tmp")

    # 注册自定义函数
    register_udf(entry)
    sql_str = """
                select company_name, 
                    corp_code, 
                    get_intersection(cp_rcver_names, cp_payer_names) as corp_cp_rel_consistent
                from tmp
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def register_udf(entry: Entry):
    # 求两个list是否有交集
    def get_intersection(list_1, list_2):
        if list_1 is None or list_2 is None:
            return 0
        set_1 = set(list_1)
        set_2 = set(list_2)
        if len(set_1 & set_2) > 0:
            return 1
        else:
            return 0

    entry.spark.udf.register('get_intersection', get_intersection, IntegerType())


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
