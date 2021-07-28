#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 17:22
@Author: wenhao@bbdservice.com
"""
from pyspark.sql.types import IntegerType

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_prepay_capinc_same_cp",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def corp_prepay_capinc_same_cp(entry: Entry, param: Parameter, logger):
    # 预付对方交易对手名称与资本金收入对方交易对手名称是否一致
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_rcv_date = t_date_dict.get('pboc_corp_rcv_jn')

    part1_sql = f"""
                    select corp_code, collect_set(new_cp_rcver_name) as cp_rcver_names
                    from (
                        select distinct corp_code, substring(cp_rcver_name, 1, 15) as new_cp_rcver_name
                        from pboc_corp_pay_jn_v 
                        where rcvpay_attr_code = 'A'
                            and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                    )
                    group by corp_code
                 """
    execute_index_sql(entry, part1_sql, logger).createOrReplaceTempView("part1")

    part2_sql = f"""
                    select corp_code, collect_set(new_cp_payer_name) as cp_payer_names
                    from (
                        select distinct corp_code, substring(cp_payer_name, 1, 15) as new_cp_payer_name
                        from pboc_corp_rcv_jn_v 
                        where tx_code in ('622011','622012','622013')
                            and rcv_date between add_months('{t_rcv_date}', -12) and date('{t_rcv_date}')
                    )
                    group by corp_code
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
                    get_intersection(cp_rcver_names, cp_payer_names) as corp_prepay_capinc_same_cp
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
