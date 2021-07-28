#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 18:05
@Author: wenhao@bbdservice.com
"""
from pyspark.sql.types import IntegerType

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="corp_cp_guar_consistent",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_con_exguaran_new_v"])
def corp_cp_guar_consistent(entry: Entry, param: Parameter, logger):
    # 预付交易对手是否与内保外贷境外债务人名称一致
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_corp_pay_jn')
    t_con_date = t_date_dict.get('pboc_con_exguaran_new')
    part1_sql = f"""
                    select corp_code, pay_date, new_cp_rcver_name
                    from (
                        select distinct corp_code, pay_date, upper(substring(cp_rcver_name, 1, 15)) as new_cp_rcver_name
                        from pboc_corp_pay_jn_v 
                        where rcvpay_attr_code = 'A'
                            and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                    )
                    group by corp_code
                 """
    execute_index_sql(entry, part1_sql, logger).createOrReplaceTempView("part1")

    part2_sql = f"""
            select distinct p1.corp_code
            from part1 p1
            join (select corp_code, upper(substring(guednameen0, 1, 15)) as new_guednameen0
                    from pboc_con_exguaran_new_v 
                    where contractdate between add_months('{t_con_date}', -12) and date('{t_con_date}')
                    and upper(countryname0) != 'CNH') exguaran
            on p1.corp_code = exguaran.corp_code 
            and p1.new_cp_rcver_name = exguaran.new_guednameen0
            and exguaran.maturity between p1.pay_date and add_months(p1.pay_date, 1)
        """

    execute_index_sql(entry, part2_sql, logger).createOrReplaceTempView("part2")

    sql_str = """
                select company.corp_code, company.company_name, 
                       case p2.corp_code is null then 0
                       else 1
                       end corp_cp_guar_consistent
                from target_company_v company 
                left join part2 p2
                on company.corp_code = p2.corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)
    return result_df

def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
