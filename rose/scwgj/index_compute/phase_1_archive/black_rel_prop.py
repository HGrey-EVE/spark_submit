#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/7 : 15:15
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="black_rel_prop",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "all_off_line_relations_v",
                        "all_company_v",
                        "negative_list_v"])
def black_rel_prop(entry: Entry, param: Parameter, logger):

    # 关联企业为总局负面清单的数量
    c4_sql = """
                select a.corp_code, 
                    count(distinct relation_corp_code) as c4 
                from (
                    select a.corp_code, 
                        b.corp_code as relation_corp_code
                    from all_off_line_relations_v a 
                    join all_company_v b 
                    on a.company_rel_degree_1 = b.company_name
                )a 
                join negative_list_v b 
                on a.relation_corp_code = b.corp_code
                group by a.corp_code
             """
    execute_index_sql(entry, c4_sql, logger).createOrReplaceTempView("view_c4")

    # 关联企业数量
    c3_sql = """
                select corp_code, 
                    count(distinct relation_corp_code) as c3 
                from (
                    select a.corp_code, 
                        b.corp_code as relation_corp_code
                    from all_off_line_relations_v a 
                    join all_company_v b 
                    on a.company_rel_degree_1 = b.company_name
                )
                group by corp_code
             """
    execute_index_sql(entry, c3_sql, logger).createOrReplaceTempView("view_c3")

    # 关联方为负面主体比例
    sql_str = """
                select company.corp_code, 
                    company.company_name,  
                    case when v1.c4 is null and v2.c3 is null then null
                         else coalesce(v1.c4, 1)/coalesce(v2.c3, 1) 
                    end black_rel_prop
                from target_company_v company
                left join view_c4 v1 on company.corp_code = v1.corp_code
                left join view_c3 v2 on company.corp_code = v2.corp_code
              """
    result_df = execute_index_sql(entry, sql_str, logger)

    return result_df


def execute_index_sql(entry: Entry, sql_str: str, logger):
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
