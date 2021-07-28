#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 10:49
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="basic_multi_phone",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v"])
def basic_multi_phone(entry: Entry, param: Parameter, logger):
    """
    指标：一期开发；
        二期有更新。
        涉及多个公司的联系电话（只要手机）
    """
    # fixme: 手机号的正则匹配规则，可能有问题
    # 【有效境内非金融机构表】、【对公付款】、【对公收入】同一申报人联系电话（剔除座机）对应多家企业；电话只要手机、去空
    temp_sql = """
                select distinct black_corp_code
                from (
                    select explode(corp_codes) as black_corp_code
                    from (
                        select tel, collect_set(corp_code) as corp_codes
                        from (
                            select rpt_tel as tel, corp_code
                            from pboc_corp_pay_jn_v
                            where rpt_tel is not null
                                and rpt_tel rlike '^[1][3456789][0-9]{9}$'
                            union all 
                            select rpt_tel as tel, corp_code
                            from pboc_corp_rcv_jn_v
                            where rpt_tel is not null 
                                and rpt_tel rlike '^[1][3456789][0-9]{9}$'
                            union all 
                            select tel, corp_code
                            from pboc_corp_v
                            where tel is not null
                                and tel rlike '^[1][3456789][0-9]{9}$'
                        )
                        group by tel
                    )
                    where size(corp_codes) >= 2
                )
               """
    logger.info(temp_sql)
    entry.spark.sql(temp_sql).createOrReplaceTempView("temp_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    case when a.black_corp_code is not null then 1
                         else 0
                    end basic_multi_phone
                from target_company_v company
                left join temp_view a on company.corp_code = a.black_corp_code
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
