#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 10:07
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="special_import_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_v",
                        "pboc_imp_custom_rpt_v"])
def special_import_ratio(entry: Entry, param: Parameter, logger):
    """
    指标计算: 非特殊监管区企业特殊监管区进口比例。
        1. 【进口报关单基本情况报】非特殊监管区进口成交总价/全部进口成交总价；
        2. 非特殊监管区企业标志，【有效境内非金融机构】是否特殊监管区=0；
        3. 分子计算：【出口报关单基本情况报】海关贸易方式in ('%33','%34')， 分母计算全口径；
        4. 指标计算当天过去一年；
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")

    # 【境内非金融机构】非特殊监管区企业
    temp_company_sql = """
                        select distinct corp_code
                        from pboc_corp_v
                        where is_taxfree = 0
                       """
    logger.info(temp_company_sql)
    entry.spark.sql(temp_company_sql).createOrReplaceTempView("temp_company_view")

    # 【进口报关单基本信息】非特殊监管区全部进口成交总价(计算：非折美元字段)
    all_company_sum_sql = f"""
                            select corp_code, sum(deal_amt) as all_sum_deal
                            from pboc_imp_custom_rpt_v
                            where corp_code in ( select corp_code from temp_company_view ) 
                            and imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12)
                                            and date('{pboc_imp_custom_rpt_end_date}')
                            group by corp_code
                           """
    logger.info(all_company_sum_sql)
    entry.spark.sql(all_company_sum_sql).createOrReplaceTempView("all_company_sum_view")

    # 【进口报关单基本情况报】非特殊监管区进口成交总价(计算：非折美元字段)
    company_sum_sql = f"""
                        select corp_code, sum(deal_amt) as company_sum_deal
                        from pboc_imp_custom_rpt_v
                        where corp_code in ( select corp_code from temp_company_view ) 
                        and imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12)
                                        and date('{pboc_imp_custom_rpt_end_date}')
                        and (custom_trade_mode_code like '%33' or custom_trade_mode_code like '%34')
                        group by corp_code
                       """
    logger.info(company_sum_sql)
    entry.spark.sql(company_sum_sql).createOrReplaceTempView("company_sum_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    case when v2.company_sum_deal is null and v1.all_sum_deal is null then 0
                         else coalesce(v2.company_sum_deal, 0)/coalesce(v1.all_sum_deal, 1)
                    end special_import_ratio
                from target_company_v company
                left join all_company_sum_view v1 on company.corp_code = v1.corp_code
                left join company_sum_view v2 on company.corp_code = v2.corp_code
               """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df