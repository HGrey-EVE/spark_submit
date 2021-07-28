#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 11:16
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="special_import_export_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_corp_v",
                        "pboc_imp_custom_rpt_v",
                        "pboc_exp_custom_rpt_v"])
def special_import_export_ratio(entry: Entry, param: Parameter, logger):
    """
    指标计算：非特殊监管区企业特殊监管区进口与出口比例;
        1. 【进口报关单基本情况报】非特殊监管区进口成交总价/【出口报关单基本情况报】非特殊监管区出口成交总价
        2. 指标计算当天过去一年;
        3. 【进口报关单基本情况报】海关贸易方式in ('%33','%34')
           【出口报关单基本情况报】海关贸易方式in ('%33','%34')
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")
    pboc_exp_custom_rpt_end_date = t_date_dict.get("pboc_exp_custom_rpt")

    # 【境内非金融机构】非特殊监管区企业
    temp_company_sql = """
                        select distinct corp_code
                        from pboc_corp_v
                        where is_taxfree = 0
                       """
    logger.info(temp_company_sql)
    entry.spark.sql(temp_company_sql).createOrReplaceTempView("temp_company_view")

    # 非特殊监管区企业进口成交总价(计算：非折美元字段)
    imp_sum_sql = f"""
                    select corp_code, sum(deal_amt) as imp_sum_amt
                    from pboc_imp_custom_rpt_v 
                    where corp_code in (select corp_code from temp_company_view)
                        and imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12)
                                        and date('{pboc_imp_custom_rpt_end_date}')
                        and (custom_trade_mode_code like '%33' or custom_trade_mode_code like '%34')
                    group by corp_code
                   """
    logger.info(imp_sum_sql)
    entry.spark.sql(imp_sum_sql).createOrReplaceTempView("imp_sum_view")

    # 非特殊监管区企业出口成交总价(计算：非折美元字段)
    exp_sum_sql = f"""
                    select corp_code, sum(deal_amt) as exp_sum_amt
                    from pboc_exp_custom_rpt_v 
                    where corp_code in (select corp_code from temp_company_view)
                        and exp_date between add_months('{pboc_exp_custom_rpt_end_date}', -12)
                                        and date('{pboc_exp_custom_rpt_end_date}')
                        and (custom_trade_mode_code like '%33' or custom_trade_mode_code like '%34')
                    group by corp_code
                   """
    logger.info(exp_sum_sql)
    entry.spark.sql(exp_sum_sql).createOrReplaceTempView("exp_sum_view")

    sql_str = """
                select corp_code, 
                    company_name, 
                    imp_sum_amt/exp_sum_amt as special_import_export_ratio
                from (
                    select company.corp_code, 
                        company.company_name, 
                        coalesce(v1.imp_sum_amt, 0) as imp_sum_amt, 
                        case when v2.exp_sum_amt is null or v2.exp_sum_amt = 0 then 1 
                             else v2.exp_sum_amt
                        end exp_sum_amt 
                    from target_company_v company
                    left join imp_sum_view v1 on company.corp_code = v1.corp_code
                    left join exp_sum_view v2 on company.corp_code = v2.corp_code
                )
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
