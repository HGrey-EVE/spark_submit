#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 11:43
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="import_actual_use",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_imp_custom_rpt_v"])
def import_actual_use(entry: Entry, param: Parameter, logger):
    """
    指标计算：进口实际使用（实际消费）单位计数。
        1. 【进口报关单基本情况表】同一申报企业的所有进口报关单，对其不同收货单位计数；
        2. 指标计算当天过去一年；
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_imp_custom_rpt_end_date = t_date_dict.get("pboc_imp_custom_rpt")

    temp_sql = f"""
                    select corp_code, count(distinct rcv_unit) as rcv_unit_count
                    from pboc_imp_custom_rpt_v
                    where imp_date between add_months('{pboc_imp_custom_rpt_end_date}', -12) 
                                        and date('{pboc_imp_custom_rpt_end_date}')
                    group by corp_code
                """
    logger.info(temp_sql)
    entry.spark.sql(temp_sql).createOrReplaceTempView("temp_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    coalesce(tmp.rcv_unit_count, 0) as import_actual_use
                from target_company_v company
                left join temp_view tmp
                on company.corp_code = tmp.corp_code
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df