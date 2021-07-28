#!usr/bin/env python
# -*- coding: utf8 -*-
"""
@Copyright: 2021, BBD Tech. Cp. Ltd
@Time: 2021/6/24 : 11:24
@Author: wenhao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="import_category_ct",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_impf_v"])
def import_category_ct(entry: Entry, param: Parameter, logger):
    """
    指标计算：进口商品大类计数。
        1. 【进口报关单基本情况表】截取商品编号前两位不同计数；每个公司进口商品大类的种数
        2. 指标计算当天过去一年；
    """
    # 获取各表的最新数据日期
    t_date_dict = param.get("tmp_dict_dict")
    pboc_impf_end_date = t_date_dict.get("pboc_impf")

    temp_sql = f"""
                select corp_code, count(distinct goods_type) as good_type_count
                from (
                    select corp_code, 
                        substring(merch_code, 0, 2) as goods_type 
                    from pboc_impf_v
                    where imp_date between add_months('{pboc_impf_end_date}', -12) 
                                        and date('{pboc_impf_end_date}')
                )
                group by corp_code 
               """
    logger.info(temp_sql)
    entry.spark.sql(temp_sql).createOrReplaceTempView("temp_view")

    sql_str = """
                select company.corp_code, 
                    company.company_name, 
                    coalesce(tmp.good_type_count, 0) as import_category_ct
                from target_company_v company
                left join temp_view tmp
                on company.corp_code = tmp.corp_code
              """
    logger.info(sql_str)
    result_df = entry.spark.sql(sql_str)
    return result_df
