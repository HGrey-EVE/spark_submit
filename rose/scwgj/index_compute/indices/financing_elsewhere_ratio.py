#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 
:Author: zhouchao@bbdservice.com
:Date: 2021-06-23 14:44
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="financing_elsewhere_ratio",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v", "pboc_dfxloan_sign_v"])
def financing_elsewhere_ratio(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_date = t_date_dict.get('pboc_dfxloan_sign')
    # 贸易融资债务人所属外汇局代码与债权人所属外汇局代码不同（用前两位外汇局代码比较）的笔数
    sql = f'''
        select corp_code, count(distinct guar_biz_no) as c1
          from (
                select crop_code, guar_biz_no 
                  from pboc_dfxloan_sign_v
                 where twhere domfx_loan_type_code = '1102'
                 and substring(safecode, 1, 2) != substring(bank_safecode, 1, 2)
                 and pay_date between add_months('{t_date}', -12) and date('{t_date}')
                )
         group by corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 贸易融资笔数
    sql = f"""
        select corp_code, count(distinct guar_biz_no) as c2 
        from (
            select crop_code, guar_biz_no  
            from pboc_dfxloan_sign_v 
            where domfx_loan_type_code = '1102'
                 and pay_date between add_months('{t_date}', -12) and date('{t_date}')
            )
        group by corp_code
        """
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")

    # 贸易融资异地办理比例
    sql = """
            select 
                company.corp_code 
                ,company.company_name 
                ,case when v1.c1 is null and v2.c2 is null then null 
                else coalesce(v1.c1, 1)/coalesce(v2.c2, 1) end financing_elsewhere_ratio
            from target_company_v company
            left join view1 v1 on v1.corp_code = company.corp_code
            left join view2 v2 on v2.corp_code = company.corp_code
            """
    logger.info(sql)
    df = entry.spark.sql(sql)

    return df
