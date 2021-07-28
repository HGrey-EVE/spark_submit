#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="export_higher_decleared",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["pboc_expf_v",
                        "target_company_v"])
def export_higher_decleared(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get("tmp_date_dict")
    pboc_expf_end_date = t_date_dict.get("pboc_expf")


    sql =  f'''
        select a.bbd_company_name as company_name, a.corp_code,
               case when b.corp_code is not null then 1
                    else 0 end export_higher_decleared
          from target_company_v a
          left join (
                    select distinct corp_code
                      from (
                            select a.corp_code, company_avg / sp_avg as tmp_prop
                              from (
                                    select corp_code, merch_code, unit_code, avg(prod_deal_amt_usd) as company_avg
                                      from (
                                            select corp_code, merch_code, unit_code, prod_deal_amt_usd
                                              from pboc_expf_v
                                             where exp_date between add_months('{pboc_expf_end_date}', -12) and date('{pboc_expf_end_date}')
                                            )
                                     group by corp_code, merch_code, unit_code
                                    )a
                              join (
                                    select merch_code, unit_code, avg(prod_deal_amt_usd) as sp_avg
                                      from (
                                            select merch_code, unit_code, prod_deal_amt_usd
                                              from pboc_expf_v
                                             where exp_date between add_months('{pboc_expf_end_date}', -12) and date('{pboc_expf_end_date}')
                                            )
                                     group by merch_code, unit_code
                                    )b
                             on a.merch_code = b.merch_code and a.unit_code = b.unit_code
                            )
                     where tmp_prop > 3
                    )b
         on a.corp_code = b.corp_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)

    return df


