#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/6/8 : 13:40
@Author: zhouchao@bbdservice.com
"""
from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register


@register(name="black_suspect_shell",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["pboc_corp_v",
                        "target_company_v"])
def black_suspect_shell(entry: Entry, param: Parameter, logger):

    sql =  '''
        select a.bbd_company_name as company_name, a.corp_code,
               case when flag is not null then flag
                else 0 end black_suspect_shell
          from target_company_v a
          left join (
                     select corp_code,
                            case when frname=contact_name then 1
                            else 0 end flag
                        from pboc_corp_v
                    )b
         on a.corp_code = b.corp_code'''

    logger.info(sql)
    df = entry.spark.sql(sql)

    return df


