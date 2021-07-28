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


@register(name="service_foreign_rel_largepay",
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=["target_company_v",
                        "pboc_fdi_sh_v",
                        "pboc_odi_inv_limit_v",
                        "pboc_corp_pay_jn_v",
                        "pboc_corp_rcv_jn_v",
                        "pboc_ovs_corp_v"
                        ])
def service_foreign_rel_largepay(entry: Entry, param: Parameter, logger):
    t_date_dict = param.get('tmp_date_dict')
    t_pay_date = t_date_dict.get('pboc_corp_pay_jn')
    t_rcv_date = t_date_dict.get('pboc_corp_pay_jn')
    # 境内企业的境外关联公司
    sql = f'''
        select fdi.pty_code as corp_code, upper(substring(ovs.pty_name, 1, 15)) as pty_name
        from pboc_fdi_sh_v fdi
        join pboc_ovs_corp_v ovs
        on fdi.sh_pty_code = ovs.pty_code 
        union 
        select odi.crop_code as corp_code, upper(substring(ovs.pty_name, 1, 15)) as pty_name 
        from pboc_odi_inv_limit_v odi
        join pboc_ovs_corp_v ovs 
        on odi.obj_pty_code = ovs.pty_code 
        union 
        select corp_code, substring(counter_party, 1, 15) as pty_name
        from pboc_corp_pay_jn_v 
        where tx_code in ('621011','621012','621013')  
        and pay_date between add_months('{t_pay_date}', -12) and date('{t_pay_date}')
        union 
        select corp_code, substring(counter_party, 1, 15) as pty_name
        from pboc_corp_rcv_jn_v 
        where tx_code in ('622011','622012','622013') 
        and rcv_date between add_months('{t_rcv_date}', -12) and date('{t_rcv_date}')
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view1")
    # 交易对手
    sql = f'''
        select corp_code, tx_amt_usd, substring(counter_party, 1, 15) as pty_name, rptno  
        from pboc_corp_pay_jn_v 
        where tx_code like '2%'
        and pay_date between add_months('{t_pay_date}', -12) and date('{t_pay_date}')
            '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view2")

    # 服贸向境外关联企业付款
    sql = f'''
        select tx_amt_usd service_foreign_rel_largepay_value, 
                corp_code, pty_name  
        from
            (select v2.*    
            from view1 v1 
            join view2 v2
            on v1.pty_name = v2.pty_name and v1.corp_code = v2.corp_code)
        group by corp_code, pty_name
            '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    df.createOrReplaceTempView("view3")
    # 大额
    sql = f'''
        select company.corp_code, 
               company.company_name, 
               v3.pty_name, 
                v3.service_foreign_rel_largepay_value, 
               case when v3.service_foreign_rel_largepay_value is null then null 
                when v3.service_foreign_rel_largepay_value > 1000 then 1  
                else 0 
                end service_foreign_rel_largepay
        from target_company_v company 
        left join view3 v3 
        on company.corp_code = v3.crop_code
        '''
    logger.info(sql)
    df = entry.spark.sql(sql)
    return df
