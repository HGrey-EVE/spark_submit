#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 外管局二期--转口贸易--关联企业只做转口贸易
"""

from whetstone.core.entry import Entry
from whetstone.core.index import Parameter, register
from pyspark.sql import DataFrame

'''
1.【外部数据】2度关联法人股东；
2.【非金融机构表】不同企业联系人或法人相同
3. 上述企业仅做转口贸易：有且仅有转口贸易业务
'''


@register(name='rel_only_entreport',
          persistent="parquet",
          persistent_param={"mode": "overwrite"},
          dependencies=['pboc_corp_pay_jn_v', 'pboc_corp_v',
                        'all_off_line_relations_degree2_v', 'all_company_v', 'target_company_v'])
def rel_only_entreport(param: Parameter, entry: Entry):
    t_date_dict = param.get('tmp_date_dict')
    pboc_corp_pay_jn_end_date = t_date_dict.get('pboc_corp_pay_jn')

    # 取出目标企业中属于2度关联方法人姓名
    sql = f'''
            select 
                distinct a.corp_code,
                b.corp_code as relation_corp_code
            from all_off_line_relations_degree2_v a 
            join all_company_v b 
            on a.company_rel_degree_2 = b.company_name

    '''
    sql_excute_newtable_tohdfs(sql=sql, newtablename='rel_only_entreport_tmp1', entry=entry)

    # 【非金融机构表】不同企业联系人或法人相同
    sql2 = f'''
        select 
            distinct a.corp_code,b.corp_code as other_corp_code
        from pboc_corp_v a 
        join 
        pboc_corp_v b 
        on (a.contact_name = b.contact_name or a.frname = b.frname) 
        and a.corp_code <> b.corp_code
    '''
    sql_excute_newtable_tohdfs(sql=sql2, newtablename='rel_only_entreport_tmp2', entry=entry)

    # 仅做转口贸易的公司,表里一家公司会有多条记录
    sql3 = f'''
        select 
        m1.corp_code
        from 
        (select 
        distinct corp_code
        from pboc_corp_pay_jn_v
        where nvl(tx_code,'') <> ''  
        ) m1 
        left join  
        (select 
            corp_code
        from 
            (select 
                corp_code,concat_ws('|',collect_set(tx_code)) as tx_code
            from 
                (select 
                distinct corp_code,tx_code
                from pboc_corp_pay_jn_v
                where nvl(tx_code,'') <> '' 
                and  pay_date between add_months('{pboc_corp_pay_jn_end_date}',-12) and date('{pboc_corp_pay_jn_end_date}')
                ) a  
                group by corp_code
            ) b 
            where tx_code not rlike '122010|121030'
        ) m2 
        on m1.corp_code = m2.corp_code
        where m2.corp_code is null
    '''
    sql_excute_newtable_tohdfs(sql=sql3, newtablename='rel_only_entreport_tmp3', entry=entry)

    sql4 = '''
        select 
        m1.corp_code,
        m1.company_name,
        case when m2.corp_code is not null then 1 else 0 end rel_only_entreport
        from
        target_company_v m1
        left join 
        (select 
            distinct a.corp_code
            from rel_only_entreport_tmp1 a  
            join 
            (select 
                a.corp_code
                from rel_only_entreport_tmp2 a join rel_only_entreport_tmp3 b 
                on a.corp_code = b.corp_code 
                union all 
                select 
                    a.other_corp_code as corp_code
                from rel_only_entreport_tmp2 a join rel_only_entreport_tmp3 b
                on a.other_corp_code = b.corp_code 
            ) b 
            on a.relation_corp_code = b.corp_code
        ) m2 
        on m1.corp_code = m2.corp_code
    '''
    return sql_excute_newtable_tohdfs(sql=sql4, index_info='关联企业只做转口贸易计算完成', entry=entry)


def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        is_cache=False,
        index_info: str = None
):
    entry.logger.info(f'*********************\n当前执行sql为:\n{sql}\n*********************')
    df: DataFrame = entry.spark.sql(sql)
    if index_info:
        entry.logger.info(f"*********************\n{index_info}\n********************")
    if is_cache:
        df.cache()
    if newtablename:
        df.createOrReplaceTempView(f'{newtablename}')
        entry.logger.info(f'*********************\n生成临时表名为:{newtablename}\n********************')

    return df
