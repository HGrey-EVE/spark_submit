#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 经信厅5+1产业指标计算
"""
from pyspark.sql import SparkSession, DataFrame
import datetime
import os

from whetstone.core.entry import Entry
from jingxinting.proj_common.hive_util import HiveUtil
from jingxinting.proj_common.common_util import save_text_to_hdfs
from jingxinting.proj_common.date_util import DateUtils
from jingxinting.enums.enum_industry import Industry
from jingxinting.biz_base_data.basic_data_proxy import BasicDataProxy
from jingxinting.proj_common.data_output_util import ResultOutputUtil


'''
    提供执行并打印sql，将结果生成临时表和写入hdfs的方法
'''
def sql_excute_newtable_tohdfs(
        sql,
        entry: Entry,
        newtablename=None,
        paquest_path=None,
        repartition_number=100,
        is_cache=False
):

    entry.logger.info(f'当前执行sql为:\n{sql}')

    df: DataFrame = entry.spark.sql(sql)

    if is_cache:
        df.cache()

    if newtablename:
        df.createOrReplaceTempView(f'{newtablename}')
        entry.logger.info(f'生成临时表名为:{newtablename}')

    if paquest_path:
        df.repartition(repartition_number).write.parquet(paquest_path, mode='overwrite')
        # save_text_to_hdfs(df=df, repartition_number=repartition_number, path=paquest_path)
        entry.logger.info(f'写入hdfs地址为{paquest_path}')
        return paquest_path

    return df


class Excute_data:

    def __init__(self,entry: Entry):
        self.savepath = entry.cfg_mgr.hdfs.get_tmp_path("hdfs-biz", "path_5p1_tmp_data")

    # 计算各个层级的投资金额
    # 先整理计算基础表
    def get_five_basic(self, entry: Entry):

        """
        等价于查询所有的16+1和5+1的字段
        """
        columns_all = Industry.all_industry_field()
        columns = ','.join(columns_all)

        """
        等价于 ind_16_1_digital = '1' or ind_16_1_soft = '1' or ind_16_1_bigdata = '1' or ind_16_1_circuit = '1' or 
            ind_16_1_net = '1' or ind_16_1_aviation = '1' or ind_16_1_smart = '1' or ind_16_1_orbital = '1' 
            or ind_16_1_energy = '1' or ind_16_1_farm_prod = '1' or ind_16_1_liquor = '1' or ind_16_1_tea = '1'
            or ind_16_1_medical = '1' or ind_16_1_material = '1' or ind_16_1_clean_energy = '1' or ind_16_1_chemical = '1'
            or ind_16_1_env = '1'
        """
        columns_16p1 = Industry.all_16p1_field()
        columns_2 = " = '1' or ".join(columns_16p1) + " = '1'"

        sql = f'''
            select 
            distinct substring (esdate,1,7) as static_month,company_type,
            bbd_qyxx_id,city_code,key_enterprise,regcap_amount,company_enterprise_status,
            {columns}
            from qyxx_basic 
            where 
            ({columns_2})  
            and company_province = '四川'
        '''

        sql_excute_newtable_tohdfs(sql=sql,newtablename='five_one_basic',is_cache=True,entry=entry)

    # 计算16+1最细维度的指标
    def get_16_1_data_1(self, entry: Entry):

        # 获取16+1对应的枚举值
        # index_list = Industry.all_16p1_field_name_code()
        index_list = [(in_d.field_name,in_d.field_code) for in_d in Industry.IND_16P1_ALL]

        # index_list = [('ind_16_1_digital','0'),('ind_16_1_soft','1'),('ind_16_1_bigdata','2'),
        #               ('ind_16_1_circuit','3'),('ind_16_1_net','4'),('ind_16_1_aviation','5'),
        #               ('ind_16_1_smart','6'),('ind_16_1_orbital','7'),('ind_16_1_energy','8'),
        #               ('ind_16_1_farm_prod','9'),('ind_16_1_liquor','10'),('ind_16_1_tea','11'),
        #               ('ind_16_1_medical','12'),('ind_16_1_material','13'),('ind_16_1_clean_energy','14'),
        #               ('ind_16_1_chemical','15'),('ind_16_1_env','16')]


        for i in index_list:

            # 计算投资金额（注册资本）
            sql = f'''
                select 
                static_month,
                '-1' as industry_5_p_1,
                '{i[1]}' as industry_16_p_1,
                a.key_enterprise as company_type,
                a.city_code as city,
                a.company_type as company_nature,
                sum(regcap_amount) as investment_amount
                from five_one_basic a 
                where {i[0]} = '1'
                and company_enterprise_status like '%%存续%%'
                group by 
                static_month,
                key_enterprise,
                city_code,
                company_type
            '''
            sql_excute_newtable_tohdfs(sql=sql,paquest_path=os.path.join(self.savepath,f"data_16_tzje_{i[1]}"),entry=entry)

            # 计算专利数
            sql = f'''
                select 
                static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,
                sum(patent_num) over(partition by substring(static_month,1,4),industry_5_p_1,industry_16_p_1,company_type,city,company_nature order by static_month) as patent_num
                 from 
                 (select 
                    substring(b.publidate,1,7) as static_month,
                    '-1' as industry_5_p_1,
                    '{i[1]}' as industry_16_p_1,
                    a.key_enterprise as company_type,
                    a.city_code as city,
                    a.company_type as company_nature,
                    count(distinct b.patent_code) as patent_num
                from five_one_basic a 
                inner join 
                patent_info b 
                on a.bbd_qyxx_id = b.bbd_qyxx_id and a.{i[0]} = '1'
                and a.company_enterprise_status like '%%存续%%'
                group by 
                substring(b.publidate,1,7),
                a.city_code,
                a.key_enterprise,
                a.company_type
                ) a 
            '''
            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_16_zls_{i[1]}"),entry=entry)

            # 计算招聘数
            sql = f'''
                select
                substring(b.pubdate,1,7) as static_month,
                '-1' as industry_5_p_1,
                '{i[1]}' as industry_16_p_1,
                a.key_enterprise as company_type,
                a.city_code as city,
                a.company_type as company_nature,
                sum(b.bbd_recruit_num) as recruit_num
            from five_one_basic a 
            inner join 
            manage_recruit b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id and a.{i[0]} = '1'
            and a.company_enterprise_status like '%%存续%%'
            group by 
                substring(b.pubdate,1,7),
                a.city_code,
                a.key_enterprise,
                a.company_type
            '''
            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_16_zps_{i[1]}"),entry=entry)

            # 存续企业数/注销企业数/外迁企业数/计算企业性质分布（数量分布）
            sql = f'''
                select
                     static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,survival_num,cancellation_num,out_num,
                     (cast(survival_num as int)+cast(num2 as int)+cast(num3 as int)) as company_num
                from 
                    (select 
                        static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,
                        sum(survival_num) over(partition by company_type,city,company_nature order by static_month) as survival_num,
                        sum(cancellation_num) over(partition by company_type,city,company_nature order by static_month) as num2,
                        sum(out_num) over(partition by company_type,city,company_nature order by static_month) as num3,
                        cancellation_num,out_num
                    from 
                        (select 
                            static_month,
                            '-1' as industry_5_p_1,
                            '{i[1]}' as industry_16_p_1,
                            a.key_enterprise as company_type,
                            a.city_code as city,
                            a.company_type as company_nature,
                            count(distinct case when company_enterprise_status like '%%存续%%' then bbd_qyxx_id end) as survival_num,
                            count(distinct case when company_enterprise_status like '%%注销%%' then bbd_qyxx_id end) as cancellation_num,
                            count(distinct case when company_enterprise_status like '%%迁出%%' then bbd_qyxx_id end) as out_num
                        from five_one_basic a
                        where {i[0]} = '1' 
                        group by 
                            static_month,
                            key_enterprise,
                            city_code,
                            a.company_type
                        ) m 
                    ) n 
            '''

            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_16_qysl_{i[1]}"),entry=entry)

        return len(index_list)

    # 计算 5+1下的“全部”
    def get_5_1_data_1(self,entry: Entry):

        """
        index_list = [('ind_5_1_digital','0'),('ind_5_1_elec','1'),('ind_5_1_equip','2'),
                      ('ind_5_1_food','3'),('ind_5_1_material','4'),('ind_5_1_energy','5')]
        """
        # index_list = Industry.all_5p1_field_name_code()
        index_list = [(in_re.field_name,in_re.field_code) for in_re in Industry.IND_5P1_ALL]

        for i in index_list:
            # 计算投资金额（注册资本）
            sql = f'''
                select 
                static_month,
                '{i[1]}' as industry_5_p_1,
                'all' as industry_16_p_1,
                a.key_enterprise as company_type,
                a.city_code as city,
                a.company_type as company_nature,
                sum(regcap_amount) as investment_amount
                from five_one_basic a 
                where {i[0]} = '1'
                and company_enterprise_status like '%%存续%%'
                group by 
                static_month,
                key_enterprise,
                city_code,
                a.company_type
            '''
            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_tzje_{i[1]}"),entry=entry)

            # 计算专利数
            sql = f'''
                select 
                static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,
                sum(patent_num) over(partition by substring(static_month,1,4),industry_5_p_1,industry_16_p_1,company_type,city,company_nature order by static_month) as patent_num
                 from
                 (select 
                    substring(b.publidate,1,7) as static_month,
                    '{i[1]}' as industry_5_p_1,
                    'all' as industry_16_p_1,
                    a.key_enterprise as company_type,
                    a.city_code as city,
                    a.company_type as company_nature,
                    count(distinct b.patent_code) as patent_num
                from five_one_basic a 
                inner join 
                patent_info b 
                on a.bbd_qyxx_id = b.bbd_qyxx_id and a.{i[0]} = '1'
                and a.company_enterprise_status like '%%存续%%'
                group by 
                substring(b.publidate,1,7),
                a.city_code,
                a.key_enterprise,
                a.company_type
                ) a 
            '''
            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_zls_{i[1]}"),entry=entry)

            # 计算招聘数
            sql = f'''
                select
                substring(b.pubdate,1,7) as static_month,
                '{i[1]}' as industry_5_p_1,
                'all' as industry_16_p_1,
                a.key_enterprise as company_type,
                a.city_code as city,
                a.company_type as company_nature,
                sum(b.bbd_recruit_num) as recruit_num
            from five_one_basic a 
            inner join 
            manage_recruit b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id and a.{i[0]} = '1'
            and a.company_enterprise_status like '%%存续%%'
            group by 
                substring(b.pubdate,1,7),
                a.city_code,
                a.key_enterprise,
                a.company_type
            '''

            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_zps_{i[1]}"),entry=entry)

            # 存续企业数/注销企业数/外迁企业数/企业性质划分下的企业数
            sql = f'''
                select
                     static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,survival_num,cancellation_num,out_num,
                     (cast(survival_num as int)+cast(num2 as int)+cast(num3 as int)) as company_num
                from 
                (select 
                    static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,
                    sum(survival_num) over(partition by company_type,city,company_nature order by static_month) as survival_num,
                    sum(cancellation_num) over(partition by company_type,city,company_nature order by static_month) as num2,
                    sum(out_num) over(partition by company_type,city,company_nature order by static_month) as num3,
                    cancellation_num,out_num
                from 
                    (select 
                    static_month,
                    '{i[1]}' as industry_5_p_1,
                    'all' as industry_16_p_1,
                    a.key_enterprise as company_type,
                    a.city_code as city,
                    a.company_type as company_nature,
                    count(distinct case when company_enterprise_status like '%%存续%%' then bbd_qyxx_id end) as survival_num,
                    count(distinct case when company_enterprise_status like '%%注销%%' then bbd_qyxx_id end) as cancellation_num,
                    count(distinct case when company_enterprise_status like '%%迁出%%' then bbd_qyxx_id end) as out_num
                    from five_one_basic a 
                    where {i[0]} = '1' 
                    group by 
                    static_month,
                    key_enterprise,
                    city_code,
                    a.company_type
                    ) m 
                ) n 
            '''

            sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_qysl_{i[1]}"),entry=entry)

        return len(index_list)

    # 总量的“全部”计算
    def get_all_data_1(self, entry: Entry):
        # 计算投资金额（注册资本）
        sql = f'''
            select 
            static_month,
            'all' as industry_5_p_1,
            '-1' as industry_16_p_1,
            a.key_enterprise as company_type,
            a.city_code as city,
            a.company_type as company_nature,
            sum(regcap_amount) as investment_amount
            from five_one_basic a 
            where company_enterprise_status like '%%存续%%'
            group by 
            static_month,
            key_enterprise,
            city_code,
            a.company_type
        '''
        sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_tzje_all"),entry=entry)

        # 计算专利数
        sql = f'''
            select 
            static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,
            sum(patent_num) over(partition by substring(static_month,1,4),industry_5_p_1,industry_16_p_1,company_type,city,company_nature order by static_month) as patent_num
             from
             (select 
                substring(b.publidate,1,7) as static_month,
                'all' as industry_5_p_1,
                '-1' as industry_16_p_1,
                a.key_enterprise as company_type,
                a.city_code as city,
                a.company_type as company_nature,
                count(distinct b.patent_code) as patent_num
            from five_one_basic a  
            inner join 
            patent_info b 
            on a.bbd_qyxx_id = b.bbd_qyxx_id
            and a.company_enterprise_status like '%%存续%%'
            group by 
            substring(b.publidate,1,7),
            a.city_code,
            a.key_enterprise,
            a.company_type
            ) a 
        '''
        sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_zls_all"),entry=entry)

        # 计算招聘数
        sql = f'''
            select
            substring(b.pubdate,1,7) as static_month,
            'all' as industry_5_p_1,
            '-1' as industry_16_p_1,
            a.key_enterprise as company_type,
            a.city_code as city,
            a.company_type as company_nature,
            sum(b.bbd_recruit_num) as recruit_num
        from five_one_basic a 
        inner join 
        manage_recruit b 
        on a.bbd_qyxx_id = b.bbd_qyxx_id
        and a.company_enterprise_status like '%%存续%%'
        group by 
            substring(b.pubdate,1,7),
            a.city_code,
            a.key_enterprise,
            a.company_type
        '''
        sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_zps_all"),entry=entry)

        # 存续企业数/注销企业数/外迁企业数/企业性质分类数量
        sql = f'''
            select 
                static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,survival_num,cancellation_num,out_num,
                (cast(survival_num as int)+cast(num2 as int)+cast(num3 as int)) as company_num
            from 
            (select 
                static_month,industry_5_p_1,industry_16_p_1,company_type,city,company_nature,
                sum(survival_num) over(partition by company_type,city,company_nature order by static_month) as survival_num,
                sum(cancellation_num) over(partition by company_type,city,company_nature order by static_month) as num2,
                sum(out_num) over(partition by company_type,city,company_nature order by static_month) as num3,
                cancellation_num,out_num
            from 
                (select 
                    static_month,
                    'all' as industry_5_p_1,
                    '-1' as industry_16_p_1,
                    a.key_enterprise as company_type,
                    a.city_code as city,
                    a.company_type as company_nature,
                    count(distinct case when company_enterprise_status like '%%存续%%' then bbd_qyxx_id end) as survival_num,
                    count(distinct case when company_enterprise_status like '%%注销%%' then bbd_qyxx_id end) as cancellation_num,
                    count(distinct case when company_enterprise_status like '%%迁出%%' then bbd_qyxx_id end) as out_num
                from five_one_basic a 
                group by 
                    static_month,
                    key_enterprise,
                    city_code,
                    a.company_type
                ) m 
            ) n 
        '''

        sql_excute_newtable_tohdfs(sql=sql, paquest_path=os.path.join(self.savepath,f"data_5_qysl_all"),entry=entry)


class Merge_data:

    def get_basic_info(self, entry: Entry):

        s = BasicDataProxy(entry=entry)

        # 基础表
        basic_df = s.get_qyxx_basic_df()
        basic_df.createOrReplaceTempView('qyxx_basic')

        # 招聘表
        recruit_df = s.get_recruit_info_df()
        recruit_df.cache().createOrReplaceTempView('manage_recruit')

        # 专利表
        patent_df = s.get_patent_info_df()
        patent_df.cache().createOrReplaceTempView('patent_info')

    def exec_merge(self, entry: Entry):
        S = Excute_data(entry=entry)
        S.get_five_basic(entry=entry)
        index_count_16 = S.get_16_1_data_1(entry=entry)
        index_count_5 = S.get_5_1_data_1(entry=entry)
        S.get_all_data_1(entry=entry)

        # 不跑sql逻辑只执行合并时候则替换上面
        # index_count_16 = 17
        # index_count_5 = 6

        # 分块读取数据合并
        # 关联条件
        join_cond = ["static_month", "company_nature", "city","company_type","industry_5_p_1","industry_16_p_1"]

        # 合并16+1最细维度部分的指标
        paquests_16 = ['data_16_qysl_','data_16_zps_','data_16_zls_','data_16_tzje_']

        tmp_write_path_16p1 = os.path.join(S.savepath, 'write_16p1')
        tmp_dump = os.path.join(S.savepath, 'dump_16p1')

        def save_tmp_result(df, write_path, dump_path):
            entry.spark.read.parquet(write_path).union(df).write.parquet(dump_path, mode='overwrite')
            entry.spark.read.parquet(dump_path).write.parquet(write_path, mode='overwrite')

        for i in range(index_count_16):
            path1 = paquests_16[0] + f'{i}'
            data1: DataFrame = entry.spark.read.parquet(os.path.join(S.savepath, path1))
            for paquest in paquests_16[1:]:
                path2 = paquest + f'{i}'
                data2 = entry.spark.read.parquet(os.path.join(S.savepath, path2))
                data1 = data1.join(data2, join_cond, "left").fillna(0)
            if i == 0:
                df_1 = data1
                df_1.write.parquet(tmp_write_path_16p1, mode='overwrite')
            else:
                save_tmp_result(data1, tmp_write_path_16p1, tmp_dump)
                # df_1 = df_1.union(data1)
            entry.logger.info(f'write {path1} success')

        df_16_1 = entry.spark.read.parquet(tmp_write_path_16p1).select('static_month',
                            'industry_5_p_1',
                            'industry_16_p_1',
                            'company_type',
                            'city',
                            'company_nature',
                            'investment_amount',
                            'patent_num',
                            'recruit_num',
                            'company_num',
                            'survival_num',
                            'cancellation_num',
                            'out_num')


        # df_16_1.repartition(100).write.parquet(os.path.join(S.savepath,'result_16p1'), mode='overwrite')

        # 合并5+1粒度下“全部”的指标
        paquests_5 = ['data_5_qysl_', 'data_5_zps_', 'data_5_zls_', 'data_5_tzje_']
        tmp_write_path_5p1 = os.path.join(S.savepath, 'write_5p1')
        tmp_dump = os.path.join(S.savepath, 'dump_5p1')

        for i in range(index_count_5):
            path1 = paquests_5[0] + f'{i}'
            data1: DataFrame = entry.spark.read.parquet(os.path.join(S.savepath, path1))
            for paquest in paquests_5[1:]:
                path2 = paquest + f'{i}'
                data2 = entry.spark.read.parquet(os.path.join(S.savepath, path2))
                data1 = data1.join(data2, join_cond, "left").fillna(0)
            if i == 0:
                df_1 = data1
                df_1.write.parquet(tmp_write_path_5p1,mode='overwrite')
            else:
                # df_1 = df_1.union(data1)
                save_tmp_result(data1, tmp_write_path_5p1, tmp_dump)
            entry.logger.info(f"save {path1} success")
        df_5_1 = entry.spark.read.parquet(tmp_write_path_5p1).select('static_month',
                            'industry_5_p_1',
                            'industry_16_p_1',
                            'company_type',
                            'city',
                            'company_nature',
                            'investment_amount',
                            'patent_num',
                            'recruit_num',
                            'company_num',
                            'survival_num',
                            'cancellation_num',
                            'out_num')

        # df_5_1.repartition(100).write.parquet(os.path.join(S.savepath, 'result_5p1'), mode='overwrite')


        # 合并总量部分的指标
        paquests_all = ['data_5_zps_all','data_5_zls_all','data_5_tzje_all','data_5_qysl_all']

        # df1: DataFrame = entry.spark.read.parquet(S.saveos.path.join(paquests_all.pop()))
        df1: DataFrame = entry.spark.read.parquet(os.path.join(S.savepath, paquests_all.pop()))
        for paquest in paquests_all:
            # df2 = entry.spark.read.parquet(S.saveos.path.join(paquest))
            df2 = entry.spark.read.parquet(os.path.join(S.savepath, paquest))
            df1 = df1.join(df2, join_cond, 'left').fillna(0)
        df_all = df1.select('static_month',
                            'industry_5_p_1',
                            'industry_16_p_1',
                            'company_type',
                            'city',
                            'company_nature',
                            'investment_amount',
                            'patent_num',
                            'recruit_num',
                            'company_num',
                            'survival_num',
                            'cancellation_num',
                            'out_num')

        df_all.write.parquet(os.path.join(S.savepath, 'result_zongji'), mode='overwrite')

        # 再将三部分的结果合并成一个df
        result_1: DataFrame = df_16_1.union(df_5_1)
        result_1.write.parquet(os.path.join(S.savepath, 'result_all_1'), mode='overwrite')

        # result: DataFrame = result_1.union(df_16_1)
        # 合并没数据，改成去读取后再合并
        df1 = entry.spark.read.parquet(os.path.join(S.savepath, 'result_all_1'))
        df2 = entry.spark.read.parquet(os.path.join(S.savepath, 'result_zongji'))
        result: DataFrame = df2.union(df1)
        # result.repartition(100).write.parquet(os.path.join(S.savepath, 'result_all'), mode='overwrite')

        return result


def pre_check(entry: Entry):
    # todo 运行前检查：如依赖的数据是否存在等等检查
    return True


def post_check(entry: Entry):
    return True


def main(entry: Entry):

    m = Merge_data()
    m.get_basic_info(entry=entry)

    result = m.exec_merge(entry=entry)

    # 处理结果
    r = ResultOutputUtil(entry=entry, table_name='monthly_info_industry_5p1', month_value_fmt='%Y-%m')
    r.save(result)


