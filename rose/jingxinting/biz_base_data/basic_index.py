#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: basic_index
:Author: xufeng@bbdservice.com 
:Date: 2021-05-26 3:20 PM
:Version: v.1.0
:Description: generate basic table, and save table data to hdfs
    qyxx_basic,
    patent_info,
    recruit_info,
    park_info
"""
import os
import traceback

from pyspark.sql.dataframe import DataFrame

from whetstone.core.entry import Entry
from jingxinting.proj_common.hive_util import HiveUtil
from jingxinting.proj_common.common_util import put_source_file_to_hdfs
from jingxinting.proj_common.data_output_util import ResultOutputUtil
from jingxinting.enums.enum_industry import Industry


class BasicIndex:

    def __init__(self, entry: Entry):
        self.entry = entry
        self.spark = entry.spark
        self.logger = entry.logger
        self.debug = entry.cfg_mgr.get('exec-mode', 'debug')

        self.db_name = entry.cfg_mgr.get('hive-dw', 'database')
        self._basic_data_path = self._get_result_data_path('qyxx_basic')
        self._patent_data_path = self._get_result_data_path('patent_info')
        self._recruit_data_path = self._get_tmp_data_path('recruit_info')  # 招聘数据不对外输出
        self._park_data_path = self._get_result_data_path('park_info')
        self._park_qyxx_path = self._get_tmp_data_path('park_qyxx_info')  # 关系数据不对外输出
        self._local_source_path_fmt = self.entry.cfg_mgr.get('biz', 'local_path') + "/source_data_outer/{}.csv"

    def execute(self):

        # 基础数据准备，生成园区信息和企业基础数据，不捕获错误，报错需直接退出！
        self.logger.info("generate park info")
        self._generate_park_info()

        self.logger.info("generate basic info")
        self._generate_basic_data()

        self.logger.info("basic data prepare success!")

        funcs = self._get_all_gen_func()

        for func in funcs:
            func_name = func.__name__
            try:
                self.logger.info(f"begin exec func {func_name}")
                func()
                self.logger.info(f"exec func {func_name} success")
            except Exception:
                self.logger.error(f"exec {func_name} failed:{traceback.format_exc()}")
                if not self.debug:
                    raise

    def get_qyxx_basic_df(self) -> DataFrame:
        return self.spark.read.parquet(self._basic_data_path)

    def get_patent_info_df(self) -> DataFrame:
        return self.spark.read.parquet(self._patent_data_path)

    def get_recruit_info_df(self) -> DataFrame:
        return self.spark.read.parquet(self._recruit_data_path)

    def get_park_info_df(self) -> DataFrame:
        return self.spark.read.parquet(self._park_data_path)

    def get_company_tag_info(self) -> DataFrame:
        """
        企业标签信息由算法程序计算，目前放到项目的 biz_company_tags 模块
        这里提供统一的对外访问出口
        :return:
        """
        data_share_dir = self.entry.cfg_mgr.hdfs.get_fixed_path('hdfs-biz', 'data_share_path')
        tag_info_path = os.path.join(data_share_dir, 'company_tag_info')
        return self.spark.read.json(tag_info_path)

    def _get_all_gen_func(self):
        """
        各个df生成有前后依赖，需要注意顺序!
        :return:
        """
        return [
            self._generate_patent_info,
            self._generate_recruit_info,
        ]

    def _generate_basic_data(self):
        """
        基础信息表生成
        :return:
        """

        # 从数仓抽取相关基础数据并落盘
        basic_df = self._gen_basic_table_df()
        basic_table_v1 = 'qyxx_basic_v1'
        tmp_basic_path = self._get_tmp_data_path(basic_table_v1)
        basic_df.drop_duplicates(['bbd_qyxx_id']).repartition(1000).write.parquet(tmp_basic_path, mode='overwrite')
        self.logger.info("write qyxx_basic_v1 to hdfs success")

        tags_df = self._prepare_tags_info()
        self.spark\
            .read\
            .parquet(tmp_basic_path)\
            .join(tags_df, 'bbd_qyxx_id', 'left')\
            .drop(tags_df.bbd_qyxx_id)\
            .write\
            .parquet(self._basic_data_path, mode='overwrite')
        self.logger.info("write qyxx_basic to hdfs success")
        self._save_result('qyxx_basic')

    def _prepare_tags_info(self) -> DataFrame:
        """生成标签信息
        原始表字段: bbd_qyxx_id, ind_16p1, parent_tag, tag, tag_level
        +--------------------------------+--------+------------+------------+---------+
        |bbd_qyxx_id                     |ind_16p1|parent_tag  |tag         |tag_level|
        +--------------------------------+--------+------------+------------+---------+
        |1253f1273a6a4c3698cd86cc76b8ef2a|1       |信息技术服务|信息技术咨询     |2        |
        |6d0233be4c4541fe946264b70cc373bf|1       |信息技术服务|信息技术咨询     |2        |
        |3d8a338e6f544b988a2809d181f8d769|1       |信息技术服务|信息技术咨询     |2        |
        |ad52b5c209a9472c893ff954bd6eb1bb|1       |信息技术服务|信息技术咨询     |2        |
        +--------------------------------+--------+------------+------------+---------+
        通过该表生成　所有　5+1　标签，16+1　标签，以及产品标签
        :return:
        """
        def row_map(row):
            row_dict = row.asDict()
            return row_dict['bbd_qyxx_id'], row_dict

        tags_df = self.get_company_tag_info()
        gen_tags_func = self._gen_tags_mapping
        ret_df = tags_df\
            .select(["bbd_qyxx_id", "ind_16p1", "tag"])\
            .rdd\
            .map(row_map)\
            .groupByKey()\
            .map(lambda x: gen_tags_func(x[1]))\
            .toDF()
        return ret_df

    @staticmethod
    def _gen_tags_mapping(kv_lst):
        """
        kv_lst每个元素结构:
            {
                'bbd_qyxx_id': xxx,
                'ind_16p1': xxx,
                'tag': xxx
            }
        :param kv_lst:
        :return:
        """
        rows = list(kv_lst)
        bbd_qyxx_id = rows[0]['bbd_qyxx_id']

        ret_dict = {ind.field_name: '0' for ind in Industry.IND_ALL}
        ret_dict['bbd_qyxx_id'] = bbd_qyxx_id
        tags = []
        lst_16p1_code = []

        for kv in rows:
            tag = kv['tag']
            if tag:
                tags.append(kv['tag'])
            lst_16p1_code.append(kv['ind_16p1'])

        update_dict = Industry.ind_5p1_16p1_code_mapping(lst_16p1_code)
        ret_dict.update(update_dict)
        ret_dict['company_product_tags'] = ','.join(tags) if tags else ''

        return ret_dict

    def _generate_patent_info(self):
        """
        专利信息提取
        :return:
        """
        table_name = f'{self.db_name}.prop_patent_data'

        columns = """
            L.bbd_qyxx_id, L.publidate, L.patent_type,
            L.title, L.public_code as patent_code, L.application_date
        """

        self._gen_biz_data(table_name, self._patent_data_path, columns)
        self._save_result('patent_info')

    def _generate_recruit_info(self):
        """
        招聘信息提取
        :return:
        """
        table_name = f'{self.db_name}.manage_recruit'
        self._gen_biz_data(table_name, self._recruit_data_path)

    def _generate_park_info(self):
        """
        园区信息提取
        :return:
        """
        # 园区信息和园区企业关系信息 csv 放入hdfs
        tmp_park_info_path = self._get_source_date_path('park_info')
        tmp_park_qyxx_path = self._get_source_date_path('park_qyxx_info_source')
        self._put_park_info_to_hdfs(tmp_park_info_path, tmp_park_qyxx_path)

        # 从hdfs读取园区信息和园区企业关系信息,并处理好园区名并做好园区的关联
        park_v = "park_source_v"
        park_qyxx_v = "park_qyxx_v"
        self.spark.read.csv(tmp_park_info_path, header=True, sep='\t').createOrReplaceTempView(park_v)
        self.spark.read.csv(tmp_park_qyxx_path, header=True, sep='\t').createOrReplaceTempView(park_qyxx_v)

        # 园区企业关系信息加入园区id，并放入hdfs
        self.spark.sql(f"""
            SELECT 
                L.*, R1.park_id
            FROM 
                {park_qyxx_v} L 
            LEFT JOIN 
                {park_v} R1 ON L.park_name = R1.park_name
        """).write.parquet(self._park_qyxx_path, mode='overwrite')
        self.logger.info("park_qyxx info load to hdfs success")

        # 园区信息处理5+1和16+1产业标签，并放入hdfs
        df_park = self.spark.sql(f"""select * from {park_v}""")
        df_park.rdd.map(self._industry_mapping).toDF()\
            .repartition(1).write.parquet(self._park_data_path, mode='overwrite')

        self._save_result('park_info')
        self.logger.info("park info load to hdfs success")

    def _save_result(self, table_name):
        out_put_util = ResultOutputUtil(self.entry, table_name, is_statistic=False, need_write_result=False)
        out_put_util.save()

    def _put_park_info_to_hdfs(self, tmp_park_info_path, tmp_park_qyxx_path):
        """
        上传文件到hdfs，注意，上传的文件名会写死!
        :param tmp_park_info_path:
        :param tmp_park_qyxx_path:
        :return:
        """
        park_info_local_path = os.path.abspath(self._local_source_path_fmt.format('park_info'))
        park_company_info_path = os.path.abspath(self._local_source_path_fmt.format('park_company_info'))
        put_source_file_to_hdfs(park_info_local_path, tmp_park_info_path)
        put_source_file_to_hdfs(park_company_info_path, tmp_park_qyxx_path)

    @staticmethod
    def _industry_mapping(row):
        ret = row.asDict()
        ind_mapping = Industry.ind_5p1_16p1_in_mapping(ret['info_16_p_1'])
        ret.update(ind_mapping)
        print(f"ret data is:{ret['info_16_p_1']}:{ind_mapping}")
        return ret

    def _create_county_view(self, view_name):
        """
        去重后的地区表
        :return:
        """
        table_name = f'{self.db_name}.company_county'
        county_dt = HiveUtil.newest_partition_with_data(self.spark, table_name)

        sql = f"""
            SELECT 
                * 
            FROM 
                (SELECT 
                    *,
                    row_number() over (partition by code order by tag) as rank 
                FROM {table_name} 
                WHERE dt = '{county_dt}' and tag!=-1) a 
            WHERE rank=1
        """
        self.logger.info(f"county sql is:{sql}")
        self.spark.sql(sql).createOrReplaceTempView(view_name)

    def _get_park_qyxx_info(self) -> DataFrame:
        return self.spark.read.parquet(self._park_qyxx_path)

    def _gen_basic_table_df(self) -> DataFrame:
        basic_table = f'{self.db_name}.qyxx_basic'
        basic_dt = HiveUtil.newest_partition(self.spark, basic_table)

        contact_table = f'{self.db_name}.contact_info'
        contact_dt = HiveUtil.newest_partition(self.spark, contact_table)

        county_view = 'v_county'
        self._create_county_view(county_view)

        park_qyxx_view = 'v_park_qyxx_info'
        self._get_park_qyxx_info().drop_duplicates(['company_name']).createOrReplaceTempView(park_qyxx_view)

        tmp_gazelle_path = self._get_source_date_path('gazelle')
        tmp_key_enterprise_path = self._get_source_date_path('key_enterprise')
        tmp_little_giant_path = self._get_source_date_path('little_giant')
        tmp_unicorns_path = self._get_source_date_path('unicorns')

        self._put_company_tag_info_to_hdfs(
            tmp_gazelle_path, tmp_key_enterprise_path, tmp_little_giant_path, tmp_unicorns_path)

        gazelle_view = 'v_gazelle'
        self._create_company_tag_view(tmp_gazelle_path, gazelle_view)
        key_enterprise_view = 'v_key_enterprise'
        self._create_company_tag_view(tmp_key_enterprise_path, key_enterprise_view)
        little_giant_view = "v_little_giant"
        self._create_company_tag_view(tmp_little_giant_path, little_giant_view)
        unicorns_view = "v_unicorns"
        self._create_company_tag_view(tmp_unicorns_path, unicorns_view)

        df_basic = self.spark.sql(
            f"""
            SELECT 
                L.bbd_qyxx_id,
                L.company_name,
                L.address,
                L.company_industry,
                L.company_companytype,
                CASE 
                    WHEN L.company_type like '%国有%' THEN '国有投资企业'
                    WHEN L.company_type like '%合%' THEN '合资企业'
                    WHEN L.company_type like '%港%' THEN '港澳台投资企业'
                    WHEN L.company_type like '%外%' THEN '外资企业'
                    ELSE '民资企业'
                END as company_type,
                L.regcap_amount / 10000 as company_scale,
                L.company_enterprise_status as company_enterprise_status,
                L.company_province as company_province,
                '510000' as province_code,
                R1.city as company_city,
                CONCAT(SUBSTR(L.company_county, 1, 4), '00') as city_code,
                L.frname_id as company_legal_person_id,
                L.frname as company_legal_person_name,
                L.regcap_amount / 10000 as regcap_amount,
                if(ifnull(L.ipo_company, '0') in('null', '0'), '0', '1') as ipo_company,
                L.esdate,
                L.cancel_date,
                R2.contact_info as contact_info,
                L.operate_scope,
                R3.park_id as park_id,
                if(ifnull(R4.company_name, '0') == '0', '0', '1') as gazelle,
                if(ifnull(R5.company_name, '0') == '0', '0', '1') as key_enterprise,
                if(ifnull(R6.company_name, '0') == '0', '0', '1') as little_giant,
                if(ifnull(R7.company_name, '0') == '0', '0', '1') as unicorns,
                if(L.company_name in(R4.company_name, R5.company_name, R6.company_name, R7.company_name), '0', '1') as common_enterprise
            FROM 
                {basic_table} L
            LEFT JOIN {county_view} R1 ON L.company_county = R1.code
            LEFT JOIN ( SELECT * 
                        FROM {contact_table} 
                        WHERE dt = '{contact_dt}' 
                            AND contact_info is not null
                            AND contact_way = 'phone') R2 ON L.bbd_qyxx_id = R2.bbd_qyxx_id
            LEFT JOIN {park_qyxx_view} R3 ON L.company_name = R3.company_name
            LEFT JOIN {gazelle_view} R4 ON L.company_name = R4.company_name
            LEFT JOIN {key_enterprise_view} R5 ON L.company_name = R5.company_name
            LEFT JOIN {little_giant_view} R6 ON L.company_name = R6.company_name
            LEFT JOIN {unicorns_view} R7 ON L.company_name = R7.company_name
            WHERE
                L.dt = '{basic_dt}'
                AND L.company_province = '四川'
                AND L.bbd_qyxx_id is not null
                AND L.bbd_qyxx_id != 'null'
                AND substr(L.company_companytype, 1, 2) not in ('91', '92', '93')
            """)
        return df_basic

    def _create_company_tag_view(self, hdfs_path, view_name):
        return self.spark.read.csv(hdfs_path, header=True, sep='\t')\
            .drop_duplicates().createOrReplaceTempView(view_name)

    def _put_company_tag_info_to_hdfs(
            self, tmp_gazelle_path, tmp_key_enterprise_path, tmp_little_giant_path, tmp_unicorns_path):
        """
        上传文件到hdfs，注意，上传的文件名会写死!
        :param tmp_gazelle_path: 独角兽企业临时目录
        :param tmp_key_enterprise_path:　重点企业临时目录
        :param tmp_little_giant_path:　小巨人企业临时目录
        :param tmp_unicorns_path: 瞪羚企业临时目录
        :return:
        """
        gazelle_local_path = os.path.abspath(self._local_source_path_fmt.format('gazelle'))
        key_enterprise_local_path = os.path.abspath(self._local_source_path_fmt.format('key_enterprise'))
        little_giant_local_path = os.path.abspath(self._local_source_path_fmt.format('little_giant'))
        unicorns_local_path = os.path.abspath(self._local_source_path_fmt.format('unicorns'))
        put_source_file_to_hdfs(gazelle_local_path, tmp_gazelle_path)
        put_source_file_to_hdfs(key_enterprise_local_path, tmp_key_enterprise_path)
        put_source_file_to_hdfs(little_giant_local_path, tmp_little_giant_path)
        put_source_file_to_hdfs(unicorns_local_path, tmp_unicorns_path)

    def _gen_biz_data(self, table_name, data_result_path, columns=None):
        dt = HiveUtil.newest_partition(self.spark, table_name)

        basic_v = "qyxx_basic_v"
        self.get_qyxx_basic_df().createOrReplaceTempView(basic_v)

        columns = 'L.*' if not columns else columns
        exec_sql = f"""
            SELECT 
                {columns}
            FROM 
                {table_name} L
            JOIN {basic_v} R 
                ON L.bbd_qyxx_id = R.bbd_qyxx_id
            WHERE L.dt = {dt}
                AND L.bbd_qyxx_id is not null
                AND L.bbd_qyxx_id != 'null'
        """
        self.logger.info(f"exec sql is:{exec_sql}")
        self.spark.sql(exec_sql).write.parquet(data_result_path, mode='overwrite')
        self.logger.info(f"load table {table_name} to hdfs success")

    def _get_result_data_path(self, table_name):
        base_path = self.entry.cfg_mgr.hdfs.get_result_path('hdfs-biz', 'path_result')
        return os.path.join(base_path, table_name)

    def _get_tmp_data_path(self, table_name):
        base_path = self.entry.cfg_mgr.hdfs.get_tmp_path('hdfs-biz', 'path_basic_data')
        return os.path.join(base_path, table_name)

    def _get_source_date_path(self, table_name):
        base_path = self.entry.cfg_mgr.hdfs.get_input_path('hdfs-biz', 'path_basic_data')
        return os.path.join(base_path, table_name)
