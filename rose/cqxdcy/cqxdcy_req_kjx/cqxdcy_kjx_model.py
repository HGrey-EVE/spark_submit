#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/4/28 : 9:52
@Author: wenhao@bbdservice.com
@Description: 科技型
"""
import json
import os

from pyspark.sql import DataFrame, functions
from pyspark.sql.types import StructType, StructField, StringType, FloatType, Row, MapType

from cqxdcy.proj_common.hive_util import HiveUtil
from whetstone.core.entry import Entry

KJX_MODULE_NAME = "cq_xdcy_kejixing"

TEMP_VIEW_QYXX_ENTERPRISE_SCALE = "temp_view_qyxx_enterprise_scale"
TEMP_VIEW_NEWS_CQZSTZ = "temp_view_news_cqzstz"
TEMP_VIEW_COMPANY_COUNTY = "temp_view_company_county"


class ComUtils:

    @staticmethod
    def get_source_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_input_path("hdfs", "hdfs_path")

    @staticmethod
    def get_temp_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")

    @staticmethod
    def get_final_path(entry: Entry):
        return entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")

    @staticmethod
    def save_csv(df: DataFrame, path: str, need_header=True, partition_num=1):
        df.repartition(partition_num).write.csv(path,
                                                header=need_header,
                                                sep="\t",
                                                mode="overwrite")


def pre_check(entry: Entry):
    command_prefix = "hadoop fs -test -e "

    model_kjx_result_path = f"{ComUtils.get_temp_path(entry)}/model/kjx_result.csv"
    industry_divide_path = os.path.join(ComUtils.get_source_path(entry), "industry_divide")
    check_path_list = [model_kjx_result_path, industry_divide_path]

    for path in check_path_list:
        file_existence = os.system(command_prefix + path)
        if file_existence != 0:
            entry.logger.error(f"{path} does not exist!")
            return False

    return True


def main(entry: Entry):
    # dw 表创建temp view
    create_dw_table_temp_view(entry)

    # 读取模型结果，暂存相关指标信息
    CheckModelResult.check_data(entry)
    # 指标计算
    CalculateIndex.calculate(entry)


def create_dw_table_temp_view(entry: Entry):
    # dw.qyxx_enterprise_scale temp view
    entry.spark.sql(f"""select * from dw.qyxx_enterprise_scale 
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.qyxx_enterprise_scale')}
                    """).createOrReplaceTempView(TEMP_VIEW_QYXX_ENTERPRISE_SCALE)
    # dw.news_cqzstz temp view
    entry.spark.sql(f"""select * from dw.news_cqzstz 
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.news_cqzstz')}
                    """).createOrReplaceTempView(TEMP_VIEW_NEWS_CQZSTZ)
    # dw.company_county temp view
    entry.spark.sql(f"""select * from dw.company_county 
                        where dt = {HiveUtil.newest_partition(entry.spark, 'dw.company_county')}
                    """).createOrReplaceTempView(TEMP_VIEW_COMPANY_COUNTY)


def post_check(entry: Entry):
    return True


class CheckModelResult:

    @classmethod
    def check_data(cls, entry: Entry):
        # 读取模型结果信息
        model_result_path = os.path.join(ComUtils.get_temp_path(entry),
                                         "model",
                                         "kjx_result.csv")
        kjx_model_result = entry.spark.read.csv(model_result_path, header=True)

        # 创建temp view, 日志记录
        kjx_model_view_name = "kjx_temp"
        entry.logger.info(f"create a temp view : '{kjx_model_view_name}'; "
                          f"and the data frame is loaded from '{model_result_path}'")
        kjx_model_result.createOrReplaceTempView(kjx_model_view_name)

        # 将 模型结果中的指标信息暂存，csv
        cls.save_kjx_model_index_infos_csv(entry,
                                           kjx_model_result,
                                           kjx_model_view_name)

    @classmethod
    def save_kjx_model_index_infos_csv(cls,
                                       entry: Entry,
                                       kjx_model_result: DataFrame,
                                       kjx_model_view_name: str):
        # 获取 bbd_qyxx_id 的schema
        temp_sql = f"select * from {kjx_model_view_name} limit 1"
        bbb_qyxx_id_schema = entry.spark.sql(temp_sql).schema[0]

        # 获取指标信息
        kjx_index_infos = kjx_model_result.rdd \
            .map(lambda x: x.asDict()) \
            .map(lambda x: cls.get_cal_index_infos(x))

        # 将 转换后的 指标信息，转为 dataframe
        kjx_index_info_df = entry.spark.createDataFrame(kjx_index_infos,
                                                        schema=StructType([
                                                            bbb_qyxx_id_schema,
                                                            StructField("valuation_model", StringType(), True),
                                                            StructField("comprehensive_score", FloatType(), True)]))
        # 暂存； csv
        csv_save_path = os.path.join(ComUtils.get_temp_path(entry),
                                     KJX_MODULE_NAME,
                                     "kjx_scores")
        ComUtils.save_csv(kjx_index_info_df, csv_save_path)
        entry.logger.info(f"Save kjx model index infos to '{csv_save_path}' successfully!")

    @classmethod
    def get_cal_index_infos(cls, score_dict: dict):
        """
        获取指标信息
        """
        return {"bbd_qyxx_id": score_dict["bbd_qyxx_id"],
                "comprehensive_score": float(score_dict["kjx_score"]),
                "valuation_model":
                    json.dumps(
                        {
                            "核心技术情况": score_dict["hxjsqk_score"],
                            "研发能力情况": score_dict["yfnlqk_score"],
                            "研发成果市场认可情况": score_dict["yfscrkqk_score"],
                            "技术成果转化情况": score_dict["jscgzhqk_score"],
                            "相对竞争优势情况": score_dict["xdjzysqk_score"]
                        }, ensure_ascii=False
                    )
                }


class CalculateIndex:
    """
    计算相关指标
    """

    @classmethod
    def calculate(cls, entry: Entry):
        # 注册spark.sql 中，自定义函数：check_industry()
        entry.spark.udf.register("check_industry",
                                 cls.get_company_qualification,
                                 StringType())

        # 计算 省市区 企业规模
        pro_city_area_scale_df = cls.cal_province_city_area_scale(entry)
        # 企业性质
        enterprise_qualification_df = cls.cal_enterprise_qualification(entry)
        # 计算科技型
        kjx_result = cls.cal_kjx_result(entry,
                                        pro_city_area_scale_df,
                                        enterprise_qualification_df)

        kjx_push_data = kjx_result.rdd.map(cls.to_push_data)
        kjx_push_data_schema = StructType([StructField("tn",
                                                       StringType(),
                                                       True),
                                           StructField("data",
                                                       MapType(StringType(), StringType()),
                                                       True)])
        kjx_push_data_df = entry.spark.createDataFrame(kjx_push_data, schema=kjx_push_data_schema)

        kjx_push_data_path = os.path.join(ComUtils.get_final_path(entry), "cq_xdcy_cultivate_enterprise_list")
        kjx_push_data_df.write.json(kjx_push_data_path, mode="overwrite")
        entry.logger.info(f"Save kjx push data to {kjx_push_data_path} successfully!")

    @classmethod
    def get_company_qualification(cls, company_nature: str):
        if company_nature is None or company_nature == "":
            qualification = "[]"
        else:
            qualification = json.dumps(company_nature.split(","),
                                       ensure_ascii=False)
        return qualification

    @classmethod
    def cal_province_city_area_scale(cls, entry: Entry) -> DataFrame:
        # 计算 省市区 企业规模
        province_city_area_df = cls.cal_province_city_area(entry)

        scale_sql = f"""
                        select bbd_qyxx_id, 
                            company_scale as enterprise_scale
                        from {TEMP_VIEW_QYXX_ENTERPRISE_SCALE}
                     """
        enterprise_scale_df = entry.spark.sql(scale_sql)

        pro_city_area_scale_df = province_city_area_df.join(enterprise_scale_df,
                                                            "bbd_qyxx_id",
                                                            "left") \
            .where("bbd_qyxx_id is not null")

        entry.logger.info("finish to calculate the enterprise scale.")
        return pro_city_area_scale_df

    @classmethod
    def cal_enterprise_qualification(cls, entry: Entry) -> DataFrame:
        # 计算企业性质(资质)
        nature_sql = f"""
                        select distinct bbd_qyxx_id,
                            case when nick_name is not null 
                                 then '科技成长型'
                                 else nick_name
                            end company_nature
                        from {TEMP_VIEW_NEWS_CQZSTZ}
                      """
        nature_df = entry.spark.sql(nature_sql)

        get_nature_udf = functions.udf(cls.get_company_qualification,
                                       StringType())
        enterprise_qualification_df = nature_df.withColumn("qualification",
                                                           get_nature_udf("company_nature"))

        entry.logger.info("finish to calculate the enterprise qualification")
        return enterprise_qualification_df

    @classmethod
    def cal_kjx_result(cls, entry: Entry,
                       enterprise_scale: DataFrame,
                       enterprise_qualification: DataFrame) -> DataFrame:
        """
        计算科技型 结果
        """

        company_industry_map = {
            "A": "农、林、牧、渔业",
            "B": "采矿业",
            "C": "制造业",
            "D": "电力、热力、燃气及水生产和供应业",
            "E": "建筑业",
            "F": "批发和零售业",
            "G": "交通运输、仓储和邮政业",
            "H": "住宿和餐饮业",
            "I": "信息传输、软件和信息技术服务业",
            "J": "金融业",
            "K": "房地产业",
            "L": "租赁和商务服务业",
            "M": "科学研究和技术服务业",
            "N": "水利、环境和公共设施管理业",
            "O": "居民服务、修理和其他服务业",
            "P": "教育",
            "Q": "卫生和社会工作",
            "R": "文化、体育和娱乐业",
            "S": "公共管理、社会保障和社会组织",
            "T": "国际组织",
            "Z": "其它"
        }

        def get_type_weight(company_type: str) -> str:
            # 根据公司所属的行业赋权，为排序做准备
            try:
                return company_industry_map[company_type]
            except:
                return '其它'

        area_scale_qua_df = enterprise_scale.join(enterprise_qualification,
                                                  "bbd_qyxx_id",
                                                  "left") \
            .where("bbd_qyxx_id is not null")
        # 读取最开始暂存的模型指标信息

        kjx_model_index_infos_df = entry.spark.read.csv(os.path.join(ComUtils.get_temp_path(entry),
                                                                     KJX_MODULE_NAME,
                                                                     "kjx_scores"),
                                                        header=True,
                                                        sep="\t")
        kjx_sc_basic = kjx_model_index_infos_df.join(area_scale_qua_df,
                                                     "bbd_qyxx_id",
                                                     "left") \
            .where("bbd_qyxx_id is not null")

        get_type_weight_udf = functions.udf(get_type_weight,
                                            StringType())
        kjx_result = kjx_sc_basic.withColumn("company_industry",
                                             get_type_weight_udf("abc")) \
            .drop("abc")
        entry.logger.info("finish to calculate the kjx result")
        return kjx_result

    @classmethod
    def cal_province_city_area(cls, entry: Entry) -> DataFrame:
        """
        计算公司 省、市、区
        :return: DataFrame
        """
        qyxx_basic_path = os.path.join(ComUtils.get_source_path(entry), "industry_divide")
        entry.spark.read \
            .parquet(qyxx_basic_path) \
            .createOrReplaceTempView("qyxx_basic")
        # 省市区 结果集
        cls.get_company_county(entry).createOrReplaceTempView("company_counties")

        sql_str = f"""
                    select
                        update_time,
                        establish_age, 
                        statistics_time, 
                        bbd_qyxx_id, 
                        company_name, 
                        credit_code, 
                        frname, 
                        address, 
                        operate_scope, 
                        esdate, 
                        regcap, 
                        regcap_amount, 
                        industry, 
                        abc, 
                        sub_industry, 
                        province, 
                        city,
                        area
                    from (
                    select 
                        from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
                        date_format(current_date , 'y') - date_format(R.esdate, 'y') as establish_age, 
                        '{entry.version[0:4]}-{entry.version[4:6]}-{entry.version[6:8]}' as statistics_time, 
                        R.bbd_qyxx_id, 
                        R.company_name, 
                        R.credit_code, 
                        R.frname, 
                        R.address, 
                        R.operate_scope, 
                        date_format(R.esdate, 'yyyy-MM-dd') as esdate, 
                        R.regcap, 
                        R.regcap_amount, 
                        check_industry(R.industry) as industry, 
                        R.company_industry as abc, 
                        check_industry(R.sub_industry) as sub_industry, 
                        L.province, 
                        L.city,
                        L.area
                    from company_counties L
                    right join qyxx_basic R
                    on L.company_county = R.company_county) a
                  """
        company_county_df = entry.spark.sql(sql_str)
        entry.logger.info("finish to calculate the company county.")
        return company_county_df

    @classmethod
    def get_company_county(cls, entry: Entry) -> DataFrame:
        """
        获取 省、市、区
        """
        sql_str = f"""select company_county, province, city, area 
                      from 
                          (select code as company_county, 
                                province, 
                                city, 
                                district as area, 
                                (row_number() over(partition by code order by tag)) as row_num
                           from {TEMP_VIEW_COMPANY_COUNTY} 
                           where tag != -1) as a
                      where row_num = 1"""
        df = entry.spark.sql(sql_str)
        entry.logger.info("finish to get company county")
        return df

    @classmethod
    def to_push_data(cls, entry: Entry, row: Row):
        """
        返回订阅推送结构的数据
        """
        tmp_data = row.asDict(True)
        entry.logger.info(tmp_data)
        push_data = {}
        tn = "cq_xdcy_cultivate_enterprise_list"
        push_data["tn"] = tn
        push_data["data"] = tmp_data
        return push_data
