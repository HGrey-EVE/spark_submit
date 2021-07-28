import sys
from datetime import datetime, date
import os
import time
from logging import Logger

from pyspark.sql.functions import udf, lit
# 由于MD5模块在python3中被移除，在python3中使用hashlib模块进行md5操作
from hashlib import md5
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from hongjing.proj_common.hive_util import HiveUtil

'''
    1、将manage_recruit表数据插入manage_recruit_simple表
    2、将manage_recruit_simple表数据存入csv文件
'''
class IndexHandler:

    def __init__(self, spark: SparkSession,
                 version: str,
                 logger: Logger,
                 output_path: str, ):
        self.spark = spark
        self.logger = logger
        self.version = version
        self.output_path = output_path

    def run(self):
        self.logger.info("统一处理临时表")
        self.hive_pre_prepare()

        self.logger.info("计算指标")
        self.handle_index()

    def handle_index(self):

        # 1、将manage_recruit表数据插入manage_recruit_simple表
        self.manage_recruit_to_simple()
        self.logger.info(f"将manage_recruit表数据插入manage_recruit_simple表中 finished! ")

        # 2、将manage_recruit_simple表数据存入csv文件
        self.manage_recruit_simple_to_csv()

        self.logger.info(f"写入csvs完成 finished! ")

    def manage_recruit_simple_to_csv(self):
        # 学历信息统计
        df = self.spark.sql(
            "select bbd_qyxx_id,collect_set(company_name)[0] company_name,education_required,cast(sum(recruit_numbers) as bigint) recruit_numbers from manage_recruit_simple group by bbd_qyxx_id,education_required ")
        df.createOrReplaceTempView("df_table")

        # 每月招聘人数统计
        df_recruit = self.spark.sql(
                            "select bbd_qyxx_id,collect_set(company_name)[0] company_name,pubdate,cast(sum(recruit_numbers) as bigint) recruit_numbers from manage_recruit_simple group by bbd_qyxx_id,pubdate ")
        df_recruit.createOrReplaceTempView("df_recruit_table")

        # union它不会去除重复数据
        df.union(df_recruit).createOrReplaceTempView("all_table")

        def get_year_month(n):
            datedata = datetime.today()
            month = datedata.month
            year = datedata.year
            for i in range(n):
                if month == 1:
                    year -= 1
                    month = 12
                else:
                    month -= 1
            return date(year, month, 1).strftime("%Y-%m")

        month_1 = get_year_month(1)
        month_2 = get_year_month(2)
        month_3 = get_year_month(3)
        month_4 = get_year_month(4)
        month_5 = get_year_month(5)
        month_6 = get_year_month(6)
        month_7 = get_year_month(7)
        month_8 = get_year_month(8)
        month_9 = get_year_month(9)
        month_10 = get_year_month(10)
        month_11 = get_year_month(11)
        month_12 = get_year_month(12)

        # 行转列
        df_all_row = self.spark.sql(f"select bbd_qyxx_id,collect_set(company_name)[0] company_name,"
                                f"MAX(CASE WHEN education_required='硕士及以上' THEN recruit_numbers ELSE 0 END) AS master,"
                                f"MAX(CASE WHEN education_required='本科' THEN recruit_numbers ELSE 0 END) AS bachelor,"
                                f"MAX(CASE WHEN education_required='本科以下' THEN recruit_numbers ELSE 0 END) AS bachelor_under,"
                                f"MAX(CASE WHEN education_required='其他' THEN recruit_numbers ELSE 0 END) AS other,"
                                f"MAX(CASE WHEN education_required='{month_1}' THEN recruit_numbers ELSE 0 END) AS month1,"
                                f"MAX(CASE WHEN education_required='{month_2}' THEN recruit_numbers ELSE 0 END) AS month2,"
                                f"MAX(CASE WHEN education_required='{month_3}' THEN recruit_numbers ELSE 0 END) AS month3,"
                                f"MAX(CASE WHEN education_required='{month_4}' THEN recruit_numbers ELSE 0 END) AS month4,"
                                f"MAX(CASE WHEN education_required='{month_5}' THEN recruit_numbers ELSE 0 END) AS month5,"
                                f"MAX(CASE WHEN education_required='{month_6}' THEN recruit_numbers ELSE 0 END) AS month6,"
                                f"MAX(CASE WHEN education_required='{month_7}' THEN recruit_numbers ELSE 0 END) AS month7,"
                                f"MAX(CASE WHEN education_required='{month_8}' THEN recruit_numbers ELSE 0 END) AS month8,"
                                f"MAX(CASE WHEN education_required='{month_9}' THEN recruit_numbers ELSE 0 END) AS month9,"
                                f"MAX(CASE WHEN education_required='{month_10}' THEN recruit_numbers ELSE 0 END) AS month10,"
                                f"MAX(CASE WHEN education_required='{month_11}' THEN recruit_numbers ELSE 0 END) AS month11,"
                                f"MAX(CASE WHEN education_required='{month_12}' THEN recruit_numbers ELSE 0 END) AS month12 "
                                f"from all_table group by bbd_qyxx_id ")
        ## udf rn
        def get_rn_json(month1, month2, month3, month4, month5, month6, month7, month8, month9, month10, month11, month12):
            result = "{"
            if month1 > 0:
                result += f",'{month_1}':" + str(month1)
            if month2 > 0:
                result += f",'{month_2}':" + str(month2)
            if month3 > 0:
                result += f",'{month_3}':" + str(month3)
            if month4 > 0:
                result += f",'{month_4}':" + str(month4)
            if month5 > 0:
                result += f",'{month_5}':" + str(month5)
            if month6 > 0:
                result += f",'{month_6}':" + str(month6)
            if month7 > 0:
                result += f",'{month_7}':" + str(month7)
            if month8 > 0:
                result += f",'{month_8}':" + str(month8)
            if month9 > 0:
                result += f",'{month_9}':" + str(month9)
            if month10 > 0:
                result += f",'{month_10}':" + str(month10)
            if month11 > 0:
                result += f",'{month_11}':" + str(month11)
            if month12 > 0:
                result += f",'{month_12}':" + str(month12)
            result += "}"
            return result.replace(',', '', 1)

        def get_er_json(master, bachelor, bachelor_under, other):
            result = "{"
            if master > 0:
                result += ",'硕士及以上':" + str(master)
            if bachelor > 0:
                result += ",'本科':" + str(bachelor)
            if bachelor_under > 0:
                result += ",'本科以下':" + str(bachelor_under)
            if other > 0:
                result += ",'其他':" + str(other)
            result += "}"
            return result.replace(',', '', 1)

        get_er_json_udf = udf(get_er_json, StringType())
        get_rn_json_udf = udf(get_rn_json, StringType())
        # 多列合并为json
        df_all_row_json = df_all_row \
            .withColumn("er_json", get_er_json_udf(df_all_row.master, df_all_row.bachelor, df_all_row.bachelor_under,df_all_row.other)) \
            .withColumn("rn_json",get_rn_json_udf(df_all_row.month1, df_all_row.month2, df_all_row.month3, df_all_row.month4,df_all_row.month5, df_all_row.month6, df_all_row.month7, df_all_row.month8,df_all_row.month9, df_all_row.month10, df_all_row.month11, df_all_row.month12)) \
            .select("bbd_qyxx_id", "company_name", "er_json", "rn_json")

        merged_result_path = os.path.join(self.output_path, os.path.join(self.version,'manage_recruit'))
        df_all_row_json.coalesce(100).write.option("delimiter", "|").csv(merged_result_path, mode="overwrite")

    def manage_recruit_to_simple(self):
        one_year_pre = self.get_one_year_pre()
        self.spark.sql(
            f"""
            select 
                bbd_qyxx_id,
                company_name,
                case when education_required like '%硕士%' then '硕士及以上' 
                     when education_required like '%本科%' then '本科' 
                     when education_required like '%初中%' or education_required like '%高中%' or education_required like '%大专%'  then '本科以下' 
                     else '其他' end education_required,
                case when recruit_numbers rlike '\^\\\\d+$' then recruit_numbers 
                else 1 end recruit_numbers,
                substr(pubdate, 0,7) pubdate
            from manage_recruit where pubdate > to_date(from_unixtime(UNIX_TIMESTAMP('{one_year_pre}','yyyy-MM-dd'))) 
            and bbd_qyxx_id is not NULL
        """).cache().createOrReplaceTempView("manage_recruit_simple")

    def hive_pre_prepare(self):
        manage_recruit_dt = HiveUtil.newest_partition(self.spark, "dw.manage_recruit")
        self.logger.info("version -> " + self.version)
        self.logger.info("manage_recruit_dt -> " + manage_recruit_dt)
        self.spark.sql(f"select * from dw.manage_recruit where dt = '{manage_recruit_dt}'").createOrReplaceTempView(
            "manage_recruit")

    # 一年前的今天
    def get_one_year_pre(self):
        start_year = int(time.strftime('%Y', time.localtime(time.time()))) - 1
        month_day = time.strftime('%m-%d', time.localtime(time.time()))
        start_time = str(start_year) + "-" + str(month_day)
        self.logger.info("time is -> " + start_time)
        return start_time