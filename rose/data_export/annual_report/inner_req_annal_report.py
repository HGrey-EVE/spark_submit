#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Copyright: 2021 , BBD Tech. Co. Ltd
@Time: 2021/5/7 : 11:37
@Author: wenhao@bbdservice.com
@Description: 统计各省份2020年没有年报的公司的总数量
"""
import datetime

from pyspark.sql import DataFrame

from whetstone.core.entry import Entry
from whetstone.utils.utils_hive import HiveUtils


def pre_check(entry: Entry):
    return True


def main(entry: Entry):
    IndexCalculator.cal(entry)


def post_check(entry: Entry):
    return True


class IndexCalculator:
    @classmethod
    def cal(cls, entry: Entry):
        qyxx_province_data_frame = cls.__get_qyxx_province(entry)
        province_qyxx_count_data_frame = cls.__cal_province_company_count(entry,
                                                                          qyxx_province_data_frame)
        # todo save_path define
        save_path = f"inner_req_annual_report/{CommonUtil.get_version()}/without_2020_annual_report_province_company_count"
        province_qyxx_count_data_frame.repartition(1).write.csv(save_path,
                                                 header=True,
                                                 sep="\t",
                                                 mode="overwrite")
        entry.logger.info(f"save the company count csv result to {save_path} successfully!")

    @classmethod
    def __get_qyxx_province(cls, entry: Entry) -> DataFrame:
        table_qyxx_basic = "dw.qyxx_basic"
        table_annual_report = "dw.qyxx_annual_report_jbxx"
        qyxx_basic_dt = HiveUtils.newest_partition(entry.spark, table_qyxx_basic)
        annual_report_dt = HiveUtils.newest_partition(entry.spark, table_annual_report)
        sql_str = f"""
                        select distinct a.bbd_qyxx_id as bbd_qyxx_id,
                            a.company_province as province,
                            a.company_enterprise_status as company_status
                        from {table_qyxx_basic} a
                        where dt = '{qyxx_basic_dt}'
                            and a.company_type not like '个体%'
                            and a.company_type not like '%农民专业合作%'
                            and a.company_type not like '%合作社%'
                            and a.regcap_amount is not null 
                            and a.company_enterprise_status in ('正常', '存续', '开业', '个体转企业', '待迁入', '迁出', '迁入')
                            and not exists (
                                select 1 
                                from (
                                    select distinct bbd_qyxx_id
                                    from {table_annual_report}
                                    where dt = '{annual_report_dt}'
                                        and year = 2020
                                ) b 
                                where a.bbd_qyxx_id = b.bbd_qyxx_id
                            )                   
                   """
        qyxx_province_data_frame = entry.spark.sql(sql_str)

        entry.logger.info("finish to get the qyxx and province")
        return qyxx_province_data_frame

    @classmethod
    def __cal_province_company_count(cls, entry: Entry, qyxx_province_data_frame: DataFrame) -> DataFrame:
        qyxx_province_temp_view = "qyxx_province_view"
        qyxx_province_data_frame.createOrReplaceTempView(qyxx_province_temp_view)
        sql_str = f"""
                    select * 
                    from(
                        select province, 
                            company_status,
                            count(bbd_qyxx_id) as company_count
                        from {qyxx_province_temp_view}
                        group by province,company_status
                    )
                    order by province
                   """
        province_qyxx_count_data_frame = entry.spark.sql(sql_str)

        entry.logger.info("finish to calculate the company count of each province")
        return province_qyxx_count_data_frame


class CommonUtil:
    @staticmethod
    def get_version():
        return datetime.datetime.now().strftime("%Y%m") + "01"

