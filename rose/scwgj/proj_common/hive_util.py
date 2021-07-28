#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: hive_util
:Author: xufeng@bbdservice.com 
:Date: 2021-04-16 11:37 AM
:Version: v.1.0
:Description:
    get table partition
"""
from pyspark.sql import SparkSession


class HiveUtil:

    @staticmethod
    def newest_partition(spark: SparkSession, table_name: str):
        """
        get newest partition for hive table
        error happened if table not exist or spark session is not activate
        :param spark: current spark session
        :param table_name:
        :return: newest partition or None
        """
        partitions = HiveUtil.get_table_partitions(spark, table_name)
        newest_partition = partitions[0] if partitions else None
        return newest_partition

    @staticmethod
    def newest_partition_with_data(
            spark: SparkSession, table_name: str, need_row: int = 1, partition_field: str = 'dt'):
        """
        get newest partition with row count >= need_row
        :param spark:
        :param table_name:
        :param need_row:
        :param partition_field:
        :return:
        """
        partitions = HiveUtil.get_table_partitions(spark, table_name)

        if not partitions:
            print(f'====table {table_name} has no partition')
            return None

        for partition in partitions:
            cnt = spark.sql(
                f"""
                SELECT 1 FROM {table_name} WHERE {partition_field} = {partition} LIMIT {need_row} 
                """).count()

            if cnt >= need_row:
                return partition

        msg = f'====table {table_name} is empty in all partitions!'
        print(msg)
        raise Exception(msg)

    @staticmethod
    def latest_partition_with_date(spark: SparkSession, table_name: str, end_date_str: str, rise_when_no_data=False):
        """
        get latest partition before calc date
        :param spark:
        :param table_name:
        :param end_date_str: date str like partition format, ex: '202103' '20210321'
        :param rise_when_no_data: if set true, will rise exception when no data found
        :return:
        """
        partitions = HiveUtil.get_table_partitions(spark, table_name)

        if not partitions:
            print(f'====table {table_name} has no partition')
            return None

        for partition in partitions:
            if partition < end_date_str:
                return partition

        msg = f'====table {table_name} has no partitions earlier than {end_date_str}'
        print(msg)
        if rise_when_no_data:
            raise Exception(msg)
        return None

    @staticmethod
    def get_table_partitions(spark: SparkSession, table_name: str):
        """
        return all partitions for table
        :param spark:
        :param table_name:
        :return: desc sorted table partition list
        """
        table_partitions_row = spark.sql(f"""show partitions {table_name}""")\
            .rdd\
            .map(lambda r: r[0].split('=')[-1])\
            .collect()
        table_partitions = sorted(list(table_partitions_row), reverse=True)
        return table_partitions
