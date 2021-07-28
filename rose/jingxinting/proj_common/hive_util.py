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
    def latest_partition_with_date(spark: SparkSession, table_name: str, end_date_str: str):
        """
        get latest partition before calc date
        :param spark:
        :param table_name:
        :param end_date_str: date str like partition format, ex: '202103' '20210321'
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
        raise Exception(msg)

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

    @staticmethod
    def get_month_newest_partition(spark: SparkSession, table_name: str, month: str, dw='dw') -> str:
        """
        When the partition table has no partitions in the specified month, the latest partition of the
        partition table is returned. When the partition table has partitions in the specified month,
        the first partition of the specified month is returned.
        :param spark:
        :param table_name:
        :param month:
        :return:
        """
        table_partitions_row = spark.sql(f"""show partitions {dw}.{table_name}""") \
            .rdd \
            .map(lambda r: r[0].split('=')[-1]) \
            .collect()
        if len(table_partitions_row) == 0:
            print(f"""====table {table_name} has no partition!""")
            raise Exception(f"""====table {table_name} has no partition!""")
        table_partitions = sorted(list(table_partitions_row), reverse=True)
        return_partition = table_partitions[0]
        table_partitions = sorted(list(table_partitions_row))
        for partition in table_partitions:
            if month in partition:
                return_partition = partition
                break
        return return_partition
