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
    def newest_partition(
            spark,
            table,
            in_month_str=None,
            max_version=None,
            partition_field="dt",
            under_force=False,
            test_num=1
    ):
        """
        获取hive表最新分区，只支持获取一个分区字段的最新分区
        :param spark:
        :param table:
        :param partition_field: 分区字段
        :param in_month: 在某个月的最新分区,注意日期格式要和分区格式一致，eg:201901,2019-01
        :param max_version:  小于等于 max_version
        :param under_force: 是否强制获取in_month_str月（或小于max_version）最新分区，不强制则：1、在该月内有最新则取最新，2、没有最新则取往前推最近的，3、往前没有最近的则取全部最新的
        :param test_num: 检查分区是否为空（低于test_num表示为空）,0或None表示可以返回空分区
        :return:
        """
        partition_field_len = len(partition_field) + 1
        partitions_arr = spark.sql("show partitions {table}".format(table=table)) \
            .rdd.map(lambda r: r[0][partition_field_len:]) \
            .collect()

        inmonth_arr_new = list(partitions_arr)
        inmonth_arr_new2 = []
        # 是否在某月内最新分区
        if in_month_str:
            in_month_str_len = len(in_month_str)
            # 是否强制在改月内
            if under_force:
                inmonth_arr_new = [p for p in partitions_arr if p[:in_month_str_len] == in_month_str]
            # 不强制则：1、在该月内有最新则取最新，2、没有最新则取往前推最近的，3、往前没有最近的则取全部最新的
            else:
                inmonth_arr_new = [p for p in partitions_arr if p[:in_month_str_len] <= in_month_str]
                inmonth_arr_new2 = [p for p in partitions_arr if p[:in_month_str_len] > in_month_str]

        maxv_arr_new = list(partitions_arr)
        maxv_arr_new2 = []
        # 是否在小于等于某个分区
        if max_version:
            # 是否强制小于等于
            if under_force:
                maxv_arr_new = [p for p in partitions_arr if p <= max_version]
            # 不强制则：1、在小于某个分区有最新则取最新，2、没有最新则取往前推最近的，3、往前没有最近的则取全部最新的
            else:
                maxv_arr_new = [p for p in partitions_arr if p <= max_version]
                maxv_arr_new2 = [p for p in partitions_arr if p > max_version]

        # 合并in_month 和 max_version结果
        partitions_arr_new = sorted(list(set(inmonth_arr_new).intersection(set(maxv_arr_new))), reverse=True)
        partitions_arr_new2 = sorted(list(set(inmonth_arr_new2).union(set(maxv_arr_new2))), reverse=False)

        def get_partition(partitions_arr, table, test_num):
            """
            #判断分区是否满足test_num
            :param partitions_arr:
            :param table:
            :param test_num:
            :return:
            """
            part = None
            for p in partitions_arr:
                if not test_num:
                    part = p
                    break
                test_count = spark.sql(
                    'select dt from {table} where dt={partition} limit {test_num}'.format(
                        table=table,
                        partition=p,
                        test_num=test_num
                    )
                ).count()
                if test_count >= test_num:
                    part = p
                    break
            return part

        # 获取在月内（或小于等于某分区）的 分区值
        partition = get_partition(partitions_arr_new, table, test_num)

        # 如果没有符合上面规则的分区，且不强制则：1、，2、，3、往前最近的分区
        if (in_month_str or max_version) and not under_force and partition is None:
            return get_partition(partitions_arr_new2, table, test_num)
        else:
            return partition

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
    def newest_partition_by_month(
            spark,
            table,
            in_month_str=None,
            max_version=None,
            partition_field="dt",
            under_force=False,
            test_num=1
    ):
        """
        获取hive表最新分区，只支持获取一个分区字段的最新分区
        :param spark:
        :param table:
        :param partition_field: 分区字段
        :param in_month: 在某个月的最新分区,注意日期格式要和分区格式一致，eg:201901,2019-01
        :param max_version:  小于等于 max_version
        :param under_force: 是否强制获取in_month_str月（或小于max_version）最新分区，不强制则：1、在该月内有最新则取最新，2、没有最新则取往前推最近的，3、往前没有最近的则取全部最新的
        :param test_num: 检查分区是否为空（低于test_num表示为空）,0或None表示可以返回空分区
        :return:
        """
        partition_field_len = len(partition_field) + 1
        partitions_arr = spark.sql("show partitions {table}".format(table=table)) \
            .rdd.map(lambda r: r[0][partition_field_len:]) \
            .collect()

        inmonth_arr_new = list(partitions_arr)
        inmonth_arr_new2 = []
        # 是否在某月内最新分区
        if in_month_str:
            in_month_str_len = len(in_month_str)
            # 是否强制在改月内
            if under_force:
                inmonth_arr_new = [p for p in partitions_arr if p[:in_month_str_len] == in_month_str]
            # 不强制则：1、在该月内有最新则取最新，2、没有最新则取往前推最近的，3、往前没有最近的则取全部最新的
            else:
                inmonth_arr_new = [p for p in partitions_arr if p[:in_month_str_len] <= in_month_str]
                inmonth_arr_new2 = [p for p in partitions_arr if p[:in_month_str_len] > in_month_str]

        maxv_arr_new = list(partitions_arr)
        maxv_arr_new2 = []
        # 是否在小于等于某个分区
        if max_version:
            # 是否强制小于等于
            if under_force:
                maxv_arr_new = [p for p in partitions_arr if p <= max_version]
            # 不强制则：1、在小于某个分区有最新则取最新，2、没有最新则取往前推最近的，3、往前没有最近的则取全部最新的
            else:
                maxv_arr_new = [p for p in partitions_arr if p <= max_version]
                maxv_arr_new2 = [p for p in partitions_arr if p > max_version]

        # 合并in_month 和 max_version结果
        partitions_arr_new = sorted(list(set(inmonth_arr_new).intersection(set(maxv_arr_new))), reverse=True)
        partitions_arr_new2 = sorted(list(set(inmonth_arr_new2).union(set(maxv_arr_new2))), reverse=False)

        def get_partition(partitions_arr, table, test_num):
            """
            #判断分区是否满足test_num
            :param partitions_arr:
            :param table:
            :param test_num:
            :return:
            """
            part = None
            for p in partitions_arr:
                if not test_num:
                    part = p
                    break
                test_count = spark.sql(
                    'select {pt_f} from {table} where {pt_f}={partition} limit {test_num}'.format(
                        table=table,
                        pt_f=partition_field,
                        partition=p,
                        test_num=test_num
                    )
                ).count()
                if test_count >= test_num:
                    part = p
                    break
            return part

        # 获取在月内（或小于等于某分区）的 分区值
        partition = get_partition(partitions_arr_new, table, test_num)

        # 如果没有符合上面规则的分区，且不强制则：1、，2、，3、往前最近的分区
        if (in_month_str or max_version) and not under_force and partition is None:
            return get_partition(partitions_arr_new2, table, test_num)
        else:
            return partition
