#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: mysql_util
:Author: xufeng@bbdservice.com 
:Date: 2021-05-10 5:42 PM
:Version: v.1.0
:Description: mysql操作流程，来自外管局二期 xiaoqiang
"""
import subprocess

from scwgj.proj_common.single_util import SingletonClass
from whetstone.core.config_mgr import ConfigManager


class MysqlUtils(metaclass=SingletonClass):

    def __init__(self, ip, port, db_name, user, password):
        self.ip = ip
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password

        print("========================== ")
        print(f"连接数据库信息：{self.ip}:{self.port},{self.user},{self.password},{self.db_name}")
        print("========================== ")

    def execute_sql(self, sql_str):
        """
        执行数据库查询操作
        :param self: 数据库连接类实例
        :param sql_str: 需要执行的操作sql语句 String类型
        :return: 若成功则返回执行结果 list类型
        """
        pass

    def get_table_meta(self, table_ame=''):
        """
        执行表查询元数据操作
        :param self: 数据库连接类实例
        :param table_ame: 表名
        :return: 返回表信息
        """
        sql = f'desc {table_ame}'
        table_desc = self.execute_sql(sql).collect()
        return {desc[0]: desc[1] for desc in table_desc}

    def replicate_table_structure(self, source_tab, destination_tab, exclude_columns=''):
        """
        复制已有表结构建新表（无数据）
        :param self: 数据库连接类实例
        :param source_tab: 原表表名
        :param destination_tab: 新表表名
        :param exclude_columns: （optional）不需被复制的列
        :return: 返回建表语句
        """
        tab_meta_dict = self.get_table_meta(source_tab)
        exclude_set = set(exclude_columns.split(','))
        ret_sql = "CREATE TABLE " + destination_tab + " as SELECT   "
        select = ""
        for (k, v) in tab_meta_dict.items():
            if k not in exclude_set:
                select = select + "," + k
        ret_sql = ret_sql + select[1:] + " from " + source_tab
        return ret_sql

    def replicate_table(self, source_tab, destination_tab="", exclude_columns=''):
        """
        复制已有表到新表
        :param source_tab: 原表表名
        :param destination_tab: 新表表名
        :param exclude_columns: （optional）不需被复制的列
        :return:
        """
        pass

    def dump_to_sql(self, table, sql_file):
        """
        sql文件从mysql导出到本地
        :param table: 导出表表名
        :param sql_file: 目标文件夹名
        :return: None
        """
        dump_query = (
                "mysqldump "
                "-h {ip} "
                "-u {user} "
                "-p{passwd} "
                "-P {port}  "
                "--databases {database} "
                "-c -t --tables {table} > {sql_file}"
        ).format(
            ip=self.ip,
            port=self.port,
            user=self.user,
            passwd=self.password,
            database=self.db_name,
            table=table,
            sql_file=sql_file
        )
        if subprocess.call(dump_query, shell=True) != 0:
            print("数据导出错误： {table}".format(table=table))

    def sqoop_export(self, table, data_path, dt=None):
        """
        用sqoop命令行的方式连接数据库，把数据从集群hdfs上批量导出到数据库
        :param self: 数据库连接配置
        :param table: 需要导入的数据库表名
        :param data_path: 数据存储在hdfs上的路径(需要使用绝对路径)
        :param dt: (optional) hdfs数据可能有分区信息，若没有则不填
        :return: None
        """
        if dt:
            data_path = data_path + "/dt=" + dt
        export_commond = (
            "sqoop export "
            "--connect jdbc:mysql://{ip}:{port}/{db_name}?characterEncoding=UTF-8 "
            "--username {user} "
            "--num-mappers {mapper} "
            "--password '{password}' "
            "--table {table_name} "
            "--export-dir {absolute_path} "
            "--input-fields-terminated-by '\\t'"
        ).format(
            ip=self.ip,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            mapper=self.mapper,
            password=self.password,
            table_name=table,
            absolute_path=data_path
        )

        self.__get_shell_record(
            subprocess.call(export_commond, shell=True),
            "导入错误：{table} {data_path}".format(table=table, data_path=data_path)
        )

    def jdbc_read(self, spark, table):
        """
        需要引mysql-connector.jar 包
        :param table: 数据库表名
        :return: spark: dataframe 的mysql数据
        """
        jdbc_query = (
            "jdbc:mysql://{ip}:{port}/{db_name}"
            "?user={user}"
            "&password={password}"
            "&characterEncoding=utf8"
        ).format(
            ip=self.ip,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            password=self.password
        )
        driver = "com.mysql.jdbc.Driver"
        df = spark.read.format("jdbc").options(
            url=jdbc_query,
            dbtable=table,
            driver=driver
        ).load()
        return df

    def jdbc_write(self, df, table, write_mode="overwrite"):
        """
        :param df: spark df格式的数据
        :param table: 写入表名
        :param write_mode: 写入模式（默认为覆盖）
        :return: None
        """
        jdbc_query = (
            "jdbc:mysql://{ip}:{port}/{db_name}"
            "?user={user}"
            "&password={password}"
            "&characterEncoding=utf8"
        ).format(
            ip=self.ip,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            password=self.password
        )
        driver = "com.mysql.jdbc.Driver"
        df.write.mode(write_mode).format("jdbc").options(
            url=jdbc_query,
            dbtable=table,
            driver=driver
        ).save()


def get_mysql_instance(section, conf: ConfigManager) -> MysqlUtils:
    instance = MysqlUtils(
        conf.get(section, 'ip'),
        conf.get(section, 'port'),
        conf.get(section, 'db_name'),
        conf.get(section, 'user'),
        conf.get(section, 'password'),
    )
    return instance


if __name__ == '__main__':
    pass
