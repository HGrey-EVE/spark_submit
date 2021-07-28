import traceback
from logging import Logger
from hongjing.proj_common.hbase_util import HbaseWriteHelper

# 由于MD5模块在python3中被移除，在python3中使用hashlib模块进行md5操作
from hashlib import md5
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pyspark.sql import SparkSession

'''
    1、将manage_recruit表数据插入manage_recruit_simple表
    2、将manage_recruit_simple表数据存入csv文件
'''
class WriteToHbase:

    def __init__(self,
                 version: str,
                 logger: Logger,
                 table_name: str,
                 family_name: str,
                 hfile_absolute_path: str,
                 index_path: str,
                 hbase_columns: str,
                 meta_table_name: str,
                 meta_row_key: str
                 ):
        self.logger = logger
        self.version = version
        self.table_name = table_name
        self.family_name = family_name
        self.hfile_absolute_path = hfile_absolute_path
        self.index_path = index_path
        self.hbase_columns = hbase_columns
        self.meta_table_name = meta_table_name
        self.meta_row_key = meta_row_key

    def run(self):

        self.load_to_hbase(self.table_name, self.family_name, self.hfile_absolute_path, self.index_path,
                           self.hbase_columns, self.meta_table_name,self.meta_row_key)
        self.logger.info("导入到hbase成功 !")

    def load_to_hbase(self, table_name, family_name, hfile_absolute_path, index_path, hbase_columns, meta_table_name,
                      meta_row_key):
        """
        数据导入hbase
        """
        hbase_write_helper = HbaseWriteHelper(
            self.logger,
            table_name,
            family_name,
            "",
            hfile_absolute_path=hfile_absolute_path,
            source_data_absolute_path=index_path,
            source_csv_delimiter="|",
            hbase_columns=hbase_columns,
            meta_table_name=meta_table_name,
            meta_row=meta_row_key
        )
        try:
            self.logger.info(f"begin load table {table_name} to hbase")
            hbase_write_helper.exec_write()
            self.logger.info(f"load table {table_name} field hbase success")
        except Exception:
            self.logger.error(
                f"load table {table_name} to hbase failed:{traceback.format_exc()}")

