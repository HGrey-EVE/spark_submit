#!/usr/bin/env python
# -*- coding-utf8 -*-
"""
:Copyright: 2019, BBD Tech. Co.,Ltd.
:File Name: hbase_util
:Author: xufeng@bbdservice.com 
:Date: 2021-04-16 11:38 AM
:Version: v.1.0
:Description:
"""
from logging import Logger
import os

from yunjian.proj_common.common_util import exec_shell, ensure_dir_path
from yunjian.proj_common.log_track_util import log_track


class HbaseWriteHelper:

    def __init__(self,
                 logger: Logger,
                 hbase_table_name: str,
                 family_name: str,
                 shell_path,
                 hfile_absolute_path: str,
                 source_data_absolute_path: str,
                 source_csv_delimiter: str,
                 hbase_columns: str,
                 disable_hbase_table=False,
                 meta_table_name: str = '',
                 meta_row: str = '',
                 meta_shell_path=None,
                 hbase_table_number_regions: int = 50,
                 hbase_local_split_path: str = None):
        """
        hbase 导入帮助类,通过hbase命令读取csv文件，生成hfile，将hfile直接导入hbase
        除shell脚本路径检查，本类不处理数据及hdfs数据路径方面的任何异常，执行中如有其他问题，会直接报错,请注意核对
        如果hbase文件数据很大:
            如果数据比较均衡的，建议直接调整　hbase_table_number_regions　大小
            如果不均衡的，可指定本地的拆分文件，指定拆分文件后 hbase_table_number_regions 失效
        :param hbase_table_name:
        :param family_name:
        :param shell_path: create hbase table shell path ex: /home/hbase_shell/xxx/bulkLoad/table_1.shell
        :param hfile_absolute_path: ex: /user/proj_name/module_name/index_name/h_file/
        :param source_data_absolute_path: /user/proj_name/module_name/index_name/source_data/
        :param source_csv_delimiter:
        :param hbase_columns: ex: HBASE_ROW_KEY,info:cnt
        :param disable_hbase_table: default False, if need disable before write should set this True
        :param meta_table_name: allow null, meta not update if null
        :param meta_row: allow null if meta table name is null
        :param meta_shell_path: allow null if meta table is null
        :param hbase_table_number_regions: default 50
        :param hbase_local_split_path:
        """
        self.logger = logger
        self.hbase_table_name = hbase_table_name
        self.family_name = family_name
        self.shell_path = shell_path
        self.hfile_absolute_path = hfile_absolute_path
        self.meta_table_name = meta_table_name
        self.meta_row = meta_row
        self.meta_shell_path = meta_shell_path
        self.source_data_absolute_path = source_data_absolute_path
        self.source_csv_delimiter = source_csv_delimiter if source_csv_delimiter else ','
        self.hbase_columns = hbase_columns
        self.disable_table = disable_hbase_table
        self.hbase_table_number_regions = hbase_table_number_regions
        self.hbase_local_split_path = hbase_local_split_path
        self.default_dir = 'bulkLoad'
        self.has_meta = True if self.meta_table_name else False

    def _pre_handle_shell_path(self):
        if not self.shell_path:
            self.shell_path = os.path.join(self.default_dir, f'{self.hbase_table_name}_create.shell')
        ensure_dir_path(self.shell_path)

        if not self.has_meta:
            return

        if not self.meta_shell_path:
            self.meta_shell_path = os.path.join(self.default_dir, f'{self.hbase_table_name}_meta_create.shell')
        ensure_dir_path(self.meta_shell_path)
    @log_track()
    def exec_write(self):

        self.logger.info('begin handle shell path')
        self._pre_handle_shell_path()
        self.logger.info('handle shell path success')

        self.logger.info('begin create hbase table')
        self._create_hbase_table()
        self.logger.info('create hbase table success')

        self.logger.info('begin load to hbase')
        self._load_to_hbase()
        self.logger.info('load to hbase success')

        self.logger.info('begin update hbase meta data')
        self._update_meta_data()
        self.logger.info('update hbase meta data success')

    def _create_hbase_table(self):
        shell_command = self._gen_create_table_command()
        with open(self.shell_path, 'w') as f:
            f.write(shell_command)
        os.system('hbase shell ' + self.shell_path + '\n')

    def _gen_create_table_command(self):
        commands = list()

        commands.append(f"""flush '{self.hbase_table_name}'""")
        if self.disable_table:
            commands.append(f"""disable '{self.hbase_table_name}'""")
            commands.append(f"""drop '{self.hbase_table_name}'""")

        if self.hbase_local_split_path:
            commands.append(
                """create '%s', { NAME => '%s', COMPRESSION => 'snappy' }, SPLITS_FILE => '%split_file'"""
                % (self.hbase_table_name, self.family_name, self.hbase_local_split_path))
        else:
            commands.append(
                """create '%s', { NAME => '%s', COMPRESSION => 'snappy' }, {NUMREGIONS => %s, SPLITALGO => 'HexStringSplit'}"""
                % (self.hbase_table_name, self.family_name, self.hbase_table_number_regions))
        commands.append('exit')

        create_table_shell = '\n'.join(commands)
        self.logger.info(f"create table shell is:{create_table_shell}")
        return create_table_shell

    def _load_to_hbase(self):
        self._generate_hfile()

        load_hfile_cmd = 'hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles {hfile_path} {table_name}'\
            .format(hfile_path=self.hfile_absolute_path, table_name=self.hbase_table_name)
        exec_shell(load_hfile_cmd, "command_load_hfile ===> ")

    def _generate_hfile(self):
        if self.hfile_absolute_path == '/' or self.hfile_absolute_path == '\\' or len(self.hfile_absolute_path) < 4:
            return

        os.system("hadoop fs -rm -f -r -skipTrash " + self.hfile_absolute_path)
        self.logger.info(f'delete old hfile from {self.hfile_absolute_path} success')

        command_create_hfile = """hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dmapreduce.map.memory.mb=10240 -Dmapreduce.reduce.memory.mb=10240 -Dmapreduce.map.output.compress=true -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024 -Dimporttsv.separator="{delimiter}" -Dimporttsv.bulk.output={hfile_absolute_path} -Dimporttsv.columns={columns} {hbase_table_name} {source_data_absolute_path}
                                          """.format(
            delimiter=self.source_csv_delimiter,
            hfile_absolute_path=self.hfile_absolute_path,
            columns=self.hbase_columns,
            hbase_table_name=self.hbase_table_name,
            source_data_absolute_path=self.source_data_absolute_path)
        exec_shell(command_create_hfile, "command_create_hfile ===> ")

    def _update_meta_data(self):
        if not self.has_meta:
            self.logger.info('no meta found!')
            return
        meta_command = """put '{meta_table}','{meta_row}','f1:table','{table_name}'
        exit
        """.format(table_name=self.hbase_table_name, meta_table=self.meta_table_name, meta_row=self.meta_row)
        self.logger.info(f'update meta command is:{meta_command}')

        with open(self.meta_shell_path, 'w') as f:
            f.write(meta_command)

        os.system('hbase shell ' + self.meta_shell_path + '\n')
