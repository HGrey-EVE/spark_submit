import os

from whetstone.core.entry import Entry

from hongjing.hongjing_zhaoping.WriteToHbase import WriteToHbase


def pre_check(entry: Entry):
    return True

def copy_hdfs(source_hdfs_path, target_hdfs_path):
    os.system("hadoop distcp -bandwidth 15 -m 50 -pb " + source_hdfs_path + " " + target_hdfs_path)
    # os.system('hadoop fs -rm -r ' + source_hdfs_path)
    print('加载hdfs成功')

'''
    1、将manage_recruit表数据插入manage_recruit_simple表
    2、将manage_recruit_simple表数据存入csv文件
'''
def main(entry: Entry):

    spark = entry.spark
    version = entry.version
    logger = entry.logger
    # 路径配置
    hdfs_source_path = os.path.join(entry.cfg_mgr.get("hdfs", "hdfs_out_path"), 'manage_recruit')
    hdfs_target_path = entry.cfg_mgr.get("hbase", "hdfs_target_path")
    hdfs_hfile_path = entry.cfg_mgr.get("hbase", "hdfs_hfile_path")
    # hbase配置
    table_name = entry.cfg_mgr.get("hbase", "table_name") + entry.version
    family_name = entry.cfg_mgr.get("hbase", "family_name")
    hbase_columns = entry.cfg_mgr.get("hbase", "hbase_columns")
    meta_table_name = entry.cfg_mgr.get("hbase", "meta_table_name")
    meta_row_key = entry.cfg_mgr.get("hbase", "meta_row_key")

    hdfs_target_path = os.path.join(hdfs_target_path, os.path.join(entry.version, 'manage_recruit'))
    copy_hdfs(hdfs_source_path, hdfs_target_path)

    WriteToHbase(version=version,
                 logger=logger,
                 table_name = table_name,
                 family_name = family_name,
                 hfile_absolute_path = hdfs_hfile_path,
                 index_path = hdfs_target_path,
                 hbase_columns = hbase_columns,
                 meta_table_name = meta_table_name,
                 meta_row_key = meta_row_key,
                 ).run()





def post_check(entry: Entry):
    return True