from whetstone.core.entry import Entry

from hongjing.hongjing_zhaoping.IndexHandler import IndexHandler


def pre_check(entry: Entry):
    return True

'''
    1、将manage_recruit表数据插入manage_recruit_simple表
    2、将manage_recruit_simple表数据存入csv文件
'''
def main(entry: Entry):

    spark = entry.spark
    version = entry.version
    logger = entry.logger
    output_path = entry.cfg_mgr.get("hdfs", "hdfs_out_path")
    IndexHandler(spark=spark,
                 version=version,
                 logger=logger,
                 output_path=output_path).run()





def post_check(entry: Entry):
    return True