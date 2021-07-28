from whetstone.core.entry import Entry

from zg11.biz_relation.IndexHandler import IndexHandler


def pre_check(entry: Entry):
    return True


def main(entry: Entry):

    spark = entry.spark
    version = entry.version
    logger = entry.logger
    entity_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path_entity")
    relation_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path_relation")
    IndexHandler(spark=spark,
                 version=version,
                 logger=logger,
                 entity_path=entity_path,
                 relation_path=relation_path).run()



def post_check(entry: Entry):
    return True