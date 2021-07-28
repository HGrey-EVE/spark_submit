from whetstone.core.entry import Entry

# from zg11sshx.biz_relation.IndexHandler import IndexHandler
from zg11.biz_property_conflict.IndexHandler import IndexHandler


def pre_check(entry: Entry):
    return True


def main(entry: Entry):

    spark = entry.spark
    version = entry.version
    logger = entry.logger
    entity_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path_entity")
    relation_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path_relation")
    qyxx_basic_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_qyxx_basic_path")
    hdfs_listed_person_path = entry.cfg_mgr.get("hdfs", "hdfs_listed_person_path")
    hdfs_listed_name_path = entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_listed_name_path")
    hdfs_conflict_path = entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_conflict_path")
    hdfs_black_path = entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_black_path")
    hdfs_basic_info_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_basic_info_path")

    IndexHandler(spark=spark,
                 version=version,
                 logger=logger,
                 entity_path=entity_path,
                 relation_path=relation_path,
                 qyxx_basic_path=qyxx_basic_path,
                 hdfs_listed_person_path=hdfs_listed_person_path,
                 hdfs_listed_name_path=hdfs_listed_name_path,
                 hdfs_conflict_path=hdfs_conflict_path,
                 hdfs_black_path=hdfs_black_path,
                 hdfs_basic_info_path=hdfs_basic_info_path).run()



def post_check(entry: Entry):
    return True