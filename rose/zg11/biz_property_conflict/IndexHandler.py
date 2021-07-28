import os
from logging import Logger

from pyspark.sql import SparkSession, DataFrame
from zg11.proj_common.hive_util import HiveUtil


class IndexHandler:

    def __init__(self, spark: SparkSession,
                 version: str,
                 logger: Logger,
                 entity_path: str,
                 relation_path: str,
                 qyxx_basic_path: str,
                 hdfs_listed_person_path: str,
                 hdfs_listed_name_path: str,
                 hdfs_conflict_path: str,
                 hdfs_black_path: str,
                 hdfs_basic_info_path: str
                 ):
        self.spark = spark
        self.logger = logger
        self.version = version
        self.entity_path = entity_path
        self.relation_path = relation_path
        self.qyxx_basic_path = qyxx_basic_path
        self.hdfs_listed_person_path = hdfs_listed_person_path
        self.hdfs_listed_name_path = hdfs_listed_name_path
        self.hdfs_conflict_path = hdfs_conflict_path
        self.hdfs_black_path = hdfs_black_path
        self.hdfs_basic_info_path = hdfs_basic_info_path

    def run(self):
        self.logger.info("统一读取entity、relation模块数据并创建临时视图")
        self.hive_pre_prepare()

        self.logger.info("计算指标")
        self.handle_index()

    def handle_index(self):
        ## 法人冲突  -- valueone && valuetwo not write
        risk_confilict_legal = self.spark.sql("""
            select
                L.uid,
                L.name,
                L.conflictprop,
                L.sourceone,
                L.sourcetwo,
                M.name as valueone,
                M.name as valuetwo
            from (select
                endnode as uid,
                name,
                startnode,
                '法人' as conflictprop,
                '工商' as sourceone,
                '披露' as sourcetwo
            from edge_person_iclegal_company
            where legalconflict != 0) L
            left join vertex_person M
            on L.startnode = M.uid
        """).dropDuplicates(['uid'])

        # 联系电话冲突
        risk_confilict_phone = self.spark.sql("""
            select
                phone,
                name,
                uid,
                typestr,
                type,
                branch
            from (
                select
                    *,
                    row_number() over(partition by phone order by branch) phone_cnt
                from (
                    select
                        phone,
                        name,
                        max(uid) uid,
                        'company' as typestr,
                        '3' as type,
                        max(branch) branch
                    from (
                        select
                            a.startnode as phone,
                            a.name,
                            a.endnode as uid,
                            a.branch
                        from (
                            select
                                L.startnode,
                                L.endnode,
                                L.name,
                                L.branch
                            from (select * from edge_phonenumber_contactway_company where contactconflict > 1) L
                            join vertex_phonenumber R
                            on L.startnode = R.uid
                        ) a
                        join vertex_company b
                        on a.endnode = b.uid
                    ) m
                    group by phone, name
                ) p
            ) q
            where phone_cnt != 1
        """)

        # 联系邮箱冲突
        risk_confilict_email = self.spark.sql("""
            select
                email,
                name,
                uid,
                typestr,
                type,
                branch
            from (
                select
                    *,
                    row_number() over(partition by email order by email) email_cnt
                from (
                    select
                        email,
                        name,
                        max(uid) uid,
                        'company' as typestr,
                        '3' as type,
                        max(branch) branch
                    from (
                        select
                            a.startnode as email,
                            a.name,
                            a.endnode as uid,
                            a.branch
                        from (
                            select
                                L.startnode,
                                L.endnode,
                                L.name,
                                L.branch
                            from (select * from edge_email_contactway_company where contactconflict > 1) L
                            join vertex_email R
                            on L.startnode = R.uid
                        ) a
                        join vertex_company b
                        on a.endnode = b.uid
                    ) m
                    group by email, name
                ) p
            ) q
            where email_cnt != 1
        """)

        # 注册地址冲突
        risk_confilict_address = self.spark.sql("""
            select
                address,
                name,
                uid,
                typestr,
                type,
                branch
            from (
                select
                    *,
                    row_number() over(partition by address order by address) address_cnt
                from (
                    select
                        address,
                        name,
                        max(uid) uid,
                        'company' as typestr,
                        '3' as type,
                        max(branch) branch
                    from (
                        select
                            a.endnode_ori as address,
                            a.name,
                            a.startnode as uid,
                            a.branch
                        from (
                            select
                                L.endnode_ori,
                                L.startnode,
                                L.name,
                                L.branch
                            from (select * from edge_company_regaddress_address where addressconflict > 1) L
                            join vertex_address R
                            on L.endnode = R.uid
                        ) a
                        join vertex_company b
                        on a.startnode = b.uid
                    ) m
                    group by address, name
                ) p
            ) q
            where address_cnt != 1
        """)

        blacklist = self.spark.sql("""
            SELECT
                a.startnode AS uid,
                b.ipo AS orgtype,
                '行政处罚' AS category,
                c.punishdate AS time,
                c.punishcontent AS cause,
                c.punishcode AS code
            FROM edge_company_ispunished_punish a
            JOIN vertex_company b
            ON a.startnode=b.uid
            JOIN vertex_punish c
            ON a.endnode=c.uid
            UNION
            SELECT
                a.startnode AS uid,
                b.ipo AS orgtype,
                '失信被执行人' AS category,
                c.date AS time,
                c.cause,
                c.code
            FROM edge_company_dishonest_bfat a
            JOIN vertex_company b
            ON a.startnode=b.uid
            JOIN vertex_bfat c
            ON a.endnode=c.uid
        """)

        '''
    对外投资信息
        '''
        out_invest = self.spark.sql("""
                with qyxx_frinv_dt as
                (select bbd_qyxx_id,frname,entname
                from qyxx_frinv 
                ),
                part_2_1 as(
                select l.code,s.name,s.uuid
                from listed_name l 
                join listed_person_unique s 
                on l.bbd_qyxx_id=s.bbd_qyxx_id 
                ),
                part_2_2 as(
                select l.code,l.name,s.endnode
                from part_2_1 l 
                join edge_person_invest_company s
                on l.uuid=s.startnode 
                )
                select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                        replace(coalesce(s.frname, ''),'null','') name, 
                        replace(coalesce(s.entname, ''),'null','') company_name, 
                        concat(cast(regexp_replace(r.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                        replace(coalesce(r.esdate, ''),'null','') esdate, 
                        replace(coalesce(r.address, ''),'null','') address                                
                from listed_name l 
                join qyxx_frinv s
                on l.bbd_qyxx_id=s.bbd_qyxx_id 
                join tmp_qyxx_basic_dt r 
                on s.entname = r.company_name
                union
                select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                                        replace(coalesce(l.name, ''),'null','') name, 
                                        replace(coalesce(s.company_name, ''),'null','') company_name, 
                                        concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                                        replace(coalesce(s.esdate, ''),'null','') esdate, 
                                        replace(coalesce(s.address, ''),'null','') address

                from part_2_2 l 
                join tmp_qyxx_basic_dt s
                on l.endnode=s.bbd_qyxx_id 
                """).dropDuplicates(["sec_code", "name", "company_name"])

        out_position = self.spark.sql("""
                with  qyxx_frposition_dt as
                (select bbd_qyxx_id,frname,entname,position
                from qyxx_frposition 
                ),
                part_2 as(
                select l.code,s.name,s.uuid
                from listed_name l 
                join listed_person_unique s
                on l.bbd_qyxx_id=s.bbd_qyxx_id 
                ),
                part_3_1 as(
                select l.code,l.name,s.endnode
                from part_2 l 
                join edge_person_chairmanof_company s
                on l.uuid=s.startnode 
                ),
                part_3_2 as(
                select l.code,l.name,s.endnode
                from part_2 l 
                join edge_person_excutiveof_company s
                on l.uuid=s.startnode 
                ),
                part_3_3 as(
                select l.code,l.name,s.endnode
                from part_2 l 
                join edge_person_managerof_company s
                on l.uuid=s.startnode 
                )
                select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                                        replace(coalesce(s.frname, ''),'null','') name, 
                                        replace(coalesce(s.entname, ''),'null','') company_name, 
                                        replace(coalesce(s.position, ''),'null','')  position,
                                        concat(cast(regexp_replace(r.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                                        replace(coalesce(cast(r.esdate as string), ''),'null','') esdate, 
                                        replace(coalesce(r.address, ''),'null','') address              
                from listed_name l 
                join qyxx_frposition s
                on l.bbd_qyxx_id=s.bbd_qyxx_id 
                join tmp_qyxx_basic_dt r 
                on s.entname = r.company_name    
                union
                select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                                        replace(coalesce(l.name, ''),'null','') name, 
                                        replace(coalesce(s.company_name, ''),'null','') company_name, 
                                        '董事' position,
                                        concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                                        replace(coalesce(cast(s.esdate as string), ''),'null','') esdate, 
                                        replace(coalesce(s.address, ''),'null','') address                             
                from part_3_1 l 
                join tmp_qyxx_basic_dt s
                on l.endnode=s.bbd_qyxx_id        
                union
                select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                                        replace(coalesce(l.name, ''),'null','') name, 
                                        replace(coalesce(s.company_name, ''),'null','') company_name, 
                                        '监事' position,
                                        concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                                        replace(coalesce(s.esdate, ''),'null','') esdate, 
                                        replace(coalesce(s.address, ''),'null','') address                                
                from part_3_2 l 
                join tmp_qyxx_basic_dt s
                on l.endnode=s.bbd_qyxx_id        
                union
                select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                                        replace(coalesce(l.name, ''),'null','') name, 
                                        replace(coalesce(s.company_name, ''),'null','') company_name, 
                                        '高管' position,
                                        concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                                        replace(coalesce(s.esdate, ''),'null','') esdate, 
                                        replace(coalesce(s.address, ''),'null','') address                               
                from part_3_3 l 
                join tmp_qyxx_basic_dt s
                on l.endnode=s.bbd_qyxx_id 
            """).dropDuplicates(["sec_code", "name", "company_name"])

        for index in [
            [risk_confilict_legal, "risk_confilict_legal"],
            [risk_confilict_phone, "risk_confilict_phone"],
            [risk_confilict_email, "risk_confilict_email"],
            [risk_confilict_address, "risk_confilict_address"],
        ]:
            index[0].write.json(os.path.join(self.hdfs_conflict_path, index[1]), mode='overwrite')
            self.logger.info(f"{index[0]} finished")

        for index in [
            [blacklist, "blacklist"],
        ]:
            index[0].write.json(os.path.join(self.hdfs_black_path, index[1]), mode='overwrite')
            self.logger.info(f"{index[0]} finished")

        for index in [
            [out_position, "out_position"],
            [out_invest, "out_invest"],
        ]:
            index[0].write.json(os.path.join(self.hdfs_basic_info_path, index[1]), mode='overwrite')
            self.logger.info(f"{index[0]} finished")



        self.logger.info(f"总共执行完成的指标个数为 1, finished! ")


    def hive_pre_prepare(self):
        ## 拼接来源于上游数据 - 关系模块
        person_iclegal_company_path = os.path.join(self.relation_path,"edge_person_iclegal_company")
        person_pllegal_company_path = os.path.join(self.relation_path,"edge_person_pllegal_company")
        phonenumber_contactway_company_path = os.path.join(self.relation_path,"edge_phonenumber_contactway_company")
        email_contactway_company_path = os.path.join(self.relation_path,"edge_email_contactway_company")
        company_regaddress_address_path = os.path.join(self.relation_path,"edge_company_regaddress_address")
        company_dishonest_bfat_path = os.path.join(self.relation_path,"edge_company_dishonest_bfat")
        company_ispunished_punish_path = os.path.join(self.relation_path,"edge_company_ispunished_punish")
        person_chairmanof_company_path = os.path.join(self.relation_path,"edge_person_chairmanof_company")
        person_excutiveof_company_path = os.path.join(self.relation_path,"edge_person_excutiveof_company")
        person_managerof_company_path = os.path.join(self.relation_path,"edge_person_managerof_company")
        person_invest_company_path = os.path.join(self.relation_path,"edge_person_invest_company")

        company_path = os.path.join(self.entity_path,"vertex_company")
        phonenumber_path = os.path.join(self.entity_path,"vertex_phonenumber")
        email_path = os.path.join(self.entity_path,"vertex_email")
        address_path = os.path.join(self.entity_path,"vertex_address")
        person_path = os.path.join(self.entity_path,"vertex_person")
        punish_path = os.path.join(self.entity_path,"vertex_punish")
        bfat_path = os.path.join(self.entity_path,"vertex_bfat")


        ## 映射关系 path : table
        mapping_edge = {
            person_iclegal_company_path: "edge_person_iclegal_company",
            person_pllegal_company_path: "edge_person_pllegal_company",
            phonenumber_contactway_company_path: "edge_phonenumber_contactway_company",
            email_contactway_company_path: "edge_email_contactway_company",
            company_regaddress_address_path: "edge_company_regaddress_address",
            company_dishonest_bfat_path: "edge_company_dishonest_bfat",
            company_ispunished_punish_path: "edge_company_ispunished_punish",
            person_chairmanof_company_path: "edge_person_chairmanof_company",
            person_excutiveof_company_path: "edge_person_excutiveof_company",
            person_managerof_company_path: "edge_person_managerof_company",
            person_invest_company_path: "edge_person_invest_company"
        }
        ## 根据映射关系创建临时视图
        for k in mapping_edge:
            self.spark.read.parquet(k).createOrReplaceTempView(mapping_edge[k])

        mapping_vertex = {
            company_path: "vertex_company",
            phonenumber_path: "vertex_phonenumber",
            email_path: "vertex_email",
            address_path: "vertex_address",
            person_path: "vertex_person",
            punish_path: "vertex_punish",
            bfat_path: "vertex_bfat"
        }
        for k in mapping_vertex:
            self.spark.read.parquet(k).createOrReplaceTempView(mapping_vertex[k])

        qyxx_frinv_dt = HiveUtil.get_month_newest_partition(self.spark, "qyxx_frinv", self.version[:6])
        self.spark.sql(f"select * from dw.qyxx_frinv where dt = '{qyxx_frinv_dt}'").createOrReplaceTempView("qyxx_frinv")
        qyxx_frposition_dt = HiveUtil.get_month_newest_partition(self.spark, "qyxx_frposition", self.version[:6])
        self.spark.sql(f"select * from dw.qyxx_frposition where dt = '{qyxx_frposition_dt}'").createOrReplaceTempView("qyxx_frposition")

        self.spark.read.csv(self.hdfs_listed_person_path, sep=',',header=True).createOrReplaceTempView("listed_person_unique")
        self.spark.read.parquet(self.qyxx_basic_path).createOrReplaceTempView("tmp_qyxx_basic_dt")
        self.spark.read.json(self.hdfs_listed_name_path).createOrReplaceTempView("listed_name")
