import os

from pyspark.sql import SparkSession, DataFrame

from logging import Logger

from pyspark.sql.types import StringType
from zg11.proj_common.hive_util import HiveUtil
import json

class IndexHandler:

    def __init__(self, spark: SparkSession,
                 version: str,
                 logger: Logger,
                 entity_path: str,
                 relation_path: str):
        self.spark = spark
        self.logger = logger
        self.version = version
        self.entity_path = entity_path
        self.relation_path = relation_path

    def run(self):
        self.logger.info("统一读取entity模块数据并创建临时视图")
        self.hive_pre_prepare()

        self.logger.info("统一注册udf函数")
        self.udf_register()

        self.logger.info("计算指标")
        self.handle_index()


    def handle_index(self):
        # 人-重要供应商-公司
        edge_person_supplier_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_gy') as uid,
                uid as startnode,
                relatedid as endnode,
                nameid as time,
                amount as trade
            from vertex_person
            where type = 'supplier'
        """)

        ## 公司-重要供应商-公司
        edge_company_supplier_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_gy') as uid,
                uid as startnode,
                relatedid as endnode,
                nameid as time,
                trade_amount as trade
            from vertex_company
            where relatedtype = '供应商'
        """)

        ## 人-重要客户-公司
        edge_person_majorcustomer_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_kh') as uid,
                uid as startnode,
                relatedid as endnode,
                nameid as time,
                amount as trade
            from vertex_person
            where type = 'customer'
        """)

        ## 公司-重要客户-公司
        edge_company_majorcustomer_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_kh') as uid,
                uid as startnode,
                relatedid as endnode,
                nameid as time,
                trade_amount as trade
            from vertex_company
            where relatedtype = '客户'
        """)

        # 电话-联系方式-公司
        edge_phonenumber_contactway_company = self.spark.sql("""
            select
                *,
                size(collect_set(endnode) over(partition by startnode)) contactconflict
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_dh') as uid,
                    L.uid as startnode,
                    L.relatedid as endnode,
                    R.name,
                    R.branch
                from vertex_phonenumber L
                join (select * from vertex_company where branch != '子公司' or branch != '分公司') R
                on L.relatedid = R.uid
            ) a
        """)
        #
        # ## 邮箱-联系方式-公司
        edge_email_contactway_company = self.spark.sql("""
            select
                *,
                size(collect_set(endnode) over(partition by startnode)) contactconflict
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_yx') as uid,
                    L.uid as startnode,
                    L.relatedid as endnode,
                    R.name,
                    R.branch
                from vertex_email L
                join (select * from vertex_company where branch != '子公司' or branch != '分公司') R
                on L.relatedid = R.uid
            ) a
        """)
        #
        # ## 公司-标识-组织机构代码
        edge_company_code_orgcode = self.spark.sql("""
            select
                uid,
                startnode,
                substring(endnode,9,9) endnode
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_bs') as uid,
                    bbd_qyxx_id as startnode,
                    regexp_replace(credit_code,'-','') as endnode
                from qyxx_basic
                where nvl(credit_code, '') != '' or credit_code = 'null' or credit_code is null
            ) a
        """)
        #
        # ## 公司-失信-失信名单
        edge_company_dishonest_bfat = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_sf') as uid,
                relatedid as startnode,
                uid endnode
            from vertex_bfat
        """)

        # 公司-注册地址-地址
        edge_company_regaddress_address = self.spark.sql("""
            select
                *,
                size(collect_set(endnode) over(partition by startnode)) addressconflict
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_dz') as uid,
                    L.relatedid as startnode,
                    L.uid as endnode,
                    L.detail as endnode_ori,
                    R.name,
                    R.branch
                from vertex_address L
                join (select * from vertex_company where branch != '子公司' or branch != '分公司') R
                on L.relatedid = R.uid
            ) a
        """)
        #
        # ## 人-高管-公司
        edge_person_managerof_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_gg') as uid,
                uid as startnode,
                relatedid as endnode
            from vertex_person
            where type rlike '经理|总监|总裁|首席'
        """)

        ## 人-出资-公司 -- ratio和amount的规则还没有写
        edge_person_invest_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_czr') as uid,
                uid as startnode,
                relatedid as endnode,
                ratio,
                amount,
                time
            from vertex_person
            where type = 'shareholder'
        """)

        ## 人-实际控制人-公司
        edge_person_actualcontrol_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_kzr') as uid,
                uid as startnode,
                relatedid as endnode,
                nameid as time
            from vertex_person
            where type = 'actualcontrol'
        """)

        ## 人-监事-公司
        edge_person_excutiveof_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_js') as uid,
                uid as startnode,
                relatedid as endnode
            from vertex_person
            where type like '%监事%'
        """)

        ## 人-董事-公司
        edge_person_chairmanof_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_ds') as uid,
                uid as startnode,
                relatedid as endnode
            from vertex_person
            where type like '%董事%'
        """)

        # 公司-出资-公司
        edge_company_invest_company = self.spark.sql("""
            select
                uid,
                startnode,
                endnode,
                ratio,
                amount,
                paid_date
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_czgs') as uid,
                    shareholder_id as startnode,
                    bbd_qyxx_id as endnode,
                    invest_ratio as ratio,
                    case when invest_amount = 0 then 'null'
                         when invest_amount = 0.0 then 'null'
                    else invest_amount end as amount,
                    udf_extract_paid_date_from_paid_detail(paid_detail) as paid_date
                from (select * from qyxx_gdxx where paid_detail != '[]' and nvl(paid_detail,'') != '' and paid_detail != 'null')
            ) a
            where nvl(paid_date,'') != '' and length(startnode) >= 4 and paid_date <= '2019-12-31'
        """)

        # 公司-实际控制人-公司
        edge_company_actualcontrol_company = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_kzgs') as uid,
                uid as startnode,
                relatedid as endnode,
                nameid as time
            from vertex_company
        """)

        ## 公司-涉及处罚-处罚
        edge_company_ispunished_punish = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_cf') as uid,
                relatedid as startnode,
                uid as endnode
            from vertex_punish
        """)

        ## 公司-涉及司法-司法
        edge_company_hasjudicial_judicial = self.spark.sql("""
            select
                concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'_sf') as uid,
                relatedid as startnode,
                uid as endnode
            from vertex_punish
        """)

        ## 人-工商法人-公司 临时视图
        self.spark.sql("""
            select
                L.uid,
                L.startnode,
                L.endnode,
                R.company_name as name
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'icfr') as uid,
                    uid as startnode,
                    relatedid as endnode
                from vertex_person where type = 'legal'
            ) L
            join qyxx_basic R
            on L.endnode = R.bbd_qyxx_id
        """).createOrReplaceTempView("edge_person_iclegal_company_1")

        ## 人-披露法人-公司 临时视图
        self.spark.sql("""
            select
                L.uid,
                L.startnode,
                L.endnode,
                R.company_name as name
            from (
                select
                    concat(regexp_replace(reflect('java.util.UUID', 'randomUUID'), '-', ''),'plfr') as uid,
                    uid as startnode,
                    relatedid as endnode
                from vertex_person where type = 'pllegal'
            ) L
            join qyxx_basic R
            on L.endnode = R.bbd_qyxx_id
        """).cache().createOrReplaceTempView("edge_person_pllegal_company_1")

        edge_person_iclegal_company = self.spark.sql("""
            select
                uid,
                startnode,
                endnode,
                name,
                case when startnode != pl_startnode then 1
                else 0 end as legalconflict
            from(
                select
                    L.uid,
                    L.startnode,
                    L.endnode,
                    L.name,
                    R.startnode as pl_startnode
                from edge_person_iclegal_company_1 L
                left join edge_person_pllegal_company_1 R
                on L.endnode = R.endnode
            ) a
        """)

        edge_person_iclegal_company.createOrReplaceTempView("edge_person_iclegal_company_for_pllegal")
        edge_person_pllegal_company = self.spark.sql("""
            select
                L.uid,
                L.startnode,
                L.endnode,
                L.name,
                case when nvl(R.legalconflict, '') != '' then 0
                else R.legalconflict end as legalconflict
            from edge_person_pllegal_company_1 L
            left join edge_person_iclegal_company_for_pllegal R
            on L.endnode = R.endnode
        """)


        count = 0
        for index in [
                  [edge_person_supplier_company, "edge_person_supplier_company"],
                  [edge_company_supplier_company, "edge_company_supplier_company"],
                  [edge_person_majorcustomer_company, "edge_person_majorcustomer_company"],
                  [edge_company_majorcustomer_company, "edge_company_majorcustomer_company"],
                  [edge_phonenumber_contactway_company, "edge_phonenumber_contactway_company"],
                  [edge_email_contactway_company, "edge_email_contactway_company"],
                  [edge_company_code_orgcode, "edge_company_code_orgcode"],
                  [edge_company_dishonest_bfat, "edge_company_dishonest_bfat"],
                  [edge_company_regaddress_address, "edge_company_regaddress_address"],
                  [edge_person_managerof_company, "edge_person_managerof_company"],
                  [edge_person_invest_company, "edge_person_invest_company"],
                  [edge_person_actualcontrol_company, "edge_person_actualcontrol_company"],
                  [edge_person_excutiveof_company, "edge_person_excutiveof_company"],
                  [edge_person_chairmanof_company, "edge_person_chairmanof_company"],
                  [edge_company_invest_company, "edge_company_invest_company"],
                  [edge_company_actualcontrol_company, "edge_company_actualcontrol_company"],
                  [edge_company_ispunished_punish, "edge_company_ispunished_punish"],
                  [edge_company_hasjudicial_judicial, "edge_company_hasjudicial_judicial"],
                  [edge_person_iclegal_company, "edge_person_iclegal_company"],
                  [edge_person_pllegal_company, "edge_person_pllegal_company"]
                  ]:
            self.save_parquet(index[0], index[1])
            self.logger.info(f"{index[0]} finished")
            count += 1

        self.logger.info(f"总共执行完成的指标个数为 {count}, finished! ")

    def hive_pre_prepare(self):
        ## 拼接来源于上游数据 - entity模块
        person_path = os.path.join(self.entity_path,
                                   "vertex_person")
        company_path = os.path.join(self.entity_path,
                                    "vertex_company")
        phonenumber_path = os.path.join(self.entity_path,
                                    "vertex_phonenumber")
        email_path = os.path.join(self.entity_path,
                                    "vertex_email")
        bfat_path = os.path.join(self.entity_path,
                                    "vertex_bfat")
        address_path = os.path.join(self.entity_path,
                                    "vertex_address")
        punish_path = os.path.join(self.entity_path,
                                    "vertex_punish")
        judical_path = os.path.join(self.entity_path,
                                    "vertex_judicial")
        qyxx_basic_path = "202106/tmp/basic/qyxx_basic/"
        ## 映射关系 path : table
        mapping = {
            person_path: "vertex_person",
            company_path: "vertex_company",
            phonenumber_path: "vertex_phonenumber",
            email_path: "vertex_email",
            bfat_path: "vertex_bfat",
            address_path: "vertex_address",
            punish_path: "vertex_punish",
            judical_path: "vertex_judcial"
        }
        ## 根据映射关系创建临时视图
        for k in mapping:
            self.spark.read.parquet(k).createOrReplaceTempView(mapping[k])

        ## 读取qyxx_basic
        self.spark.read.parquet(qyxx_basic_path).createOrReplaceTempView("qyxx_basic")

        ## qyxx_gdxx表预处理
        qyxx_gdxx_max_dt = HiveUtil.get_month_newest_partition(self.spark, "qyxx_gdxx", self.version[:6])
        self.spark.sql(f"select * from dw.qyxx_gdxx where dt = '{qyxx_gdxx_max_dt}'").createOrReplaceTempView(
            "qyxx_gdxx")
    @classmethod
    def udf_extract_paid_date_from_paid_detail(cls, paid_detail: str) -> str:
        result = ""
        if len(paid_detail) > 0 or paid_detail is not None:
            detail_dic: dict = json.loads(paid_detail)[0]
            result = detail_dic.get("paid_date", "")
        return result


    def udf_register(self):
        self.spark.udf.register("udf_extract_paid_date_from_paid_detail", IndexHandler.udf_extract_paid_date_from_paid_detail, StringType())


    def save_parquet(self, index: DataFrame, index_name: str):
        index.write.parquet(os.path.join(self.relation_path,
                                     index_name), mode='overwrite')








