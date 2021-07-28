# encoding:utf-8
__author__ = 'houguanyu'

from whetstone.core.entry import Entry
from ..proj_common.hive_util import HiveUtil
from datetime import date
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

'''
项目：临时导数据,信用中心数据统计需求
数据源：dw数据库表 

'''


def pre_check(entry: Entry):
    return True


def xuqiu2_1(entry: Entry):
    spark = entry.spark
    spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            bbd_xgxx_id,
            punish_date,
            year(punish_date) year,
            month(punish_date) month,
            CASE  WHEN punish_fact REGEXP '(食物.*?中毒)|(过期.*?食品)|(问题食品)|(过期.*?饮料)|食品安全|有害食品' THEN '食品安全'
            WHEN punish_fact REGEXP '(保健品.*?(欺诈|欺骗))|(假冒.*?保健品)' THEN '保健品欺诈'
            WHEN punish_fact REGEXP '逃废债|反催收|恶意逾期|恶意欠款' THEN '逃废债'
            WHEN punish_fact REGEXP '((网贷|P2P).*?(坏账|违规|暴力催收|诈骗|违约|虚假))|砍头息' THEN '网贷欺诈'
            WHEN punish_fact REGEXP '(债务.*?违约)|(拖欠.*?债务)|(借贷.*?违约)|(债务.*?逾期)' THEN '债务违约'
            WHEN punish_fact REGEXP '(债券.*?违约)|(债券.*?逾期)|(票据.*?违约)|(票据.*?逾期)' THEN '债券违约'
            WHEN punish_fact REGEXP '套路贷' THEN '套路贷'
            WHEN punish_fact REGEXP '骗保|骗赔|假保单' THEN '骗保' 
            WHEN punish_fact REGEXP '(非法集资)|(集资.*?诈骗)|(非法.*?吸收.*?存款)' THEN '非法集资/集资诈骗' 
            WHEN punish_fact REGEXP '强制购物|强制消费' THEN '强制消费'
            WHEN punish_fact REGEXP '传销' THEN '传销'
            WHEN punish_fact REGEXP '无证行医|非法行医|非法医疗|非医师行医|擅自从事医疗活动' THEN '非法行医'
            WHEN punish_fact REGEXP '假药|过期疫苗|(非法.*?药物.*?生产)' THEN '制售假药'
            WHEN punish_fact REGEXP '(互联网|网络|平台|直播|视频|网站).*?(色情|情色|涉黄)' THEN '互联网不良信息'
            WHEN punish_fact REGEXP '((电信|网络|电话).*?诈骗)|钓鱼网站|伪基站|黑广播' THEN '电信网络诈骗'
            WHEN punish_fact REGEXP '(违法.*?排污)|环境污染|环境污染' THEN '严重污染环境'
            WHEN punish_fact REGEXP
             '不合理低价游|黑导游|非法一日游|((旅游|导游|旅行社|景区|游客).*?(违法|欺客|宰客|强制消费|误导消费者|天价))' THEN '旅游乱象'
            WHEN punish_fact REGEXP '(生产|安全|责任).*?事故' THEN '生产安全责任事故'
            WHEN punish_fact REGEXP '((不合格|伪劣|三无|假冒).*?产品)|(产品.*?不合格)' THEN '不合格产品'
            WHEN punish_fact REGEXP '(论文|学术|考试|学位|学历|招生|科研|研究|文凭).*?(造假|抄袭|作弊|不端|作假)' 
                THEN '学术/科研/论文造假'
            WHEN punish_fact REGEXP '((侵犯|侵权).*?(知识产权|商标|专利|版权|著作权))|盗版|剽窃|盗用|侵权' THEN '侵犯知识产权'
            WHEN punish_fact REGEXP '(家政|保姆|看护|月嫂|护工).*?(虚假|无证|违反道德|信息不明|盗窃|殴打|虐待|杀|谋害|纵火|毒死|
                摧残|折磨|侮辱|咒骂|讽刺|护理不当|投诉|惩罚|处罚|恶意|不正当|哄抬|误导消费)' THEN '家政乱象'
            WHEN punish_fact REGEXP '失信被执行人' THEN '法院判决不执行'
            WHEN punish_fact REGEXP '合同.*?欺诈' THEN '合同欺诈'
            WHEN punish_fact REGEXP '逃税|骗税|偷税|漏税|假发票|(发票.*？造假)|(虚开.*?发票)|(伪造.*?发票)|
                (编造.*?发票)|(造假.*?发票)|税务违法' THEN '偷税漏税/虚开发票'
            WHEN punish_fact REGEXP '(虚假.*?宣传)|(虚假.*?广告)|(虚假.*?营销)|(夸大.*?宣传)|(失实.*?宣传)' THEN '虚假广告/虚假营销'
            WHEN punish_fact REGEXP '((伪造|虚假|假造|造假|虚构|捏造|不实|失实|作假).*?(材料|证明|证件|印章|公文|票据))
                |((材料|证明|证件|印章|公文|票据).*?(伪造|虚假|假造|造假|虚构|捏造|不实|失实|作假))' THEN '伪造材料证明'
            WHEN punish_fact REGEXP '(拖欠.*?(工资|薪资))|欠薪' THEN '拖欠工资'
            WHEN punish_fact REGEXP '(社会组织.*?失信)|(非法.*?社会组织)|(非法.*?组织)|(山寨.*?组织)|(山寨.*?社会组织)'
               THEN '非法社会组织'
            WHEN punish_fact REGEXP '长租公寓.*?(爆雷|欺诈|欺骗|隐形收费|租金贷|租房贷|失信|违建|违章建筑|污染)' THEN '长租公寓'
            WHEN punish_fact REGEXP '(培训机构|教育机构).*?(停业|((培训|课程费|学费).*?违约)|倒闭|爆雷|跑路)|培训贷'
               THEN '教育机构失信'
            WHEN punish_fact REGEXP '直播.*?(带货|销售).*?(造假|虚假宣传|夸大|刷单|欺诈|误导|诱导|违规|欺骗|诈骗|三无产品|质量不合格)'
                THEN '直播带货失信'
            WHEN punish_fact REGEXP '(虚增.*?(利润|资产|成本))|(财务.*?(造假|虚报))' THEN '财务造假' else "" end keyword
            FROM  dw.risk_punishment 
            WHERE  dt = {}
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.risk_punishment"))).createOrReplaceTempView("xuqiu2_1_temp")

    xuqiu2_1 = spark.sql(
        '''
        SELECT
            keyword, 
            month, 
            year, 
            company_province,
            company_industry,
            count(DISTINCT xuqiu2_1_temp.bbd_qyxx_id) qy_count,
            count(DISTINCT bbd_xgxx_id) rc_count
        from xuqiu2_1_temp join 
        (select 
                bbd_qyxx_id,
                company_province,
                company_industry,
                company_companytype 
            from  dw.qyxx_basic 
            where dt={}) cr
        on xuqiu2_1_temp.bbd_qyxx_id = cr.bbd_qyxx_id
        where (keyword != "") 
            and (company_companytype not in ('9100','9200','9300','9310','9320')) 
            and (punish_date between date('2019-01-01') and date('2020-12-31'))
        group by year,month,company_province,company_industry,keyword
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.qyxx_basic")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu2_1_data")
    xuqiu2_1.repartition(1).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')


def xuqiu2_2(entry: Entry):
    spark = entry.spark
    spark.sql(
        '''
        SELECT
            bbd_qyxx_id,
            count(bbd_xgxx_id) rc_count_temp,
            year(sentence_date) year,
            month(sentence_date) month,
            CASE WHEN notice_content REGEXP '(食物.*?中毒)|(过期.*?食品)|(问题食品)|(过期.*?饮料)|食品安全|有害食品' THEN '食品安全'
            WHEN notice_content REGEXP '(保健品.*?(欺诈|欺骗))|(假冒.*?保健品)' THEN '保健品欺诈'
            WHEN notice_content REGEXP '逃废债|反催收|恶意逾期|恶意欠款' THEN '逃废债'
            WHEN notice_content REGEXP '((网贷|P2P).*?(坏账|违规|暴力催收|诈骗|违约|虚假))|砍头息' THEN '网贷欺诈'
            WHEN notice_content REGEXP '(债务.*?违约)|(拖欠.*?债务)|(借贷.*?违约)|(债务.*?逾期)' THEN '债务违约'
            WHEN notice_content REGEXP '(债券.*?违约)|(债券.*?逾期)|(票据.*?违约)|(票据.*?逾期)' THEN '债券违约'
            WHEN notice_content REGEXP '套路贷' THEN '套路贷'
            WHEN notice_content REGEXP '骗保|骗赔|假保单' THEN '骗保' 
            WHEN notice_content REGEXP '(非法集资)|(集资.*?诈骗)|(非法.*?吸收.*?存款)' THEN '非法集资/集资诈骗' 
            WHEN notice_content REGEXP '强制购物|强制消费' THEN '强制消费'
            WHEN notice_content REGEXP '传销' THEN '传销'
            WHEN notice_content REGEXP '无证行医|非法行医|非法医疗|非医师行医|擅自从事医疗活动' THEN '非法行医'
            WHEN notice_content REGEXP '假药|过期疫苗|(非法.*?药物.*?生产)' THEN '制售假药'
            WHEN notice_content REGEXP '(互联网|网络|平台|直播|视频|网站).*?(色情|情色|涉黄)' THEN '互联网不良信息'
            WHEN notice_content REGEXP '((电信|网络|电话).*?诈骗)|钓鱼网站|伪基站|黑广播' THEN '电信网络诈骗'
            WHEN notice_content REGEXP '(违法.*?排污)|环境污染|环境污染' THEN '严重污染环境'
            WHEN notice_content REGEXP
             '不合理低价游|黑导游|非法一日游|((旅游|导游|旅行社|景区|游客).*?(违法|欺客|宰客|强制消费|误导消费者|天价))' THEN '旅游乱象'
            WHEN notice_content REGEXP '(生产|安全|责任).*?事故' THEN '生产安全责任事故'
            WHEN notice_content REGEXP '((不合格|伪劣|三无|假冒).*?产品)|(产品.*?不合格)' THEN '不合格产品'
            WHEN notice_content REGEXP '(论文|学术|考试|学位|学历|招生|科研|研究|文凭).*?(造假|抄袭|作弊|不端|作假)' 
                THEN '学术/科研/论文造假'
            WHEN notice_content REGEXP '((侵犯|侵权).*?(知识产权|商标|专利|版权|著作权))|盗版|剽窃|盗用|侵权' THEN '侵犯知识产权'
            WHEN notice_content REGEXP '(家政|保姆|看护|月嫂|护工).*?(虚假|无证|违反道德|信息不明|盗窃|殴打|虐待|杀|谋害|纵火|毒死|
                摧残|折磨|侮辱|咒骂|讽刺|护理不当|投诉|惩罚|处罚|恶意|不正当|哄抬|误导消费)' THEN '家政乱象'
            WHEN notice_content REGEXP '失信被执行人' THEN '法院判决不执行'
            WHEN notice_content REGEXP '合同.*?欺诈' THEN '合同欺诈'
            WHEN notice_content REGEXP '逃税|骗税|偷税|漏税|假发票|(发票.*？造假)|(虚开.*?发票)|(伪造.*?发票)|
                (编造.*?发票)|(造假.*?发票)|税务违法' THEN '偷税漏税/虚开发票'
            WHEN notice_content REGEXP '(虚假.*?宣传)|(虚假.*?广告)|(虚假.*?营销)|(夸大.*?宣传)|(失实.*?宣传)' THEN '虚假广告/虚假营销'
            WHEN notice_content REGEXP '((伪造|虚假|假造|造假|虚构|捏造|不实|失实|作假).*?(材料|证明|证件|印章|公文|票据))
                |((材料|证明|证件|印章|公文|票据).*?(伪造|虚假|假造|造假|虚构|捏造|不实|失实|作假))' THEN '伪造材料证明'
            WHEN notice_content REGEXP '(拖欠.*?(工资|薪资))|欠薪' THEN '拖欠工资'
            WHEN notice_content REGEXP '(社会组织.*?失信)|(非法.*?社会组织)|(非法.*?组织)|(山寨.*?组织)|(山寨.*?社会组织)'
               THEN '非法社会组织'
            WHEN notice_content REGEXP '长租公寓.*?(爆雷|欺诈|欺骗|隐形收费|租金贷|租房贷|失信|违建|违章建筑|污染)' THEN '长租公寓'
            WHEN notice_content REGEXP '(培训机构|教育机构).*?(停业|((培训|课程费|学费).*?违约)|倒闭|爆雷|跑路)|培训贷'
               THEN '教育机构失信'
            WHEN notice_content REGEXP '直播.*?(带货|销售).*?(造假|虚假宣传|夸大|刷单|欺诈|误导|诱导|违规|欺骗|诈骗|三无产品|质量不合格)'
                THEN '直播带货失信'
            WHEN notice_content REGEXP '(虚增.*?(利润|资产|成本))|(财务.*?(造假|虚报))' THEN '财务造假' else 'null' end keyword
        FROM dw.legal_adjudicative_documents 
        WHERE dt = {} and bbd_qyxx_id != 'null' and (sentence_date between date('2019-01-01') and date('2020-12-31'))
        group by year,month,bbd_qyxx_id,keyword
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.legal_adjudicative_documents"))).createOrReplaceTempView("xuqiu2_2_temp")

    xuqiu2_2 = spark.sql(
        '''
        SELECT
            keyword,
            month,
            year,
            company_province,
            company_industry,
            count(xuqiu2_2_temp.bbd_qyxx_id) qy_count,
            sum(rc_count_temp) rc_count
        from xuqiu2_2_temp join (select 
            bbd_qyxx_id,
            company_province,
            company_industry,
            company_companytype 
        from  dw.qyxx_basic 
        where dt={}) cr
        on xuqiu2_2_temp.bbd_qyxx_id = cr.bbd_qyxx_id
        where (company_companytype not in ('9100','9200','9300','9310','9320'))  and (keyword != 'null')
        group by year,month,company_province,company_industry,keyword
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.qyxx_basic")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu2_2_data")
    xuqiu2_2.repartition(100).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')

def xuqiu2_3(entry: Entry):
    spark = entry.spark
    spark.sql('''
        SELECT
            bbd_qyxx_id,
            count(bbd_xgxx_id) rc_count_temp,
            year(sentence_date) year,
            month(sentence_date) month,
            case_results,
            CASE WHEN notice_content REGEXP '(食物.*?中毒)|(过期.*?食品)|(问题食品)|(过期.*?饮料)|食品安全|有害食品' THEN '食品安全'
            WHEN notice_content REGEXP '(保健品.*?(欺诈|欺骗))|(假冒.*?保健品)' THEN '保健品欺诈'
            WHEN notice_content REGEXP '逃废债|反催收|恶意逾期|恶意欠款' THEN '逃废债'
            WHEN notice_content REGEXP '((网贷|P2P).*?(坏账|违规|暴力催收|诈骗|违约|虚假))|砍头息' THEN '网贷欺诈'
            WHEN notice_content REGEXP '(债务.*?违约)|(拖欠.*?债务)|(借贷.*?违约)|(债务.*?逾期)' THEN '债务违约'
            WHEN notice_content REGEXP '(债券.*?违约)|(债券.*?逾期)|(票据.*?违约)|(票据.*?逾期)' THEN '债券违约'
            WHEN notice_content REGEXP '套路贷' THEN '套路贷'
            WHEN notice_content REGEXP '骗保|骗赔|假保单' THEN '骗保' 
            WHEN notice_content REGEXP '(非法集资)|(集资.*?诈骗)|(非法.*?吸收.*?存款)' THEN '非法集资/集资诈骗' 
            WHEN notice_content REGEXP '强制购物|强制消费' THEN '强制消费'
            WHEN notice_content REGEXP '传销' THEN '传销'
            WHEN notice_content REGEXP '无证行医|非法行医|非法医疗|非医师行医|擅自从事医疗活动' THEN '非法行医'
            WHEN notice_content REGEXP '假药|过期疫苗|(非法.*?药物.*?生产)' THEN '制售假药'
            WHEN notice_content REGEXP '(互联网|网络|平台|直播|视频|网站).*?(色情|情色|涉黄)' THEN '互联网不良信息'
            WHEN notice_content REGEXP '((电信|网络|电话).*?诈骗)|钓鱼网站|伪基站|黑广播' THEN '电信网络诈骗'
            WHEN notice_content REGEXP '(违法.*?排污)|环境污染|环境污染' THEN '严重污染环境'
            WHEN notice_content REGEXP
             '不合理低价游|黑导游|非法一日游|((旅游|导游|旅行社|景区|游客).*?(违法|欺客|宰客|强制消费|误导消费者|天价))' THEN '旅游乱象'
            WHEN notice_content REGEXP '(生产|安全|责任).*?事故' THEN '生产安全责任事故'
            WHEN notice_content REGEXP '((不合格|伪劣|三无|假冒).*?产品)|(产品.*?不合格)' THEN '不合格产品'
            WHEN notice_content REGEXP '(论文|学术|考试|学位|学历|招生|科研|研究|文凭).*?(造假|抄袭|作弊|不端|作假)' 
                THEN '学术/科研/论文造假'
            WHEN notice_content REGEXP '((侵犯|侵权).*?(知识产权|商标|专利|版权|著作权))|盗版|剽窃|盗用|侵权' THEN '侵犯知识产权'
            WHEN notice_content REGEXP '(家政|保姆|看护|月嫂|护工).*?(虚假|无证|违反道德|信息不明|盗窃|殴打|虐待|杀|谋害|纵火|毒死|
                摧残|折磨|侮辱|咒骂|讽刺|护理不当|投诉|惩罚|处罚|恶意|不正当|哄抬|误导消费)' THEN '家政乱象'
            WHEN notice_content REGEXP '失信被执行人' THEN '法院判决不执行'
            WHEN notice_content REGEXP '合同.*?欺诈' THEN '合同欺诈'
            WHEN notice_content REGEXP '逃税|骗税|偷税|漏税|假发票|(发票.*？造假)|(虚开.*?发票)|(伪造.*?发票)|
                (编造.*?发票)|(造假.*?发票)|税务违法' THEN '偷税漏税/虚开发票'
            WHEN notice_content REGEXP '(虚假.*?宣传)|(虚假.*?广告)|(虚假.*?营销)|(夸大.*?宣传)|(失实.*?宣传)' THEN '虚假广告/虚假营销'
            WHEN notice_content REGEXP '((伪造|虚假|假造|造假|虚构|捏造|不实|失实|作假).*?(材料|证明|证件|印章|公文|票据))
                |((材料|证明|证件|印章|公文|票据).*?(伪造|虚假|假造|造假|虚构|捏造|不实|失实|作假))' THEN '伪造材料证明'
            WHEN notice_content REGEXP '(拖欠.*?(工资|薪资))|欠薪' THEN '拖欠工资'
            WHEN notice_content REGEXP '(社会组织.*?失信)|(非法.*?社会组织)|(非法.*?组织)|(山寨.*?组织)|(山寨.*?社会组织)'
               THEN '非法社会组织'
            WHEN notice_content REGEXP '长租公寓.*?(爆雷|欺诈|欺骗|隐形收费|租金贷|租房贷|失信|违建|违章建筑|污染)' THEN '长租公寓'
            WHEN notice_content REGEXP '(培训机构|教育机构).*?(停业|((培训|课程费|学费).*?违约)|倒闭|爆雷|跑路)|培训贷'
               THEN '教育机构失信'
            WHEN notice_content REGEXP '直播.*?(带货|销售).*?(造假|虚假宣传|夸大|刷单|欺诈|误导|诱导|违规|欺骗|诈骗|三无产品|质量不合格)'
                THEN '直播带货失信'
            WHEN notice_content REGEXP '(虚增.*?(利润|资产|成本))|(财务.*?(造假|虚报))' THEN '财务造假' else 'null' end keyword
        FROM  dw.legal_adjudicative_documents 
        WHERE  dt = {} and source_key='defendant' and bbd_qyxx_id != 'null' and (sentence_date between date('2019-01-01') and date('2020-12-31'))
        group by year,month,bbd_qyxx_id,keyword,case_results
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.legal_adjudicative_documents"))).createOrReplaceTempView("xuqiu2_3_temp")

    xuqiu2_3 = spark.sql('''
        with cr as
        (select 
            bbd_qyxx_id,
            company_province,
            company_industry,
            company_companytype 
        from  dw.qyxx_basic 
        where dt={})
        
        SELECT
            keyword,
            case_results,
            month,
            year,
            company_province,
            company_industry,
            count(xuqiu2_3_temp.bbd_qyxx_id) qy_count,
            sum(rc_count_temp) rc_count
        from xuqiu2_3_temp join cr
        on xuqiu2_3_temp.bbd_qyxx_id = cr.bbd_qyxx_id
        where (keyword != "") and (company_companytype not in ('9100','9200','9300','9310','9320')) 
        group by year,month,company_province,company_industry,keyword,case_results
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.qyxx_basic")))

    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu2_3_data")
    xuqiu2_3.repartition(100).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')


def main(entry: Entry):
    """
       程序主入口,配置初始化和业务逻辑入口 newest_partition_with_data
       """
    entry.logger.info(entry.cfg_mgr.get("spark-submit-opt", "queue"))
    entry.logger.info("start")

    # entry.logger.info(f"开始执行 xuqiu2_1数据")
    # xuqiu2_1(entry)
    entry.logger.info(f"开始执行 xuqiu2_2数据")
    xuqiu2_2(entry)
    # entry.logger.info(f"开始执行 xuqiu2_3数据")
    # xuqiu2_3(entry)
    # entry.logger.info("finish")

def post_check(entry: Entry):
    return True


