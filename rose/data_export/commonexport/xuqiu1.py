# encoding:utf-8
__author__ = 'houguanyu'

from whetstone.core.entry import Entry
from ..proj_common.hive_util import HiveUtil
from datetime import date
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

'''
项目：临时导数据，数字资本对传统资本的挤出效应
数据源：dw数据库表 

'''


def pre_check(entry: Entry):
    return True


def xuqiu1(entry: Entry):
    spark = entry.spark
    xuqiu1 = spark.sql(
        '''
        with cr1 as
        (select 
            esdate,
            cancel_date,
            bbd_qyxx_id,
            operate_scope,
            company_name,
            company_industry,
            company_type,
            enterprise_status,
            regcap_amount,
            company_companytype,
            company_county,
            company_province,
        case when operate_scope REGEXP 'BI|大数据|流式处理|机器学习|数据采集|数据存储|数据挖掘|数据仓库|数据分析|数据集成|数据共
        享|数据挖掘|数据抓取|数据服务|数据可视化|自然语言处理|数据清洗|数据整理|数据脱敏|数据加密|数据托管|数据挖掘|数据交易|数据可
        视化|数据库|数据流'
        THEN '大数据产业'
        when operate_scope REGEXP '云计算|云服务|云平台|云存储|并行计算|分布式计算|分布式存储' THEN '云计算产业'
        when operate_scope REGEXP '区块链|分布式存储|比特币|分布式记账|分布式账本|以太坊|虚拟货币|数字货币|加密货币|联盟链|
        去中心化|供应链金融|智能合约|能源互联网'
        THEN '区块链产业'
        when operate_scope REGEXP '信息安全|网络安全|防火墙|互联网安全|通讯安全|数据安全|边界安全|身份鉴别|访问控制'
        THEN '信息安全产业'
        when operate_scope REGEXP '人工智能|无人驾驶|智能家居|图像识别|人脸识别|商业智能|AI|机器学习|类脑|深度学习|神经网络|
        遗传算法|语义识别|机器翻译|语音识别|自然语言处理|区块链|比特币|智能汽车|无人驾驶|无人车|图像处理|智能制造'
        THEN '人工智能'
        when operate_scope REGEXP '集成电路|5G|汽车电子|超高清视频|智能终端|物联网|EDA|电子设计自动化|关键IP|芯片|单片机|CMOS
        |FPGA|MEMS|功率器件|硅光集成|模拟电路|嵌入式系统'
        THEN '集成电路设计产业'
        when operate_scope REGEXP '工业软件|新型工业APP|工业互联网|云原生|工业系统低代码开发|超密集异构|5G+MEC|智联技术|工业互联
        网端边融合|实时数据仓库构建技术|工业以太网|工业系统智能应用'
        THEN '工业互联网产业'
        when operate_scope REGEXP '互联网金融数据处理|金融大数据|新兴金融业态|金融产品研发|金融科技'
        THEN '金融科技产业'
        when operate_scope REGEXP '智慧城市|智能城市|数字城市|智能电网|智慧安防|智慧交通|智慧医疗|智慧医院|互联网医疗|智慧教育|
        智慧政务|智慧企业|智慧物流'
        THEN '智慧城市产业'
        when operate_scope REGEXP '电子商务|跨境电商|网络零售|B2B|B2C|C2C|O2O|社交电商|电商平台|网络营销|电子货币交换|
        电子交易市场|网上支付|网络平台服务|网络交易服务|网络贸易服务|网络交易保障服务|现代物流'
        THEN '电子商务产业'
        when operate_scope REGEXP '数字图书馆|数字博物馆|数字文化馆|数字美术馆|文体设施智慧服务平台|数字出版|虚拟博物馆|
        数字阅读|在线音频|数字融媒体|数字创意|网络文学|VR|数字音乐|电竞|数字动漫'
        THEN '数字创意产业'
        when operate_scope REGEXP '远程办公|线上办公|数字化治理|共享经济|在线展览|生鲜电商零售|无接触配送|在线教育|移动出行|
        在线医疗|在线研发设计|虚拟产业园|虚拟产业集群|网络直播'
        THEN '其他新兴业态' else "" end keyword
        from dw.qyxx_basic
        where dt = {}
        and esdate >= '2016-01-01'
        ),
        cr2 as(
        select bbd_qyxx_id,company_scale
        from dw.qyxx_enterprise_scale
        where dt = {}
        )
    
        select esdate,cancel_date,cr1.bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,
        enterprise_status,regcap_amount,company_companytype,company_county,company_province,
        company_scale,keyword
        from cr1 INNER JOIN cr2
        on cr1.bbd_qyxx_id = cr2.bbd_qyxx_id
        where keyword != ""
        '''.format(HiveUtil.newest_partition_with_data(spark, "dw.qyxx_basic"),
                   HiveUtil.newest_partition_with_data(spark, "dw.qyxx_enterprise_scale")))
    OUTPUT_HDFS_PATH = entry.cfg_mgr.hdfs.get_result_path("hdfs-biz", "xuqiu1_data")
    xuqiu1.repartition(80).write.csv(path=OUTPUT_HDFS_PATH, header=True, mode='overwrite')


def main(entry: Entry):
    """
       程序主入口,配置初始化和业务逻辑入口 newest_partition_with_data
       """
    entry.logger.info(entry.cfg_mgr.get("spark-submit-opt", "queue"))
    entry.logger.info("start")

    entry.logger.info(f"开始执行 xuqiu1数据")
    xuqiu1(entry)

def post_check(entry: Entry):
    return True


