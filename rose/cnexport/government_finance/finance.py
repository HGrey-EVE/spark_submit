#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description: 禅道 - http://10.28.200.161/zentao/index.php?m=story&f=view&storyID=5270
:Author: weifuwan@bbdservice.com
:Date: 2021-04-28 16:08
"""
from pyspark.sql.functions import udf
from pyspark.sql import functions as fun, DataFrame
from pyspark.sql import Row, functions as F

from cnexport.proj_common.hive_util import HiveUtil
from pyspark.sql.types import StringType
from whetstone.core.entry import Entry

def pre_check(entry: Entry):
    return True

"""
    create table dw_finance.shuzijingji20210429_v6
    (esdate string,  cancel_date string, bbd_qyxx_id string,operate_scope string,   company_name string,company_industry string, company_type string, enterprise_status  string, regcap_amount string,company_companytype string, company_county string,company_province  string,company_scale string,industry string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE;
    
    create table dw_finance.shuzijingji20210429_v7
    (esdate string,  cancel_date string, bbd_qyxx_id string,operate_scope string,   company_name string,company_industry string, company_type string, enterprise_status  string, regcap_amount string,company_companytype string, company_county string,company_province  string,company_scale string,industry string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\001'
    stored as orc;
    load data inpath '/user/cqxdcyfzyjy/tmp/wfw/finance_final_v3/*' into table dw_finance.shuzijingji20210429_v7;
    
    insert into table dw_finance.shuzijingji20210429_v7
    select * from dw_finance.shuzijingji20210429_v6
    
    -- 爆破标签
    create table dw_finance.shuzijingji20210429_v2 as 
    select
     esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province,
        industry_explode as industry
    from (
    select
        esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province,
        industry_explode
    from dw_finance.shuzijingji20210429
    lateral view explode(split(industry,',')) tmp as industry_explode
    ) a
    
    
    create table dw_finance.shuzijingji20210429_v1 as 
    select * from dw_finance.shuzijingji20210429 where esdate != 'esdate'
    
"""
def main(entry: Entry):
    dt = HiveUtil.newest_partition(entry.spark, 'dw.qyxx_basic')
    _sql = f"""
        select esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province, 
            case when operate_scope rlike 'BI|大数据|流式处理|机器学习|数据采集|数据存储|数据挖掘|数据仓库|数据分析|数据集成|数据共享|数据挖掘|数据抓取|数据服务|数据可视化|自然语言处理|数据清洗|数据整理|数据脱敏|数据加密|数据托管|数据挖掘|数据交易|数据可视化|数据库|数据流' then '大数据产业'
                 when operate_scope rlike '云计算|云服务|云平台|云存储|并行计算|分布式计算|分布式存储' then '云计算产业'
                 when operate_scope rlike '区块链|分布式存储|比特币|分布式记账|分布式账本|以太坊|虚拟货币|数字货币|加密货币|联盟链|去中心化|供应链金融|智能合约|能源互联网' then '区块链产业'  
                 when operate_scope rlike '信息安全|网络安全|防火墙|互联网安全|通讯安全|数据安全|边界安全|身份鉴别|访问控制' then '信息安全产业'  
                 when operate_scope rlike '人工智能|无人驾驶|智能家居|图像识别|人脸识别|商业智能|AI|机器学习|类脑|深度学习|神经网络|遗传算法|语义识别|机器翻译|语音识别|自然语言处理|区块链|比特币|智能汽车|无人驾驶|无人车|图像处理|智能制造' then '人工智能'  
                 when operate_scope rlike '集成电路|5G|汽车电子|超高清视频|智能终端|物联网|EDA|电子设计自动化|关键IP|芯片|单片机|CMOS|FPGA|MEMS|功率器件|硅光集成|模拟电路|嵌入式系统' then '集成电路设计产业'  
                 when operate_scope rlike '工业软件|新型工业APP|工业互联网|云原生|工业系统低代码开发|超密集异构|5G+MEC|智联技术|工业互联网端边融合|实时数据仓库构建技术|工业以太网|工业系统智能应用' then '工业互联网产业'  
                 when operate_scope rlike '互联网金融数据处理|金融大数据|新兴金融业态|金融产品研发|金融科技' then '金融科技产业'  
                 when operate_scope rlike '智慧城市|智能城市|数字城市|智能电网|智慧安防|智慧交通|智慧医疗|智慧医院|互联网医疗|智慧教育|智慧政务|智慧企业|智慧物流' then '智慧城市产业'  
                 when operate_scope rlike '电子商务|跨境电商|网络零售|B2B|B2C|C2C|O2O|社交电商|电商平台|网络营销|电子货币交换|电子交易市场|网上支付|网络平台服务|网络交易服务|网络贸易服务|网络交易保障服务|现代物流' then '电子商务产业'  
                 when operate_scope rlike '数字图书馆|数字博物馆|数字文化馆|数字美术馆|文体设施智慧服务平台|数字出版|虚拟博物馆|数字阅读|在线音频|数字融媒体|数字创意|网络文学|VR|数字音乐|电竞|数字动漫' then '数字创意产业'        
                 when operate_scope rlike '远程办公|线上办公|数字化治理|共享经济|在线展览|生鲜电商零售|无接触配送|在线教育|移动出行|在线医疗|在线研发设计|虚拟产业园|虚拟产业集群|网络直播' then '其他新兴业态'        
            end industry from (select * from dw.qyxx_basic 
                  where operate_scope rlike 'BI|大数据|流式处理|机器学习|数据采集|数据存储|数据挖掘|数据仓库|数据分析|数据集成|数据共享|数据挖掘|数据抓取|数据服务|数据可视化|自然语言处理|数据清洗|数据整理|数据脱敏
                    |数据加密|数据托管|数据挖掘|数据交易|数据可视化|数据库|数据流|云计算|云服务|云平台|云存储|并行计算|分布式计算|分布式存储|区块链|分布式存储|比特币|分布式记账|分布式账本
                    |以太坊|虚拟货币|数字货币|加密货币|联盟链|去中心化|供应链金融|智能合约|能源互联网
                    |信息安全|网络安全|防火墙|互联网安全|通讯安全|数据安全|边界安全|身份鉴别|访问控制
                    |人工智能|无人驾驶|智能家居|图像识别|人脸识别|商业智能|AI|机器学习|类脑|深度学习|神经网络|遗传算法|语义识别|机器翻译|语音识别|自然语言处理|区块链|比特币|智能汽车|无人驾驶|无人车|图像处理|智能制造
                    |集成电路|5G|汽车电子|超高清视频|智能终端|物联网|EDA|电子设计自动化|关键IP|芯片|单片机|CMOS|FPGA|MEMS|功率器件|硅光集成|模拟电路|嵌入式系统
                    |工业软件|新型工业APP|工业互联网|云原生|工业系统低代码开发|超密集异构|5G+MEC|智联技术|工业互联网端边融合|实时数据仓库构建技术|工业以太网|工业系统智能应用
                    |互联网金融数据处理|金融大数据|新兴金融业态|金融产品研发|金融科技
                    |智慧城市|智能城市|数字城市|智能电网|智慧安防|智慧交通|智慧医疗|智慧医院|互联网医疗|智慧教育|智慧政务|智慧企业|智慧物流
                    |电子商务|跨境电商|网络零售|B2B|B2C|C2C|O2O|社交电商|电商平台|网络营销|电子货币交换|电子交易市场|网上支付|网络平台服务|网络交易服务|网络贸易服务|网络交易保障服务|现代物流
                    |数字图书馆|数字博物馆|数字文化馆|数字美术馆|文体设施智慧服务平台|数字出版|虚拟博物馆|数字阅读|在线音频|数字融媒体|数字创意|网络文学|VR|数字音乐|电竞|数字动漫
                    |远程办公|线上办公|数字化治理|共享经济|在线展览|生鲜电商零售|无接触配送|在线教育|移动出行|在线医疗|在线研发设计|虚拟产业园|虚拟产业集群|网络直播'
                     and dt='20210426')
    """

    __sql = f"""
            select
                esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province, 
                concat_ws(",",operate_scope_1,operate_scope_2,operate_scope_3,operate_scope_4,operate_scope_5,operate_scope_6,operate_scope_7,operate_scope_8,operate_scope_9,operate_scope_10,operate_scope_11,operate_scope_12) industry
            from (
                select esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province, 
                    case when operate_scope rlike 'BI|大数据|流式处理|机器学习|数据采集|数据存储|数据挖掘|数据仓库|数据分析|数据集成|数据共享|数据挖掘|数据抓取|数据服务|数据可视化|自然语言处理|数据清洗|数据整理|数据脱敏|数据加密|数据托管|数据挖掘|数据交易|数据可视化|数据库|数据流' then '大数据产业'
                    else '' end operate_scope_1,
                    case when operate_scope rlike '云计算|云服务|云平台|云存储|并行计算|分布式计算|分布式存储' then '云计算产业'
                    else '' end operate_scope_2,
                    case when operate_scope rlike '区块链|分布式存储|比特币|分布式记账|分布式账本|以太坊|虚拟货币|数字货币|加密货币|联盟链|去中心化|供应链金融|智能合约|能源互联网' then '区块链产业'  
                    else '' end operate_scope_3,
                    case when operate_scope rlike '信息安全|网络安全|防火墙|互联网安全|通讯安全|数据安全|边界安全|身份鉴别|访问控制' then '信息安全产业'  
                    else '' end operate_scope_4,
                    case when operate_scope rlike '人工智能|无人驾驶|智能家居|图像识别|人脸识别|商业智能|AI|机器学习|类脑|深度学习|神经网络|遗传算法|语义识别|机器翻译|语音识别|自然语言处理|区块链|比特币|智能汽车|无人驾驶|无人车|图像处理|智能制造' then '人工智能'  
                    else '' end operate_scope_5,
                    case when operate_scope rlike '集成电路|5G|汽车电子|超高清视频|智能终端|物联网|EDA|电子设计自动化|关键IP|芯片|单片机|CMOS|FPGA|MEMS|功率器件|硅光集成|模拟电路|嵌入式系统' then '集成电路设计产业'  
                    else '' end operate_scope_6,
                    case when operate_scope rlike '工业软件|新型工业APP|工业互联网|云原生|工业系统低代码开发|超密集异构|5G+MEC|智联技术|工业互联网端边融合|实时数据仓库构建技术|工业以太网|工业系统智能应用' then '工业互联网产业'  
                    else '' end operate_scope_7,
                    case when operate_scope rlike '互联网金融数据处理|金融大数据|新兴金融业态|金融产品研发|金融科技' then '金融科技产业'  
                    else '' end operate_scope_8,
                    case when operate_scope rlike '智慧城市|智能城市|数字城市|智能电网|智慧安防|智慧交通|智慧医疗|智慧医院|互联网医疗|智慧教育|智慧政务|智慧企业|智慧物流' then '智慧城市产业'  
                    else '' end operate_scope_9,
                    case when operate_scope rlike '电子商务|跨境电商|网络零售|B2B|B2C|C2C|O2O|社交电商|电商平台|网络营销|电子货币交换|电子交易市场|网上支付|网络平台服务|网络交易服务|网络贸易服务|网络交易保障服务|现代物流' then '电子商务产业'  
                    else '' end operate_scope_10,
                    case when operate_scope rlike '数字图书馆|数字博物馆|数字文化馆|数字美术馆|文体设施智慧服务平台|数字出版|虚拟博物馆|数字阅读|在线音频|数字融媒体|数字创意|网络文学|VR|数字音乐|电竞|数字动漫' then '数字创意产业'        
                    else '' end operate_scope_11,
                    case when operate_scope rlike '远程办公|线上办公|数字化治理|共享经济|在线展览|生鲜电商零售|无接触配送|在线教育|移动出行|在线医疗|在线研发设计|虚拟产业园|虚拟产业集群|网络直播' then '其他新兴业态'        
                    else '' end operate_scope_12 from (select * from dw.qyxx_basic 
                          where operate_scope rlike 'BI|大数据|流式处理|机器学习|数据采集|数据存储|数据挖掘|数据仓库|数据分析|数据集成|数据共享|数据挖掘|数据抓取|数据服务|数据可视化|自然语言处理|数据清洗|数据整理|数据脱敏
                            |数据加密|数据托管|数据挖掘|数据交易|数据可视化|数据库|数据流|云计算|云服务|云平台|云存储|并行计算|分布式计算|分布式存储|区块链|分布式存储|比特币|分布式记账|分布式账本
                            |以太坊|虚拟货币|数字货币|加密货币|联盟链|去中心化|供应链金融|智能合约|能源互联网
                            |信息安全|网络安全|防火墙|互联网安全|通讯安全|数据安全|边界安全|身份鉴别|访问控制
                            |人工智能|无人驾驶|智能家居|图像识别|人脸识别|商业智能|AI|机器学习|类脑|深度学习|神经网络|遗传算法|语义识别|机器翻译|语音识别|自然语言处理|区块链|比特币|智能汽车|无人驾驶|无人车|图像处理|智能制造
                            |集成电路|5G|汽车电子|超高清视频|智能终端|物联网|EDA|电子设计自动化|关键IP|芯片|单片机|CMOS|FPGA|MEMS|功率器件|硅光集成|模拟电路|嵌入式系统
                            |工业软件|新型工业APP|工业互联网|云原生|工业系统低代码开发|超密集异构|5G+MEC|智联技术|工业互联网端边融合|实时数据仓库构建技术|工业以太网|工业系统智能应用
                            |互联网金融数据处理|金融大数据|新兴金融业态|金融产品研发|金融科技
                            |智慧城市|智能城市|数字城市|智能电网|智慧安防|智慧交通|智慧医疗|智慧医院|互联网医疗|智慧教育|智慧政务|智慧企业|智慧物流
                            |电子商务|跨境电商|网络零售|B2B|B2C|C2C|O2O|社交电商|电商平台|网络营销|电子货币交换|电子交易市场|网上支付|网络平台服务|网络交易服务|网络贸易服务|网络交易保障服务|现代物流
                            |数字图书馆|数字博物馆|数字文化馆|数字美术馆|文体设施智慧服务平台|数字出版|虚拟博物馆|数字阅读|在线音频|数字融媒体|数字创意|网络文学|VR|数字音乐|电竞|数字动漫
                            |远程办公|线上办公|数字化治理|共享经济|在线展览|生鲜电商零售|无接触配送|在线教育|移动出行|在线医疗|在线研发设计|虚拟产业园|虚拟产业集群|网络直播'
                             and dt='20210426')
            ) a
            
        """
    # entry.spark.sql(__sql).write.csv("tmp/wfw/finance", header=True, sep='\t', mode='overwrite')
    # entry.spark.read.csv("tmp/wfw/finance", header=True, sep='\t').createOrReplaceTempView("tmp")
    # entry.spark.sql("""
    #     select b.*, a.company_scale
    #     from (
    #         select
    #             bbd_qyxx_id,
    #             company_scale
    #         from dw.qyxx_enterprise_scale
    #         where dt = '20210401'
    #     ) a
    #     inner join tmp b
    #     on a.bbd_qyxx_id = b.bbd_qyxx_id
    # """).write.csv("tmp/wfw/finance_final",header=True, sep='\t', mode='overwrite')
    #
    # def map1(industry):
    #     l = list()
    #     arr = industry.split(",")
    #     for industry in arr:
    #         if industry != '':
    #             l.append(industry)
    #     return ','.join(l)
    # udf = fun.udf(map1, StringType())
    # entry.spark.read.csv("tmp/wfw/finance_final", sep='\t',header=True)\
    #     .withColumn("industry", udf("industry"))\
    #     .write\
    #     .csv("tmp/wfw/finance_final_v2", sep='\t',header=True, mode='overwrite')
    #
    # entry.spark.read.csv("tmp/wfw/finance_final_v2", sep='\t', header=True).createOrReplaceTempView("tmp2")
    # entry.spark.sql("""
    # select
    #  esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province,company_scale,
    #     industry_explode as industry
    # from (
    # select
    #     esdate,cancel_date,bbd_qyxx_id,operate_scope,company_name,company_industry,company_type,enterprise_status,regcap_amount,company_companytype,company_county,company_province,company_scale,
    #     industry_explode
    # from tmp2
    # lateral view explode(split(industry,',')) tmp as industry_explode
    # ) a
    # """).write\
    #     .csv("tmp/wfw/finance_final_v3", sep='\001', mode='overwrite', header=True) ## bug 最好用\001, 不然数据对不上

    # _sql_6 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,a.company_scale,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end as zhuceziben,a.industry,count(distinct a.bbd_qyxx_id) as company_num,
    # count (DISTINCT c.bbd_xgxx_id) as zhaopin_fabushuliang,SUM(c.bbd_recruit_num) as zhaopin_renshu,AVG(c.bbd_salary) as pingjun_xinzi,SUM(c.delivery_number) as toudi_shuliang
    # from (select * from dw_finance.shuzijingji20210429_v8)a
    # Join (select bbd_qyxx_id,regcap_amount from dw.qyxx_basic where dt='20210426')b
    # on a.bbd_qyxx_id=b.bbd_qyxx_id
    # Join (select bbd_qyxx_id,bbd_xgxx_id,pubdate,bbd_source,bbd_salary,cast(bbd_recruit_num as double) as bbd_recruit_num,cast(regexp_extract(delivery_number,'([0-9\.]*)') as decimal(30,0)) as delivery_number
    # from dw.manage_recruit where dt='20210427'and delivery_number != '')c
    # on a.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate),a.company_scale,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end,a.industry
    # order by year(c.pubdate),month(c.pubdate),a.company_scale,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end,a.industry
    # """
    #
    # entry.spark.sql(_sql_6).write.csv("tmp/wfw/finance/req_6", sep='`', header=True, mode='overwrite')

    # _sql_6_1 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end as zhuceziben,count(distinct a.bbd_qyxx_id) as company_num,
    # count (DISTINCT c.bbd_xgxx_id) as zhaopin_fabushuliang,SUM(c.bbd_recruit_num) as zhaopin_renshu,AVG(c.bbd_salary) as pingjun_xinzi,SUM(c.delivery_number) as toudi_shuliang
    # from (select * from dw.qyxx_enterprise_scale where dt ='20210401')a
    # Join (select * from dw.qyxx_basic b where dt='20210426' and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = b.bbd_qyxx_id))b
    # on a.bbd_qyxx_id=b.bbd_qyxx_id
    # Join (select bbd_qyxx_id,bbd_xgxx_id,pubdate,bbd_source,bbd_salary,cast(bbd_recruit_num as double) as bbd_recruit_num,cast(regexp_extract(delivery_number,'([0-9\.]*)') as decimal(30,0)) as delivery_number
    # from dw.manage_recruit where dt='20210427'and delivery_number != '')c
    # on a.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # order by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # """
    #
    # entry.spark.sql(_sql_6_1).write.csv("tmp/wfw/finance/req_6_1", sep='`', header=True, mode='overwrite')
    #
    # _sql_6_2 = """
    #     select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    #     when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    #     when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    #     when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    #     when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    #     when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    #     when b.regcap_amount>=100000001 then '>100000001' else '其他' end as zhuceziben,count(distinct a.bbd_qyxx_id) as company_num,
    #     count (DISTINCT c.bbd_xgxx_id) as zhaopin_fabushuliang,SUM(c.bbd_recruit_num) as zhaopin_renshu,AVG(c.bbd_salary) as pingjun_xinzi,SUM(c.delivery_number) as toudi_shuliang
    #     from (select * from dw.qyxx_enterprise_scale where dt ='20210401')a
    #     Join (select * from dw.qyxx_basic where dt='20210426')b
    #     on a.bbd_qyxx_id=b.bbd_qyxx_id
    #     Join (select bbd_qyxx_id,bbd_xgxx_id,pubdate,bbd_source,bbd_salary,cast(bbd_recruit_num as double) as bbd_recruit_num,cast(regexp_extract(delivery_number,'([0-9\.]*)') as decimal(30,0)) as delivery_number
    #     from dw.manage_recruit where dt='20210427'and delivery_number != '')c
    #     on a.bbd_qyxx_id=c.bbd_qyxx_id
    #     group by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    #     when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    #     when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    #     when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    #     when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    #     when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    #     when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    #     order by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    #     when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    #     when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    #     when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    #     when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    #     when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    #     when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # """
    # entry.spark.sql(_sql_6_2).write.csv("tmp/wfw/finance/req_6_2", sep='`', header=True, mode='overwrite')
    #
    # _sql_7 = """
    #     select year(esdate) as esyear, month(esdate) as esmonth,count(distinct bbd_qyxx_id),sum(regcap_amount) as zhucezibenzonge
    # from dw.qyxx_basic c where dt='20210426' and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = c.bbd_qyxx_id)
    # group by year(esdate),month(esdate)
    # order by year(esdate),month(esdate)
    # """
    # entry.spark.sql(_sql_7).write.csv("tmp/wfw/finance/req_7", sep='`', header=True, mode='overwrite')
    #
    #
    # _sql_2 = """
    #     select year(a.cancel_date) as cancelyear, month(a.cancel_date) as cancelmonth,b.company_scale,count(distinct a.bbd_qyxx_id) as company_num
    #     from
    #     (select * from dw.qyxx_basic b where dt = '20210426' and cancel_date between date('2016-01-01') and date('2021-04-29') and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = b.bbd_qyxx_id)) a
    #     join (select * from dw.qyxx_enterprise_scale where dt = '20210401') b
    #     on a.bbd_qyxx_id = b.bbd_qyxx_id
    #     group by year(a.cancel_date),month(a.cancel_date),b.company_scale
    #     order by year(a.cancel_date),month(a.cancel_date)
    # """
    #
    # entry.spark.sql(_sql_2).write.csv("tmp/wfw/finance/req_2", sep='`', header=True, mode='overwrite')

    # entry.spark.read.csv("tmp/wfw/finance/req_2", sep='\t', header=True).repartition(1).write.csv("tmp/wfw/finance/req_2_back", sep='`', header=True, mode='overwrite')
    # entry.spark.read.csv("tmp/wfw/finance/req_6", sep='\t', header=True).repartition(1).write.csv("tmp/wfw/finance/req_6_back", sep='`', header=True, mode='overwrite')
    # entry.spark.read.csv("tmp/wfw/finance/req_6_1", sep='\t', header=True).repartition(1).write.csv("tmp/wfw/finance/req_6_1_back", sep='`', header=True, mode='overwrite')
    # entry.spark.read.csv("tmp/wfw/finance/req_6_2", sep='\t', header=True).repartition(1).write.csv("tmp/wfw/finance/req_6_2_back", sep='`', header=True, mode='overwrite')
    # entry.spark.read.csv("tmp/wfw/finance/req_7", sep='\t', header=True).repartition(1).write.csv("tmp/wfw/finance/req_7_back", sep='`', header=True, mode='overwrite')

    # _sql_20210430_6 = """
    #
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,a.company_scale,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end as zhuceziben,a.industry,count(distinct a.bbd_qyxx_id) as company_num,
    # count (DISTINCT c.bbd_xgxx_id) as zhaopin_fabushuliang,SUM(c.bbd_recruit_num) as zhaopin_renshu,AVG(c.bbd_salary) as pingjun_xinzi
    # from (select * from dw_finance.shuzijingji20210429_v8)a
    # Join (select bbd_qyxx_id,regcap_amount from dw.qyxx_basic where dt='20210426')b
    # on a.bbd_qyxx_id=b.bbd_qyxx_id
    # Join (select * from dw.manage_recruit where dt='20210427')c
    # on a.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate),a.company_scale,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end,a.industry
    # order by year(c.pubdate),month(c.pubdate),a.company_scale,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end,a.industry
    # """
    # entry.spark.sql(_sql_20210430_6).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_6", sep='`', header=True, mode='overwrite')
    #
    # _sql_20210430_6_1 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end as zhuceziben,count(distinct a.bbd_qyxx_id) as company_num,
    # count (DISTINCT c.bbd_xgxx_id) as zhaopin_fabushuliang,SUM(c.bbd_recruit_num) as zhaopin_renshu,AVG(c.bbd_salary) as pingjun_xinzi
    # from (select * from dw.qyxx_enterprise_scale where dt ='20210401')a
    # Join (select * from dw.qyxx_basic b where dt='20210426' and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = b.bbd_qyxx_id))b
    # on a.bbd_qyxx_id=b.bbd_qyxx_id
    # Join (select * from dw.manage_recruit where dt='20210427')c
    # on a.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # order by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # """
    # entry.spark.sql(_sql_20210430_6_1).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_6_1", sep='`', header=True,
    #                                            mode='overwrite')
    #
    # _sql_20210430_6_2 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end as zhuceziben,count(distinct a.bbd_qyxx_id) as company_num,
    # count (DISTINCT c.bbd_xgxx_id) as zhaopin_fabushuliang,SUM(c.bbd_recruit_num) as zhaopin_renshu,AVG(c.bbd_salary) as pingjun_xinzi
    # from (select * from dw.qyxx_enterprise_scale where dt ='20210401')a
    # Join (select * from dw.qyxx_basic where dt='20210426')b
    # on a.bbd_qyxx_id=b.bbd_qyxx_id
    # Join (select * from dw.manage_recruit where dt='20210427')c
    # on a.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # order by year(c.pubdate),month(c.pubdate),a.company_scale,b.company_industry,c.bbd_source,case when b.regcap_amount<100001 then '<100001'
    # when b.regcap_amount>=100001 and b.regcap_amount<500001 then '100001-500001'
    # when b.regcap_amount>=500001 and b.regcap_amount<1000001 then '500001-1000001'
    # when b.regcap_amount>=1000001 and b.regcap_amount<5000001 then '1000001-5000001'
    # when b.regcap_amount>=5000001 and b.regcap_amount<10000001 then '5000001-10000001'
    # when b.regcap_amount>=10000001 and b.regcap_amount<100000001 then '10000001-100000001'
    # when b.regcap_amount>=100000001 then '>100000001' else '其他' end
    # """
    # entry.spark.sql(_sql_20210430_6_2).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_6_2", sep='`', header=True,
    #                                            mode='overwrite')
    #
    # _sql_20210430_8_1 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,AVG(c.bbd_salary) as pingjun_xinzi
    # from (select * from dw_finance.shuzijingji20210429_v8)a
    # Join (select * from dw.manage_recruit where dt='20210429')c
    # on a.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate)
    # order by year(c.pubdate),month(c.pubdate)
    # """
    # entry.spark.sql(_sql_20210430_8_1).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_8_1", sep='`',header=True,mode='overwrite')
    #
    # _sql_20210430_8_2 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,AVG(c.bbd_salary) as pingjun_xinzi
    # from (select * from dw.qyxx_basic b where dt='20210426' and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = b.bbd_qyxx_id))b
    # Join (select * from dw.manage_recruit where dt='20210427')c
    # on b.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate)
    # order by year(c.pubdate),month(c.pubdate)
    # """
    # entry.spark.sql(_sql_20210430_8_2).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_8_2", sep='`',
    #                                                             header=True, mode='overwrite')
    #
    # _sql_20210430_8_3 = """
    # select year(c.pubdate) as zhaopin_pubyear,month(c.pubdate) as zhaopin_pubmonth,AVG(c.bbd_salary) as pingjun_xinzi
    # from (select * from dw.qyxx_basic where dt='20210426')b
    # Join (select * from dw.manage_recruit where dt='20210427')c
    # on b.bbd_qyxx_id=c.bbd_qyxx_id
    # group by year(c.pubdate),month(c.pubdate)
    # order by year(c.pubdate),month(c.pubdate)
    # """
    # entry.spark.sql(_sql_20210430_8_3).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_8_3", sep='`',
    #                                                             header=True, mode='overwrite')

    # _sql_20210430_01 = """
    # select year(a.esdate) as esyear, month(a.esdate) as esmonth,b.company_scale,count(distinct a.bbd_qyxx_id) as company_num
    # from (select * from dw.qyxx_basic b where dt='20210426' and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = b.bbd_qyxx_id)) a
    # join (select bbd_qyxx_id,company_scale from dw.qyxx_enterprise_scale where dt = '20210401') b
    # on a.bbd_qyxx_id = b.bbd_qyxx_id
    # group by year(a.esdate),month(a.esdate),b.company_scale
    # order by year(a.esdate),month(a.esdate)
    # """
    # entry.spark.sql(_sql_20210430_01).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_01", sep='`',
    #                                                             header=True, mode='overwrite')
    # _sql_20210430_02 = """
    # select year(a.esdate) as esyear, month(a.esdate) as esmonth,b.company_scale,count(distinct a.bbd_qyxx_id) as company_num
    # from
    # (select esdate, bbd_qyxx_id from dw.qyxx_basic where dt = '20210426')a
    # join (select bbd_qyxx_id,company_scale from dw.qyxx_enterprise_scale where dt = '20210401') b
    # on a.bbd_qyxx_id = b.bbd_qyxx_id
    # group by year(a.esdate),month(a.esdate),b.company_scale
    # order by year(a.esdate),month(a.esdate)
    # """
    # entry.spark.sql(_sql_20210430_02).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_02", sep='`',
    #                                                            header=True, mode='overwrite')
    #
    # _sql_20210430_03 = """
    # select substr(esdate,1,4) as esyear, substr(esdate,6,2) as esmonth,company_scale,count(distinct bbd_qyxx_id) as company_num
    # from dw_finance.shuzijingji20210429_v8
    # group by substr(esdate,1,4),substr(esdate,6,2),company_scale
    # order by substr(esdate,1,4),substr(esdate,6,2)
    # """
    #
    # entry.spark.sql(_sql_20210430_03).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_03", sep='`',
    #                                                            header=True, mode='overwrite')
    # _sql_20210430_04 = """
    # select year(a.cancel_date) as cancelyear, month(a.cancel_date) as cancelmonth,b.company_scale,count(distinct a.bbd_qyxx_id) as company_num
    # from
    # (select * from dw.qyxx_basic b where dt='20210426' and company_industry in ('A','B','C','D','E') and not exists (select 1 from (SELECT distinct bbd_qyxx_id from dw_finance.shuzijingji20210429_v8) a where a.bbd_qyxx_id = b.bbd_qyxx_id)) a
    # join (select * from dw.qyxx_enterprise_scale where dt = '20210401') b
    # on a.bbd_qyxx_id = b.bbd_qyxx_id
    # group by year(a.cancel_date),month(a.cancel_date),b.company_scale
    # order by year(a.cancel_date),month(a.cancel_date)
    # """
    # entry.spark.sql(_sql_20210430_04).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_04", sep='`',
    #                                                            header=True, mode='overwrite')
    # _sql_20210430_05 = """
    #     select year(a.cancel_date) as cancelyear, month(a.cancel_date) as cancelmonth,b.company_scale,count(distinct a.bbd_qyxx_id) as company_num
    #     from
    #     (select * from dw.qyxx_basic where dt = '20210426')a
    #     join (select * from dw.qyxx_enterprise_scale where dt = '20210401') b
    #     on a.bbd_qyxx_id = b.bbd_qyxx_id
    #     group by year(a.cancel_date),month(a.cancel_date),b.company_scale
    #     order by year(a.cancel_date),month(a.cancel_date)
    # """
    # entry.spark.sql(_sql_20210430_05).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_05", sep='`',
    #                                                            header=True, mode='overwrite')

    # _sql_20210430_06 = """
    #     select substr(cancel_date,1,4) as cancelyear, substr(cancel_date,6,2) as cancelmonth,company_scale,count(distinct bbd_qyxx_id) as company_num
    # from dw_finance.shuzijingji20210429_v8
    # group by substr(cancel_date,1,4),substr(cancel_date,6,2),company_scale
    # order by substr(cancel_date,1,4),substr(cancel_date,6,2)
    # """
    # entry.spark.sql(_sql_20210430_06).repartition(1).write.csv("tmp/wfw/finance_0430/_sql_20210430_06", sep='`',
    #                                                            header=True, mode='overwrite')

    # _sql_20210507_01 = """
    #     select * from dw.legal_adjudicative_documents where dt ='20210505' and update > date('2021-04-19') and case_results in ('原告胜诉','部分胜诉') and source_key = 'defendant' and bbd_qyxx_id in (select bbd_qyxx_id from dw.qyxx_basic where dt = '20210505' and company_name like '%养老%' or operate_scope like '%养老%')
    # """
    # step_1 = entry.spark.sql(_sql_20210507_01).repartition(1)
    # step_1.write.parquet("tmp/wfw/finannce_0507_01", mode="overwrite")
    # entry.spark.read.parquet("tmp/wfw/finannce_0507_01").write.csv("tmp/wfw/finance_0507/finannce_0507_01", sep='`',
    #
    #                                                            header=True, mode='overwrite')

    _sql_20210512 = """
    select * from dw.legal_adjudicative_documents where dt ='20210512'
    and update > date('2021-05-04')
    and case_results in ('原告胜诉','部分胜诉')
    and source_key = 'defendant' 
    and notice_content rlike '集资|吸收资金|吸收存款|吸收公众存款|诈骗|投资欺诈|退款难'
    and bbd_qyxx_id in (select bbd_qyxx_id from dw.qyxx_basic where dt = '20210512' and company_name like '%养老%' or operate_scope like '%养老%')
    """
    entry.spark.sql(_sql_20210512).repartition(1).write.csv("tmp/wfw/finance_0507/finannce_0512_01", sep='`',
                                                               header=True, mode='overwrite')
def post_check(entry: Entry):
    return True
