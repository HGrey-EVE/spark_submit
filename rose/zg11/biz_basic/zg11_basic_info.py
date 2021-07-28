#encoding:utf-8
__author__ = 'liting'

from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession, Row, DataFrame
from whetstone.core.entry import Entry
from ..proj_common.hive_util import HiveUtil
from datetime import  datetime
import os,re,sys

'''
数据源：dw数据库表 
dw.qyxx_jqka_ipo_gdxx  同花顺-股东信息
dw.qyxx_jqka_ipo_baxx  同花顺-备案信息
dw.qyxx_jqka_ipo_basic 同花顺-基本信息 
dw.ktgg                开庭公告
dw.legal_adjudicative_documents 裁判文书
dw.qyxx_xzcf       企业行政处罚信息
dw.legal_persons_subject_to_enforcement 被执行人
dw.legal_dishonest_persons_subject_to_enforcement 失信被执行人
启动命令：
'''

def pre_check(entry: Entry):
    return True
def listed_name(spark,outputdata,report_date,entry):
    """
    上市公司名单
    :return:
    """
    listed_name_table= spark.sql(
        """ with tmp as(
        SELECT stock_code,company_name,bbd_qyxx_id
        ,frname,frname_id,frname_compid,report_date,ulcontroller
        ,row_number() over(partition by stock_code,company_name order by report_date desc) rk
        from dw.qyxx_jqka_ipo_basic
        where dt = '{}'
        and report_date>='{}'
        and bbd_qyxx_id<>'null'
        and substr(stock_code,1,1) in ('3','0','6')
        )
        select stock_code `code`
        ,case when a.company_name='null' then '' else a.company_name end  as  company_name
        ,bbd_qyxx_id  
        ,case when a.frname='null' then '' else a.frname end  as   frname
        ,case when a.frname_id='null' then '' else a.frname_id end  as   frname_id
        ,case when a.frname_compid='null' then '' else a.frname_compid end  as   frname_compid
        ,report_date 
        ,a.ulcontroller
        from tmp a  
        where rk =1""".format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_jqka_ipo_basic"),report_date)).persist(StorageLevel.MEMORY_AND_DISK)
    entry.logger.info(f"开始listed_name数据")
    listed_name_table.coalesce(1).write.json(outputdata + '/listed_name',  mode='overwrite')
    listed_name_table.createOrReplaceTempView('listed_name')
    ##上市关联方
    listed_relation_company_table=spark.sql(
     '''with cr as(
        select bbd_qyxx_id,company_name,source_bbd_id,source_name,source_isperson,destination_bbd_id,destination_name,destination_isperson
        from dw.off_line_relations
        where dt = '{}'
        ),
        OLC as(
        select a.*
        from cr a inner join listed_name b
        on a.bbd_qyxx_id=b.bbd_qyxx_id
        )
        
        select  bbd_qyxx_id,company_name
        from OLC
        union all
        select  source_bbd_id  bbd_qyxx_id,source_name company_name
        from OLC
        where source_isperson=0
        union 
        select  destination_bbd_id  bbd_qyxx_id,destination_name company_name
        from OLC
        where destination_isperson=0
        '''.format(HiveUtil.newest_partition_with_data(spark,"dw.off_line_relations" )))\
        .dropDuplicates(["bbd_qyxx_id","company_name"])\
        .persist(StorageLevel.MEMORY_AND_DISK)
    entry.logger.info(f"开始listed_relation_company数据")
    listed_relation_company_table.coalesce(1).write.json(outputdata + '/listed_relation_company',mode='overwrite')
    listed_relation_company_table.createOrReplaceTempView('listed_relation_company')
def qyxx_table_new_partitions(spark,outputdata,entry):

    tables=["dw.qyxx_jqka_ipo_basic","dw.qyxx_jqka_ipo_baxx","dw.qyxx_jqka_ipo_gdxx","dw.qyxx_jqka_ipo_fxxg","dw.qyxx_jqka_ipo_kggs","dw.qyxx_jqka_ipo_ltgd"]
    for table_name in tables:
        entry.logger.info("开始{}数据".format(table_name[3:]))
        spark.sql('''
        select a.* from {} a where dt='{}'   --同花顺——基本、备案、股东、发行、控股、流通股东信息
        '''.format(table_name,HiveUtil.newest_partition_with_data(spark,table_name)))\
            .coalesce(10).write.json( os.path.join(outputdata,table_name[3:]),  mode='overwrite')
    entry.logger.info("开始qyxx_basic数据")
    #临时处理：
    spark.read.json(os.path.join(outputdata, "listed_name")).createOrReplaceTempView("listed_name")
    spark.sql('''
        select a.* 
        from dw.qyxx_basic a 
        where dt='{}'   --同花顺——基本、备案、股东、发行、控股、流通股东信息
        and exists(select 1 from listed_name b where a.bbd_qyxx_id=b.bbd_qyxx_id)
        '''.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_basic")))\
            .coalesce(10).write.json( os.path.join(outputdata,"qyxx_basic"),  mode='overwrite')




def basic_info(spark,outputdata,address_detail,paquest_path):
    ##工商信息表数据basic_info
    spark.sql(
        '''select a.* from dw.qyxx_basic  a
          where dt='{}' 
          and  substring (company_companytype,0,2)!='93'
          and exists(select  1 from listed_relation_company b where  a.bbd_qyxx_id = b.bbd_qyxx_id)
        '''.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_basic"))).repartition(10).write.parquet(paquest_path + "/qyxx_basic", mode="overwrite")
    spark.read.parquet(paquest_path + "/qyxx_basic").createOrReplaceTempView("tmp_qyxx_basic_dt")
    spark.sql(
        '''
        select regexp_replace(l.code, '\\\.SZ|\\\.SH','') as sec_code,l.company_name,
                  r.bbd_qyxx_id, r.credit_code, r.frname, r.company_type, 
                  concat(cast(regexp_replace(r.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap, 
                  r.regorg, r.address, r.operate_scope, r.company_enterprise_status enterprise_status, 
                  r.esdate, r.approval_date, r.openfrom, r.opento, r.company_county, 
                  if(locate('存续',r.company_enterprise_status)>0, '1', '2') flag 
                  from listed_name l join tmp_qyxx_basic_dt as r 
                  on l.bbd_qyxx_id=r.bbd_qyxx_id
        '''
    ).createOrReplaceTempView('basic_info_part1')
    spark.read.csv(address_detail,sep='\t',  header=True).createOrReplaceTempView("address_detail")
    spark.sql(
        '''
        with cr as(
        select sec_code,company_name,bbd_qyxx_id,credit_code,frname,company_type,regcap,regorg,address,operate_scope,enterprise_status
        ,esdate,approval_date,openfrom,opento,company_county,flag
        from (select a.*,row_number() over(distribute by company_name order by flag asc ) rk
        from basic_info_part1 a
        ) temp
        where rk =1
        )
        select replace(coalesce(sec_code, ''),'null','')   sec_code
              , l.bbd_qyxx_id
              , credit_code
              , frname
              , company_type
              ,coalesce(regcap, '') regcap
              , regorg
              , address
              ,regexp_replace(operate_scope,'\\t','') operate_scope
              , enterprise_status
              ,esdate
              ,approval_date
              ,coalesce(openfrom, '') openfrom
              ,replace(coalesce(opento, ''),'null','')   opento
              ,coalesce(r.province, '') province
              ,coalesce(r.city, '') city
              ,coalesce(r.county, '') county 
        from cr l 
        left join address_detail r 
        on l.company_county = r.company_county
        ''').dropDuplicates(["sec_code"]).coalesce(1).write.json(outputdata + '/basic_info', mode='overwrite')

    ##变更数据

    spark.sql(
        '''
        with qyxx_bgxx_merge_dt as
        (select bbd_qyxx_id,change_items,content_before_change,content_after_change,change_date 
        from dw.qyxx_bgxx_merge 
        where dt='{}'
        )
        select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
                case when r.change_items='null' then '' else r.change_items end as change_items,
                regexp_replace(r.content_before_change, '\\t|null', '')  content_before_change, 
                regexp_replace(r.content_after_change, '\\t|null', '')  content_after_change , 
                cast (change_date as string)  change_date
        from listed_name l 
        join qyxx_bgxx_merge_dt r on l.bbd_qyxx_id=r.bbd_qyxx_id 
        '''.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_bgxx_merge"))).dropDuplicates(["sec_code", "change_items"]).coalesce(1) \
        .write.json(outputdata + '/alter_info',  mode='overwrite')
    ##分支机构
    spark.sql(
        '''
        with qyxx_fzjg_clean_dt as
        (select bbd_branch_id,`name` from dw.qyxx_fzjg_clean where dt='{}' and bbd_branch_id<>'null')
        select regexp_replace(l.code, '\\\.SZ|\\\.SH','')  sec_code
                ,r.name as  branch_name
                ,q.address 
        from listed_name l 
        join qyxx_fzjg_clean_dt r  on l.bbd_qyxx_id=r.bbd_branch_id 
        join tmp_qyxx_basic_dt q on r.bbd_branch_id=q.bbd_qyxx_id
        '''.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_fzjg_clean"))
    ).dropDuplicates(["sec_code", "branch_name"]).coalesce(1) \
        .write.json(outputdata + '/branch_info',  mode='overwrite')
def shareholder_info(spark,outputdata,report_date):
    """
    股东信息
    :return:
    """
    shareholder_info_table=spark.sql("""
            select  a.stock_code as sec_code	    --代码
                    ,shareholder_name  as shareh_name --股东名	
                    ,case when invest_amount rlike '亿' then  split(invest_amount,'亿')[0]*100000000  else nvl(split(invest_amount,'万')[0],1)*10000 end  as shares_held --股东持有量
                    ,case when a.invest_ratio='null' then '' else a.invest_ratio end as share_perce --认缴出资比例
                    ,cast(a.report_date as string) as cninfo_year --报告期
                    ,case when name_compid=0 then '法人'  else   '自然人' end  as type  --股东类型
             from dw.qyxx_jqka_ipo_gdxx a   --同花顺-股东信息
             where dt='{}'
             and name_compid<2
             and report_date>='{}'
             and stock_code<>'null'
             and shareholder_name<>'null'
        """.format(HiveUtil.newest_partition_with_data(spark,'dw.qyxx_jqka_ipo_gdxx'),report_date)) \
           .dropDuplicates(["sec_code","shareh_name","shares_held","share_perce","cninfo_year","type"])
    shareholder_info_table.coalesce(1) \
                         .write.json(outputdata + '/shareholder_info', mode='overwrite')
    shareholder_info_table.createOrReplaceTempView("shareholder_info")
def manager_info(spark,outputdata,report_date):
    """
    董监高信息
    :return:
    """
    manager_info_table=spark.sql("""
            select stock_code as sec_code  --证券代码
                ,case when name='null' then '' else name end as name      --姓名
                ,max(case when position='null' then '' else position end) position   --职位
                ,max(case when resume='null' then '' else resume end) resume  --简历
                ,'' sex
                ,'' age
                ,'' education
                ,max(cast(report_date as string)) as cninfo_year --报告期
            from dw.qyxx_jqka_ipo_baxx a
            where dt='{}'
            and report_date>='{}'
            and stock_code<>'null'
            and name<>'null'
            group by stock_code,case when name='null' then '' else name end
        """.format(HiveUtil.newest_partition_with_data(spark,'dw.qyxx_jqka_ipo_baxx'),report_date))\
        .dropDuplicates(["sec_code","name","position","resume","sex","age","education","cninfo_year"])
    manager_info_table.coalesce(1).write.json(outputdata + '/manager_info', mode='overwrite')
    manager_info_table.createOrReplaceTempView("manager_info")
def lawsuit_info(spark,outputdata):
    """
    涉及诉讼
    :return:
    """
    lawsuit_info_table=spark.sql("""
            WITH tmp1 as  
            (select '开庭公告'type
                    ,bbd_qyxx_id
                    ,case_code   --案号
                    ,litigant    --涉及主体
                    ,title       --标题
                    ,main
                    ,trial_date as date
                    ,city
                    ,trial_date  --开庭时间
            from dw.ktgg a
            where dt='{}'
            union all 
            select  '裁判文书'type
                ,bbd_qyxx_id
                ,case_code   --案号
                ,litigant
                ,title
                ,notice_content as main
                ,sentence_date as date
                ,province as city 
                ,ctime as trial_date --入库时间
            from dw.legal_adjudicative_documents
            where dt='{}'
            )
            select  regexp_replace(a.code, '\\\.SZ|\\\.SH','') as sec_code  --证券代码
                    ,b.type      --文书类型
                    ,case when b.case_code='null' then '' else b.case_code end as case_code   --案号
                    ,case when b.litigant='null' then '' else b.litigant end as litigant    --涉及主体
                    ,case when b.title='null' then '' else b.title end as title      --标题
                    ,case when b.main='null' then '' else b.main end as main       --
                    ,cast (b.date as string)  as date
                    ,case when b.city='null' then '' else b.city end as city
            from listed_name a 
            join tmp1  b 
            on a.bbd_qyxx_id=b.bbd_qyxx_id
        """.format(HiveUtil.newest_partition_with_data(spark,'dw.ktgg'),HiveUtil.newest_partition_with_data(spark,'dw.legal_adjudicative_documents'))) \
        .select("sec_code","type","case_code","litigant","title","main","date","city") \
        .dropDuplicates(["sec_code","case_code"])
    lawsuit_info_table.coalesce(3).write.json(outputdata + '/lawsuit_info', mode='overwrite')
    lawsuit_info_table.createOrReplaceTempView("lawsuit_info")
def punish_info(spark,outputdata):
    """
    涉及处罚信息
    :return:
    """
    punish_info_table=spark.sql("""
                WITH tmp1 as  
                (select bbd_qyxx_id
                        ,name as punish_name
                        ,punish_code
                        ,frname as punish_frname
                        ,'暂无详情' punish_law
                        ,punish_org
                        ,punish_date
                from dw.qyxx_xzcf
                where dt='{}'
                union all 
                select bbd_qyxx_id
                        ,title as punish_name
                        ,'暂无详情' punish_code
                        ,litigant as punish_frname
                        ,'暂无详情' punish_law
                        ,'保监会' as punish_org  --处罚机关
                        ,pubdate as punish_date
                from dw.qyxg_circxzcf
                where dt='{}'
                union all 
                select bbd_qyxx_id
                        ,case_name as punish_name
                        , punish_code
                        ,name as punish_frname
                        ,punish_basis as  punish_law
                        ,punish_org                 --处罚机关
                        ,public_date as punish_date
                from dw.xzcf
                where dt='{}'
                )
                select  a.bbd_qyxx_id
                        ,regexp_replace(a.code, '\\\.SZ|\\\.SH','') as sec_code     --证券代码
                        ,case when b.punish_name='null' then ''  else b.punish_name end as punish_name   
                        ,case when b.punish_code='null' then ''  else b.punish_code end as punish_code
                        ,case when b.punish_frname='null' then ''  else b.punish_frname end as punish_frname       --
                        ,case when b.punish_law='null' then ''  else b.punish_law end as punish_law
                        ,case when b.punish_org='null' then ''  else b.punish_org end as punish_org
                        ,case when b.punish_date='null' then ''  else b.punish_date end as punish_date
                from listed_name a 
                join tmp1  b 
                on a.bbd_qyxx_id=b.bbd_qyxx_id
            """.format(HiveUtil.newest_partition_with_data(spark,'dw.qyxx_xzcf'),HiveUtil.newest_partition_with_data(spark,'dw.qyxg_circxzcf'),HiveUtil.newest_partition_with_data(spark,'dw.xzcf')))\
                .dropDuplicates(["sec_code","punish_code","punish_date"])
    punish_info_table.select ("sec_code","punish_name","punish_code","punish_frname","punish_law","punish_org","punish_date") \
                    .coalesce(2).write.json(outputdata + '/punish_info',  mode='overwrite')
    punish_info_table.createOrReplaceTempView("punish_info")
def zhixing_info(spark,outputdata):
    """
    被执行人信息
    :return:
    """
    zhixing_info_table=spark.sql("""
               select a.*,regexp_replace(b.code, '\\\.SZ|\\\.SH','') as sec_code   --证券代码
                from (select a.bbd_qyxx_id
                            ,case when case_code='null' then '' else case_code end as case_code
                            ,case when pname_origin='null' then '' else pname_origin end as pname_origin 
                            ,case when exec_subject='null' then '' else exec_subject end as exec_subject
                            ,case when exec_court_name='null' then '' else exec_court_name end as exec_court_name
                            ,case when register_date='null' then '' else register_date end as register_date
                    from dw.legal_persons_subject_to_enforcement a
                    where dt='{}'
                    and bbd_qyxx_id<>'null'
                ) a 
                inner join  listed_name b 
                on a.bbd_qyxx_id=b.bbd_qyxx_id
            """.format(HiveUtil.newest_partition_with_data(spark,'dw.legal_persons_subject_to_enforcement'))) \
                .dropDuplicates(["sec_code", "case_code"])
    zhixing_info_table.select ("sec_code","case_code","pname_origin","exec_subject","exec_court_name","register_date") \
                    .coalesce(2) \
                    .write.json(outputdata + '/zhixing_info', mode='overwrite')
    zhixing_info_table.createOrReplaceTempView("zhixing_info")
def dishonesty_info(spark,outputdata):
    """
    失信被执行人
    :return:
    """
    dishonesty_info_table=spark.sql("""
               select a.*,regexp_replace(b.code, '\\\.SZ|\\\.SH','') as sec_code   --证券代码
                from (select bbd_qyxx_id 
                          ,case when case_code='null' then '' else case_code end as case_code
                          ,case when pname='null' then '' else pname end as pname
                          ,case when dishonest_situation='null' then '' else dishonest_situation end as dishonest_situation
                          ,case when exec_court_name='null' then '' else exec_court_name end as exec_court_name
                          ,case when pubdate='null' then '' else pubdate end as pubdate
                    from dw.legal_dishonest_persons_subject_to_enforcement 
                    where dt='{}'
                    and bbd_qyxx_id<>'null'
                ) a 
                inner join  listed_name b 
                on a.bbd_qyxx_id=b.bbd_qyxx_id
            """.format(HiveUtil.newest_partition_with_data(spark,'dw.legal_dishonest_persons_subject_to_enforcement'))) \
                 .dropDuplicates(["sec_code", "case_code"])
    dishonesty_info_table.select ("sec_code","case_code","pname","dishonest_situation","exec_court_name","pubdate") \
                        .coalesce(2) \
                        .write.json(outputdata + '/dishonesty_info',  mode='overwrite')
    dishonesty_info_table.createOrReplaceTempView("dishonesty_info")
def person_basic_info(spark,outputdata,report_date,bjx):
    """
    个人基础信息
    :return:
    """
    def basic(data):
        frname = data.name
        if frname is not None and frname != '' and frname != 'null':
            if frname[0] in bjx:
                return Row(sec_code=data.sec_code,name=data.name,report_date=data.report_date)
            else:
                None
        else:
            None

    person_basic_info_rdd = spark.sql("""
                    with tmp1 as(
                     select stock_code
                                 ,split(ulcontroller,'\\\(|（')[0] as ulcontroller
                                 ,max(cast(report_date as string)) report_date
                    from dw.qyxx_jqka_ipo_basic
                    where dt='{}'
                    and report_date>='{}'
                    group by stock_code,split(ulcontroller,'\\\(|（')[0]
                    )
                   select case when stock_code='null' then '' else stock_code end as  sec_code
                            ,name
                            ,cast(report_date as string) as report_date--报告期
                    from tmp1 LATERAL VIEW explode(split(ulcontroller,'\\\、|\\\，|\\\,')) d AS name
                    where length(name)<4 
                    """.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_jqka_ipo_basic"), report_date)).rdd\
        .map(basic)\
        .filter(lambda x: x is not None)
    spark.createDataFrame(person_basic_info_rdd).createOrReplaceTempView("tmp2")
    person_basic_info_table=spark.sql("""
        with tmp3 as (  
                select    sec_code
                        ,name
                        ,'实控人' type
                        ,''sex
                        ,''age
                        ,''education
                        ,''position
                        ,'' ratio
                        ,'' nationality
                        ,report_date--报告期
                        ,'' resume
                from tmp2
                union                           
                select  sec_code
                        ,shareh_name as name
                        ,'股东' type
                        ,''sex
                        ,''age
                        ,''education
                        ,''position
                        ,share_perce as ratio
                        ,'' nationality
                        ,cninfo_year  as report_date
                        ,'' resume
                from shareholder_info
                where type='自然人'
                union  
                select  sec_code
                        ,name
                        ,'董监高' as type
                        ,''sex
                        ,''age
                        ,''education
                        ,position
                        ,''ratio
                        ,''nationality
                        ,cninfo_year as report_date
                        ,resume
                from manager_info 
                union  
                select regexp_replace(code, '\\\.SZ|\\\.SH','') as sec_code
                        ,frname as name
                        ,'法人'type
                        ,''sex
                        ,''age
                        ,''education
                        ,''position
                        ,''ratio
                        ,''nationality
                        ,report_date
                        ,''resume
                from listed_name 
                where frname_compid=1
                )
                select sec_code
                      ,name
                      ,max(sex) as sex
                      ,max(age) as age
                      ,max(education) as education
                      ,max(position) as position
                      ,max(ratio) as ratio
                      ,max(nationality) as nationality
                      ,max(report_date) as report_date
                      ,max(resume) as resume
                      ,concat_ws('、',collect_set(type)) type
                from tmp3
                group by sec_code,name
            """)
    person_basic_info_table.select ("sec_code","name","type","sex","age","education","position","ratio","nationality","report_date","resume").coalesce(2) \
                            .write.json(outputdata + '/person_basic_info',  mode='overwrite')
    person_basic_info_table.createOrReplaceTempView("person_basic_info")
# def out_invest(spark,outputdata,listed_person_unique,relation_path):
#     '''
# 对外投资信息
#     '''
#     spark.read.csv(listed_person_unique, sep=',', header=True).createOrReplaceTempView("listed_person_unique")
#     spark.read.parquet(os.path.join(relation_path, "edge_person_invest_company")).createOrReplaceTempView("person_invest_company")
#     spark.sql("""
#         with qyxx_frinv_dt as
#         (select bbd_qyxx_id,frname,entname
#         from dw.qyxx_frinv
#         where dt='{}'
#         ),
#         part_2_1 as(
#         select l.code,s.name,s.uuid
#         from listed_name l
#         join listed_person_unique s
#         on l.bbd_qyxx_id=s.bbd_qyxx_id
#         ),
#         part_2_2 as(
#         select l.code,l.name,s.endnode
#         from part_2_1 l
#         join person_invest_company s
#         on l.uuid=s.startnode
#         )
#         select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
#                 replace(coalesce(s.frname, ''),'null','') name,
#                 replace(coalesce(s.entname, ''),'null','') company_name,
#                 concat(cast(regexp_replace(r.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap,
#                 replace(coalesce(r.esdate, ''),'null','') esdate,
#                 replace(coalesce(r.address, ''),'null','') address
#         from listed_name l
#         join qyxx_frinv_dt s
#         on l.bbd_qyxx_id=s.bbd_qyxx_id
#         join tmp_qyxx_basic_dt r
#         on s.entname = r.company_name
#         union
#         select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
#                                 replace(coalesce(l.name, ''),'null','') name,
#                                 replace(coalesce(s.company_name, ''),'null','') company_name,
#                                 concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap,
#                                 replace(coalesce(s.esdate, ''),'null','') esdate,
#                                 replace(coalesce(s.address, ''),'null','') address
#
#         from part_2_2 l
#         join tmp_qyxx_basic_dt s
#         on l.endnode=s.bbd_qyxx_id
#         """.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_frinv"))).dropDuplicates(["sec_code","name","company_name"]) \
#         .coalesce(1) \
#         .write.json(outputdata + '/out_invest', mode='overwrite')
# def out_position(spark,outputdata,relation_path):
#     '''
# 对外任职信息
#     '''
#     spark.read.parquet(os.path.join(relation_path, "edge_person_chairmanof_company")).createOrReplaceTempView("person_chairmanof_company")
#     spark.read.parquet(os.path.join(relation_path, "edge_person_excutiveof_company")).createOrReplaceTempView("person_excutiveof_company")
#     spark.read.parquet(os.path.join(relation_path, "edge_person_managerof_company")).createOrReplaceTempView("person_managerof_company")
#     spark.sql("""
#         with  qyxx_frposition_dt as
#         (select bbd_qyxx_id,frname,entname,position
#         from dw.qyxx_frposition
#         where dt='{}'
#         ),
#         part_2 as(
#         select l.code,s.name,s.uuid
#         from listed_name l
#         join listed_person_unique s
#         on l.bbd_qyxx_id=s.bbd_qyxx_id
#         ),
#         part_3_1 as(
#         select l.code,l.name,s.endnode
#         from part_2 l
#         join person_chairmanof_company s
#         on l.uuid=s.startnode
#         ),
#         part_3_2 as(
#         select l.code,l.name,s.endnode
#         from part_2 l
#         join person_excutiveof_company s
#         on l.uuid=s.startnode
#         ),
#         part_3_3 as(
#         select l.code,l.name,s.endnode
#         from part_2 l
#         join person_managerof_company s
#         on l.uuid=s.startnode
#         )
#         select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
#                                 replace(coalesce(s.frname, ''),'null','') name,
#                                 replace(coalesce(s.entname, ''),'null','') company_name,
#                                 replace(coalesce(s.position, ''),'null','')  position,
#                                 concat(cast(regexp_replace(r.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap,
#                                 replace(coalesce(cast(r.esdate as string), ''),'null','') esdate,
#                                 replace(coalesce(r.address, ''),'null','') address
#         from listed_name l
#         join qyxx_frposition_dt s
#         on l.bbd_qyxx_id=s.bbd_qyxx_id
#         join tmp_qyxx_basic_dt r
#         on s.entname = r.company_name
#         union
#         select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
#                                 replace(coalesce(l.name, ''),'null','') name,
#                                 replace(coalesce(s.company_name, ''),'null','') company_name,
#                                 '董事' position,
#                                 concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap,
#                                 replace(coalesce(cast(s.esdate as string), ''),'null','') esdate,
#                                 replace(coalesce(s.address, ''),'null','') address
#         from part_3_1 l
#         join tmp_qyxx_basic_dt s
#         on l.endnode=s.bbd_qyxx_id
#         union
#         select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
#                                 replace(coalesce(l.name, ''),'null','') name,
#                                 replace(coalesce(s.company_name, ''),'null','') company_name,
#                                 '监事' position,
#                                 concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap,
#                                 replace(coalesce(s.esdate, ''),'null','') esdate,
#                                 replace(coalesce(s.address, ''),'null','') address
#         from part_3_2 l
#         join tmp_qyxx_basic_dt s
#         on l.endnode=s.bbd_qyxx_id
#         union
#         select  regexp_replace(l.code, '\\\.SZ|\\\.SH','') sec_code,
#                                 replace(coalesce(l.name, ''),'null','') name,
#                                 replace(coalesce(s.company_name, ''),'null','') company_name,
#                                 '高管' position,
#                                 concat(cast(regexp_replace(s.regcap, '[\\u4e00-\\u9fa5]', '') as double), '万元') regcap,
#                                 replace(coalesce(s.esdate, ''),'null','') esdate,
#                                 replace(coalesce(s.address, ''),'null','') address
#         from part_3_3 l
#         join tmp_qyxx_basic_dt s
#         on l.endnode=s.bbd_qyxx_id
#     """.format(HiveUtil.newest_partition_with_data(spark,"dw.qyxx_frposition"))).dropDuplicates(["sec_code","name","company_name"]) \
#         .coalesce(1) \
#         .write.json(outputdata + '/out_position', mode='overwrite')
def person_lawsuit_info(spark,outputdata):
    """
        个人涉及诉讼
        :return:
        """
    spark.sql("""
          with  tmp1 as (select sec_code  --证券代码
                ,regexp_replace(litigant_1,'当事人|原告|被告|\\\：|\\\:','')   as name 
                ,type
                ,case_code
                ,litigant  --涉及主体
                ,title
                ,main  --正文
                ,date
                ,city
           from lawsuit_info  LATERAL VIEW explode(split(litigant,'\\\;|\\\,|\\\；')) d AS litigant_1
           where litigant<>'null' 
           and litigant<>''
           ) 
           select a.sec_code  --证券代码
                ,b.name
                ,a.type
                ,a.case_code
                ,a.litigant  --涉及主体
                ,a.title
                ,a.main  --正文
                ,a.date
                ,a.city
           from lawsuit_info a
           inner join  person_basic_info b on  a.sec_code=b.sec_code
           union all 
           select sec_code  --证券代码
                ,name
                ,type
                ,case_code
                ,litigant  --涉及主体
                ,title
                ,main  --正文
                ,date
                ,city 
            from tmp1 a 
            where exists(select 1 from person_basic_info b where a.name=b.name)
              """).dropDuplicates(["sec_code", "case_code","name"]) \
        .coalesce(50) \
        .write.json(outputdata + '/person_lawsuit_info', mode='overwrite')
def person_punish_info(spark,outputdata):
    """
        个人涉及处罚
        :return:
        """
    spark.sql("""
          with  tmp1 as (
          select  punish_name
                ,punish_code
                ,punish_frname
                ,punish_law
                ,punish_org
                ,punish_date
                ,punish_frname_1 as name 
                ,sec_code
          from punish_info LATERAL VIEW explode(split(punish_frname,'\\\;|\\\,|\\\；')) d AS punish_frname_1
          where punish_frname<>'null'
          and punish_frname<>''
           ) 
           SELECT a.sec_code
                ,b.name
                ,a.punish_name
                ,a.punish_code
                ,a.punish_frname
                ,a.punish_law
                ,a.punish_org
                ,a.punish_date
           from punish_info a 
           inner join  person_basic_info b on  a.sec_code=b.sec_code
           union all 
           select a.sec_code
                ,a.name
                ,a.punish_name
                ,a.punish_code
                ,a.punish_frname
                ,a.punish_law
                ,a.punish_org
                ,a.punish_date
            from tmp1 a 
            where exists(select 1 from person_basic_info b where a.name=b.name)
              """).dropDuplicates(["sec_code", "punish_name","name"]) \
        .coalesce(2) \
        .write.json(outputdata + '/person_punish_info', mode='overwrite')
def person_zhixing_info(spark,outputdata):
    """
        个人被执行人
        :return:
        """
    spark.sql("""
          with  tmp1 as (
          select case_code
                ,pname_origin_1 as name 
                ,pname_origin
                ,exec_subject
                ,exec_court_name
                ,register_date
                ,sec_code
          from zhixing_info  LATERAL VIEW explode(split(pname_origin,'\\\;|\\\,|\\\；')) d AS pname_origin_1
          where pname_origin<>'null'
          and pname_origin<>''
           ) 
           SELECT a.sec_code
                ,b.name
                ,a.case_code
                ,a.pname_origin
                ,a.exec_subject
                ,a.exec_court_name
                ,a.register_date
           from zhixing_info a 
           inner join  person_basic_info b on  a.sec_code=b.sec_code
           union all 
           select a.sec_code
                ,a.name
                ,a.case_code
                ,a.pname_origin
                ,a.exec_subject
                ,a.exec_court_name
                ,a.register_date
            from tmp1 a 
            where exists(select 1 from person_basic_info b where a.name=b.name)
              """).dropDuplicates(["sec_code", "case_code","name"]) \
        .coalesce(2) \
        .write.json(outputdata + '/person_zhixing_info', mode='overwrite')
def person_dishonesty_info(spark,outputdata):
    """
        个人失信被执行人
        :return:
        """
    spark.sql("""
          with  tmp1 as (
          select sec_code
                ,pname_1 as name 
                ,case_code
                ,pname
                ,dishonest_situation
                ,exec_court_name
                ,pubdate
          from dishonesty_info  LATERAL VIEW explode(split(pname,'\\\;|\\\,|\\\；')) d AS pname_1
          where pname<>''
          and pname<>'null'
           ) 
           SELECT a.sec_code
                ,b.name 
                ,a.case_code
                ,a.pname
                ,a.dishonest_situation
                ,a.exec_court_name
                ,a.pubdate
           from dishonesty_info a 
           inner join  person_basic_info b on  a.sec_code=b.sec_code
           union all 
           select a.sec_code
                ,a.name 
                ,a.case_code
                ,a.pname
                ,a.dishonest_situation
                ,a.exec_court_name
                ,a.pubdate
            from tmp1 a 
            where exists(select 1 from person_basic_info b where a.name=b.name)
              """).dropDuplicates(["sec_code", "case_code","name"]) \
        .coalesce(2) \
        .write.json(outputdata + '/person_dishonesty_info', mode='overwrite')

def main(entry: Entry):
    """
       程序主入口,配置初始化和业务逻辑入口 newest_partition_with_data
       """
    entry.logger.info(entry.cfg_mgr.get("spark-submit-opt", "queue"))
    entry.logger.info("start")
    spark= entry.spark

    outputdata= entry.cfg_mgr.hdfs.get_result_path("hdfs", "hdfs_path")
    paquest_path=entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path")
    relation_path = entry.cfg_mgr.hdfs.get_tmp_path("hdfs", "hdfs_path_relation")

    address_detail="/user/jiangyin/zjh/address_detail.csv"
    listed_person_unique="/user/bf-zg11/input/listed_person_unique.csv"
    now_date=datetime.now().strftime('%Y-%m-%d')
    if now_date[6:]>'04-30':
        report_date=str(int(now_date[0:4])-1)+'-12-30'
    else:
        report_date =str(int(now_date[0:4])-1)+ '-09-30'
    bjx = u'赵|钱|孙|李|周|吴|郑|王|冯|陈|褚|卫|蒋|沈|韩|杨|朱|秦|尤|许|何|吕|施|张|孔|曹|严|华|金|魏|陶|姜|戚|谢|邹|喻|柏|水|窦|章|云|苏|潘|葛|奚|范|彭|郎|鲁|韦|昌|马|苗|凤|花|方|俞|任|袁|柳|酆|鲍|史|唐|费|廉|岑|薛|雷|贺|倪|汤|滕|殷|罗|毕|郝|邬|安|常|乐|于|时|傅|皮|卞|齐|康|伍|余|元|卜|顾|孟|平|黄|和|穆|萧|尹|姚|邵|湛|汪|祁|毛|禹|狄|米|贝|明|臧|计|伏|成|戴|谈|宋|茅|庞|熊|纪|舒|屈|项|祝|董|粱|杜|阮|蓝|闵|席|季|麻|强|贾|路|娄|危|江|童|颜|郭|梅|盛|林|刁|钟|徐|邱|骆|高|夏|蔡|田|樊|胡|凌|霍|虞|万|支|柯|昝|管|卢|莫|经|房|裘|缪|干|解|应|宗|丁|宣|贲|邓|郁|单|杭|洪|包|诸|左|石|崔|吉|钮|龚|程|嵇|邢|滑|裴|陆|荣|翁|荀|羊|於|惠|甄|麴|家|封|芮|羿|储|靳|汲|邴|糜|松|井|段|富|巫|乌|焦|巴|弓|牧|隗|山|谷|车|侯|宓|蓬|全|郗|班|仰|秋|仲|伊|宫|宁|仇|栾|暴|甘|钭|厉|戎|祖|武|符|刘|景|詹|束|龙|叶|幸|司|韶|郜|黎|蓟|薄|印|宿|白|怀|蒲|邰|从|鄂|索|咸|籍|赖|卓|蔺|屠|蒙|池|乔|阴|欎|胥|能|苍|双|闻|莘|党|翟|谭|贡|劳|逄|姬|申|扶|堵|冉|宰|郦|雍|舄|璩|桑|桂|濮|牛|寿|通|边|扈|燕|冀|郏|浦|尚|农|温|别|庄|晏|柴|瞿|阎|充|慕|连|茹|习|宦|艾|鱼|容|向|古|易|慎|戈|廖|庾|终|暨|居|衡|步|都|耿|满|弘|匡|文|寇|广|禄|阙|东|殴|殳|沃|利|蔚|越|夔|隆|师|巩|厍|聂|晁|勾|敖|融|冷|訾|辛|阚|那|简|饶|空|曾|毋|沙|乜|养|鞠|须|丰|巢|关|蒯|相|查|後|荆|红|游|竺|权|逯|盖|益|桓|公|上|欧|赫|皇|尉|澹|淳|太|轩|令|宇|长|鲜|闾|亓|仉|督|子|颛|端|漆|壤|拓|夹|晋|楚|闫|法|汝|鄢|涂|钦|百|东|南|呼|归|海|微|岳|帅|缑|亢|况|后|有|琴|西|商|牟|佘|佴|伯|赏|墨|哈|谯|笪|年|爱|阳|佟|第|言|福|楼|勇|梁|鹿|付|仝|慧|肖|卿|苟|茆|瘳|覃|檀|片|代|青|珠|旷|服|衣|粟|基|纳|伞|热|尧|弥|次|操|扬|曲|门|敏|官|占|英|央|志|今|达|丘|刑|朴|扎|巨|尢|光|夕|佐|凃|风|延|色|延|由|敬|户|麦|芦|赛|兰|催|锁|占|雒|育|苓|隋|缐|来|但|才|宠|蹇|凡|宠|昂|知|丛|雪|候|蒿|原|阆|阿|忻|过|校|闪|伦|德|巾|缴|潭|将|礼|啕|星|钧|营|厚|艺|鮑|慈|郄|崖|嘉|维|傲|冶|买|革|丽|普|泰|卡|攀|北|疏|邸|协|洛|排|禤|同|展|矣|及|腾|苑|圣|折|静|露|随|迟|库|无|陕|庆|栗|闭|多|尕|畅|俄|栗|修|卲|宛|血|谌|布|句|练|战|立|化|智|紫|邝|依|佳|区|贠|竹|闰|开|冼|母|薜|承|是|侍|琼|辜|塔|知|本|位|丹｜税｜续｜綦|揭|镡' \
        .split('|')
    # 指标计算
    entry.logger.info(f"sparksql计算数据开始")
    qyxx_table_new_partitions(spark,outputdata,entry)
    # listed_name(spark,outputdata,report_date,entry)
    #
    # entry.logger.info(f"开始执行 basic_info数据")
    # basic_info(spark,outputdata,address_detail,paquest_path)
    #
    # entry.logger.info(f"开始执行 shareholder_info数据")
    # shareholder_info(spark,outputdata,report_date)
    #
    # entry.logger.info(f"开始执行 manager_info数据")
    # manager_info(spark,outputdata,report_date)
    #
    # entry.logger.info(f"开始执行 lawsuit_info数据")
    # lawsuit_info(spark,outputdata)
    #
    # entry.logger.info(f"开始执行 punish_info数据")
    # punish_info(spark,outputdata)
    #
    # entry.logger.info(f"开始执行 zhixing_info数据")
    # zhixing_info(spark,outputdata)
    #
    # entry.logger.info(f"开始执行 dishonesty_info数据")
    # dishonesty_info(spark,outputdata)
    #
    # entry.logger.info(f"开始执行 person_basic_info数据")
    # person_basic_info(spark,outputdata,report_date,bjx)
    #
    # entry.logger.info(f"开始执行person_punish_info数据")
    # person_punish_info(spark,outputdata)
    #
    # entry.logger.info(f"开始执行person_zhixing_info数据")
    # person_zhixing_info(spark,outputdata)
    #
    # entry.logger.info(f"开始执行pperson_dishonesty_info数据")
    # person_dishonesty_info(spark,outputdata)
    # #
    # # entry.logger.info(f"开始执行out_invest数据")
    # # out_invest(spark, outputdata,listed_person_unique,relation_path)
    # #
    # # entry.logger.info(f"开始执行out_position数据")
    # # out_position(spark,outputdata,relation_path)
    #
    # entry.logger.info(f"开始执行person_lawsuit_info数据")
    # person_lawsuit_info(spark, outputdata)

    entry.logger.info(f"执行完毕")

def post_check(entry: Entry):
    return True




