### 成都经信厅项目

#### 需求相关
    svn地址: https://192.168.112.201/svn/higgs/项目/2109-四川经信厅工业智慧云平台大数据应用建设项目/1-过程库/1-正式研发/2-产品需求
    
    大数据输出数据字典: 
        https://192.168.112.201/svn/higgs/项目/2109-四川经信厅工业智慧云平台大数据应用建设项目/1-过程库/1-正式研发/5-数据库设计

#### 环境相关
    代码:
        git: http://git.bbdops.com/dsc/project/inner-project/spark_plan_zero/-/tree/master/rose/jingxinting 
        branch: master
    线上环境:
        10.28.52.29(bbd29): scjxt
        path: /data6/scjxt/jxt_code
        queue: projects.scjxt
    资源(申请量):
        150 vcore, 900 内存, 1.5T 磁盘

#### 模块介绍

- biz_base_data: 基础数据准备 许峰
- biz_domestic_compare: 国内对标 刘豪
- biz_city_development: 市州发展 刘豪
- biz_industry_5_p_1: 5+1产业 李仁超
- biz_park_analysis: 园区分析 李仁超
- biz_enterprise_monitor: 企业监测 李仁超

#### 维护相关
    各个模块负责人:
		项目负责人: 李婉聪 (李博)
		大数据指标规则: 赵如玉
		项管: 向毅
		产品: 胡凯
		模型: 梅剑、张文莎
		运维: 李林
	后端业务对接: 总: 王加平
	    刘治: 国内对标，市州发展
	    梁万: 5+1产业，园区分析
	    江楠: 企业监测
	    张冲: 

#### 任务执行
    后期各个模块都启动脚本放到 bin/module_start_script 目录下面
    总的启动脚本为 bin/start.sh,启动方式: nohup sh start.sh &
    有数据问题的可以直接在服务起上看spark脚本验证，启动 bin/start_test_shell 就可在服务器上使用 spark shell
