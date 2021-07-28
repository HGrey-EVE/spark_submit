### 主体数据测试流程及注意事项

#### 环境相关
    服务器: 10.28.103.31 hakcredit
    代码: /data8/hakcredit/scwgj/spark_plan_zero/rose/scwgj/graph_subject_data/test
    hive数据库: dw
    mysql:     
        ip=10.28.103.50
        port=3306
        db_name=waiguanju
        user=root
        password=bbd!@#
    配置版本: test    
    
#### 测试结果
    流程测试通过
    数据处理逻辑测试通过
    重复执行测试中，其它地方正常，mysql的增量数据同一个表的重复跑多次会有多份，理论上不影响图构建的最终结果
