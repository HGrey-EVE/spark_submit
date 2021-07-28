export PYSPARK_PYTHON=/opt/anaconda3/bin/python
dt=(20201201 20210101 20210201 20210301 20210401 20210501)
for ele in ${dt[*]}
do
    /opt/anaconda3/bin/python CQ_risk_detail_scoring.py $ele
    hdfs dfs -mkdir /user/cqxdcyfzyjy/$ele/tmp/cqxdcy/model/
    hdfs dfs -put risk_result.csv /user/cqxdcyfzyjy/$ele/tmp/cqxdcy/model/
    /opt/anaconda3/bin/python /data2/cqxdcyfzyjy/f_cq/spark_plan_zero/stone/src/whetstone/main.py -p cqxdcy -m cqxdcy_req_risk -t risk_model -e dev -v $ele -d
done