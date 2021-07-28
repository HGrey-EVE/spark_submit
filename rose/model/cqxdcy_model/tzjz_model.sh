export PYSPARK_PYTHON=/opt/anaconda3/bin/python
dt=(20201201 20210101 20210201 20210301 20210401 20210501)
for ele in ${dt[*]}
do
    /opt/anaconda3/bin/python CQ_invest_value_scoring.py $ele
    hdfs dfs -mkdir /user/cqxdcyfzyjy/$ele/tmp/cqxdcy/model/
    hdfs dfs -put invest_scores.csv /user/cqxdcyfzyjy/$ele/tmp/cqxdcy/model/
    /opt/anaconda3/bin/python /data2/cqxdcyfzyjy/f_cq/spark_plan_zero/stone/src/whetstone/main.py -p cqxdcy -m cqxdcy_req_tzjz -t cqxdcy_tzjz_model -e dev -v $ele -d
done