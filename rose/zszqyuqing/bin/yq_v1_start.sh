#!/bin/sh
cur_date=`date +"%Y%m%d"`
/bin/sh /data2/zszqyuqing/spark_plan_zero/rose/zszqyuqing/yq_v1/sqoop_start.sh > sqoop_start_debug.log 2>&1
wait
sleep 5s
/opt/anaconda3/bin/python  /data2/zszqyuqing/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m yq_v1 -t event_main -v $cur_date -d -e dev > event_main_debug.log 2>&1
wait
sleep 5s
/opt/anaconda3/bin/python  /data2/zszqyuqing/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m yq_v1 -t result_data -v $cur_date -d -e dev > result_data_debug.log 2>&1
wait
sleep 5s
/opt/anaconda3/bin/python  /data2/zszqyuqing/spark_plan_zero/stone/src/whetstone/main.py -p zszqyuqing -m yq_v1 -t delete_data -v $cur_date -d -e dev > delete_data_debug.log 2>&1
