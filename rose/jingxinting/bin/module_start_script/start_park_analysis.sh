curday=`date -d "last month" +%Y%m`
nohup /opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_park_analysis -t park_analysis -v $curday -d -e dev > park_analysis_debug.log 2>&1 &