curday=`date -d "last month" +%Y%m`
nohup /opt/anaconda3/bin/python /data6/scjxt/jxt_code/spark_plan_zero/stone/src/whetstone/main.py -p jingxinting -m biz_company_tags -t company_tags -v $curday -d -e dev > company_tags_debug.log 2>&1 &