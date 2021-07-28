set -e
curday=`date -d "last month" +%Y%m`
/opt/anaconda3/bin/python /data4/bf-zg11/zg11_code/spark_plan_zero/stone/src/whetstone/main.py -p zg11 -m biz_basic -t zg11_basic_info -v $curday -d -e dev 2>&1 &
/opt/anaconda3/bin/python /data4/bf-zg11/zg11_code/spark_plan_zero/stone/src/whetstone/main.py -p zg11 -m biz_entity -t zg11_entity_etl -v $curday -d -e dev 2>&1