#!/bin/bash
# 获取前一天跑出来的性骚扰司机
start=$1
dt=`date -d "$start" +%Y-%m-%d`

data_result=$(hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
select
driver_id as driver_id,
product_id as product_id
from service_security.sexual_harassment_driver
where product_id in (1,2,3,4,6,7) and  concat_ws('-',year,month,day)='${dt}'
")

echo "executing adding bigdata tags"

IFS=$'\n'
k=0
for i in `echo "$data_result" | grep -v 'driver_id'`
do
    echo $k
    driver_id=`echo $i | cut -f1`
    product_id=`echo $i | cut -f2`
    token=`cat /proc/sys/kernel/random/uuid`
    echo "driver_id=${driver_id}&product_id=${product_id}&token=${token}"
    curl "http://100.69.238.59:8000/skynet/addBigdataUserTag" -d "tag_name=性骚扰司机&user_id=${driver_id}&product_id=${product_id}&token=${token}"
    let k++
done

if [ $k -gt 0 ]; then
    echo "k=$k, added $k bigdata usertag"
    echo "finishing add bigdata tags"
    curl "http://100.69.238.59:8000/skynet/finishBigdataUserTag" -d "tag_name=性骚扰司机"
else
    echo "k=$k, empty result"
fi