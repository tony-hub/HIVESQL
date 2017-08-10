Create  table service_security.dis_time(
order_id            bigint             COMMENT '订单ID',
driver_id           bigint              COMMENT'司机ID ',
passenger_id        bigint              COMMENT'乘客ID',
normal_distance     double              COMMENT'正常行驶距离',
start_dest_distance double              COMMENT'预估路面距离',
distance_rate       double              COMMENT'实际/预估',
real_time           double              COMMENT'实际时间',
forecast_time       double              COMMENT'预估时间',
time_rate           double              COMMENT'实际/预估 '
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/dis_time'

  ================================
  #!/bin/bash
spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;
INSERT INTO TABLE service_security.dis_time
PARTITION(year,month,day)
select
order_id,
driver_id,
passenger_id,
normal_distance,
start_dest_distance/1000 as start_dest_distance,
round(normal_distance/cast(start_dest_distance/1000.0 as double),4)as distance_rate,
(case when finish_time ='0000-00-00 00:00:00'  then 0  else
cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)end) as real_time,
forecast_time,
(case when finish_time ='0000-00-00 00:00:00'  then 0  else
round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/forecast_time,4)
end) as time_rate,
'$years' as year,
'$months' as month,
'$days' as day
from gulfstream_dw.dw_v_order_base --订单基础表
where concat_ws('-',year,month,day)='$start' and begin_charge_time !='0000-00-00 00:00:00'
and start_dest_distance!=0 and normal_distance>5 and
 (a_birth_time  between concat_ws(' ','$start','00:00:00') and concat_ws(' ','$start','$time1')or
 a_birth_time between concat_ws(' ','$start','$time2') and concat_ws(' ','$start','23:59:59'))
distribute by (year,month,day);"

"
Create  table service_security.sex_driver(
order_id  bigint    COMMENT '司机ID',
cnt    double      COMMENT'性骚扰司机被重复投诉性骚扰次数'
)
Location
'hdfs://mycluster-tj/user/common_plat_security/data/service_security/sex_driver'

insert into table service_security.sex_driver
select
driver_id,
count(*) -1 as cnt
from
(
select driver_id from major where driver_id !=0 and type=0 and
path like '%性骚扰%'and path not like '%顺风车%'  and path not like '%交通事故%' and path not like '%失联%'
and path not like '%延误行程%' and path not like '%出租车%' and product_id in (3,4,7) and order_status in(5,7,11,12)
and order_id is not null and length(cast(order_id as string))>=6
)a group by driver_id;