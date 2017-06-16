#!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive  --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.driver_info_all
PARTITION(year,month,day)
select
driver.driver_id,
driver.one_star_orders,
driver.two_star_orders,
driver.three_star_orders,
driver.four_star_orders,
driver.five_star_orders,
'$years' as year,
'$months' as month,
'$days' as day
from
(
select driver_id from gulfstream_dw.dw_v_driver_base where concat_ws('-',year,month,day)='$start'
) driver_base
left outer join
(
select driver_id, sum(one_star_orders) as one_star_orders,sum(two_star_orders) as two_star_orders,
sum(three_star_orders) as three_star_orders,sum(four_star_orders) as four_star_orders,
sum(five_star_orders) as five_star_orders
from  gulfstream_dw.dw_m_driver_order where concat_ws('-',year,month,day)<'$start'
group by driver_id
)driver
on driver_base.driver_id=driver.driver_id
distribute by(year,month,day);"
#!/bin/bash
start=$1
start1=`date -d "1 day ago $start" +%Y-%m-%d`
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapredfiles = true;

INSERT INTO TABLE service_security.passenger_order_info_normal
PARTITION(year,month,day)
select
b.pas_id,
a.passenger_complaint_orders,
a.driver_complaint_orders,
a.cancel_before_count,
a.cancel_after_count,
'$years'as year,
'$months' as month,
'$days' as day
from
(
select pas_id from gulfstream_dw.dw_v_passenger_base where concat_ws('-',year,month,day)='$start'
)b
left outer join
(
select pas_id,sum(passenger_complaint_orders) as passenger_complaint_orders,
sum(driver_complaint_orders) as driver_complaint_orders ,
sum(cancel_before_count) as cancel_before_count,
sum(cancel_after_count) as cancel_after_count from gulfstream_dw.dw_m_passenger_order
where concat_ws('-',year,month,day)<'$start' group by pas_id
)a
on a.pas_id=b.pas_id
distribute by(year,month,day);"


from
(select * from poi)e
select  e.order_id limit 100;
> select
    > order_id,
    > driver_id,
    > passenger_id,
    > round(normal_distance/cast(start_dest_distance/1000.0 as double),4)as distance_rate,
    > round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/forecast_time,4) as time_rate
    > from major_complaint_info_com
    > where concat_ws('-',year,month,day)='2017-05-24' and finish_time!='0000-00-00 00:00:00' and begin_charge_time !='0000-00-00 00:00:00'
    > and start_dest_distance!=0 ;
 selec driver_city_name from gulfstream_dw.dw_v_driver_base where driver_city_name like '%县%' limit 10

 Create table service_security.d_applist(
driver_id BIGINT COMMENT'ID',
name string COMMENT'身份证号码归属地'
)
ROW format delimited
 fields terminated by ' '
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/d_identify';
   load data local inpath '/home/common_plat_security/dlc/city.txt' into table d_identify;"
   select eventid,uid  ,passengerid,app_name,os_version,brand from omega.native_daily limit 100

   select uid, regexp_replace(attrs['installed_apps'],'\n', ' ') as installed_apps from omega.native_daily
   where concat(year,month,day)='20170520' and appid in (5,6) and eventid='OMGODAT' and
   attrs['installed_apps'] is not null   and uid=565115708768944;
   and passengerid is not null and passengerid!=0

   565461165681193
   driver_id  passenger_id
565115708768944 3162985144209
567950153944281 3061115066249

select order_status,  count(*) from gulfstream_dw.dw_v_order_base where concat_ws('-',year,month,day)='2017-05-24'
and product_id in(1,2) and order_status in(5,7,11,12)  group by order_status;

   select uid,count(*) from omega.native_daily
   where concat(year,month,day)='20170319' and appid in (5,6) and eventid='OMGODAT' and
   attrs['installed_apps'] is not null  group by uid;