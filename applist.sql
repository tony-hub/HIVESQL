Create table service_security.driver_applist_all(
passenger_id BIGINT COMMENT'ID',
Installed_app string COMMENT'安装的APP列表',
brand STRING COMMENT'手机品牌'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_applist_all';

防止查询的数据没数据
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
3.如果是spark方式运行还需要设置下面2个参数:
--conf spark.sql.hive.metastorePartitionPruning=true
--conf spark.sql.hive.convertMetastoreParquet=false
  =================================================================================
  #!/bin/bash
  start=$1
 years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive  --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"

set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.auto.convert.join=true;
set hive.exec.max.dynamic.partitions.pernode=100000;

insert into table service_security.driver_applist
PARTITION(year,month,day)
select
a.drivcer_id,
b.installed_apps,
b.brand,
'$years' as year,
'$months' as month,
'$days' as day
from
(
select drivcer_id from major_complaint_info_com where concat_ws('-',year,month,day)='$start'
and  driver_id !=0
)a
join
(
select distinct uid, regexp_replace(attrs['installed_apps'],'\n', ' ') as installed_apps, brand from omega.native_daily
where concat_ws('-',year,month,day)='$start' and appid in(5,6) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and uid !=0
)b
on a.drivcer_id=b.uid
distribute by(year,month,day);"


==========================================passenger================="
#!/bin/bash
start=$1
years=`date -d "$start" +%Y`
months=`date -d "$start" +%m`
days=`date -d "$start" +%d`

hive --hiveconf mapreduce.job.queuename=pingtaijishubu-gonggongpingtai.commonapi -e"

set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.auto.convert.join=true;
set hive.exec.max.dynamic.partitions.pernode=100000;

insert into table service_security.p_applist
PARTITION(year,month,day)
select
a.passenger_id,
b.installed_apps,
b.brand
'$years' as year,
'$months' as month,
'$days' as day
from
(
select passenger_id from major_complaint_info_com where concat_ws('-',year,month,day)='2017-5-20'
and passenger_id!=0
)a
join
(
select passengerid, regexp_replace(attrs['installed_apps'],'\n', ' \t') as installed_apps,brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-20' and appid in(1,2) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and passengerid !=0
)b
on a.passenger_id=b.passengerid
distribute by(year,month,day);"


select
a.passenger_id,
b.installed_apps,
b.brand
from
(
select passenger_id from major_complaint_info_com where concat_ws('-',year,month,day)='2017-02-28'
and passenger_id!=0
)a
join
(
select passengerid, regexp_replace(attrs['installed_apps'],'\n', ' \t') as installed_apps,brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-20' and appid in(1,2) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and passengerid !=0
)b
on a.passenger_id=b.passengerid



set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
select
a.driver_id,
b.installed_apps
from
(
select driver_id,year,month,day from major_complaint_info_com where driver_id !=0 and type=0 and
path like '%性骚扰%'and path not like '%顺风车%'  and path not like '%交通事故%' and path not like '%失联%'
and path not like '%延误行程%' and path not like '%出租车%' and
product_id in (3,4,7) and order_status in(5,7,11,12)
and order_id is not null and length(cast(order_id as string))>=6
)a
join
(
select count(*) from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-24' and appid in(1,2) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and uid !=0 and attrs['installed_apps'] like '%%'
)b
on a.driver_id=b.uid;

select
*
from(
select driver_id,year,month,day from major_complaint_info_com where driver_id !=0 and type=0 and
path like '%性骚扰%'and path not like '%顺风车%'  and path not like '%交通事故%' and path not like '%失联%'
and path not like '%延误行程%' and path not like '%出租车%' and
product_id in (3,4,7) and order_status in(5,7,11,12)
and order_id is not null and length(cast(order_id as string))>=6
)a
join
(
select driver_id,Installed_app from driver_applist
where Installed_app like '%情趣%'
)b
on a.driver_id=b.driver_id
