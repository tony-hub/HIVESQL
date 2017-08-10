Create table service_security.driver_applist(
driver_id BIGINT COMMENT'ID',
brand STRING COMMENT'手机品牌',
Installed_app string COMMENT'安装的APP列表',

)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_applist';

Create table service_security.major_order_app(
app STRING COMMENT'安装的APP',
num int COMMENT'app出现的次数',
rate double
)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/major_order_app';

 Create table service_security.major_worksheet_order(
app STRING COMMENT'安装的APP',
num string COMMENT'app出现的次数'
)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/major_worksheet_order';

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
"
select
a.driver_id,
b.installed_apps,
b.brand
from
(
select driver_id from major where concat_ws('-',year,month,day)='2017-05-28'
and  driver_id !=0
)a
join
(
select distinct uid, extract_dlc(cast(attrs['installed_apps'] as string)) as installed_apps, brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-28' and appid in(5,6) and eventid='OMGODAT' and
attrs['installed_apps'] !="" and uid !=0
)b
on a.driver_id=b.uid
limit 10;
distribute by(year,month,day);"

select
a.passenger_id,
b.installed_apps,
b.brand
from
(
select passenger_id from service_security.major where concat_ws('-',year,month,day)='2017-02-28'
and passenger_id!=0
)a
join
(
select passengerid, extract_dlc(cast (attrs['installed_apps'] as string)) as installed_apps ,brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-02-28' and appid in(1,2) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and passengerid !=0
)b
on a.passenger_id=b.passengerid
limit 10



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
select uid ,extract_dlc(cast (attrs['installed_apps'] as string)) as installed_apps,brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-24' and appid in(5,6) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and uid !=0  and uid=566357250483385

select driver_id ,extract_dlc( regexp_replace(installed_app,' ', ' \n')) as installed_apps from driver_applist limit 10;

select passengerid,attrs['installed_apps'] as installed_apps,brand from omega.native_daily
where concat_ws('-',year,month,day)='2017-05-20' and appid in(1,2) and eventid='OMGODAT' and
attrs['installed_apps'] is not null and passengerid !=0 limit 10;

select driver_id, regexp_replace(installed_app,'\n', ', ') as installed_apps, brand
from driver_applist_all

select driver_id,brand,split(col5,',')[0] from driver_applist a lateral  view explode(split(installed_app,' '))  b AS col5;

select c.driver_id,c.brand,concat_ws(',',collect_set(c.app))  from
(
select driver_id,brand,split(col5,',')[0] as app from driver_applist a lateral  view explode(split(installed_app,' '))  b AS col5)c group by c.driver_id,c.brand;
"
insert into table service_security.driverapp
select
l.driver_id,
l.brand,
substring(l.app,4)
from
(
select *,row_number() over (partition by t.driver_id order by t.app desc)  as num
from
(
select d.driver_id,d.brand,concat_ws(',',collect_set(d.app)) as app  from
(
select distinct c.driver_id,c.brand,c.app from
(
select driver_id,brand,split(col5,',')[0] as app
from driver_applist_all a lateral  view explode(split(regexp_replace(installed_app,'\n', ', '),','))  b AS col5
)c
)d group by d.driver_id,d.brand
)t
)l where l.num=1  ;

select driver_id,brand,split(col5,',')[0] as app
from driver_applist_all a lateral  view explode(split(regexp_replace(installed_app,'\n', ', '),','))  b AS col5 limit 10;

select substring(installed_app,3) from driver_app limit 10;

========================================

===============sex=======================
insert into table service_security.sex_order_app
select count(*)
from
(select distinct driver_id from major where  type=0 and (path like "%人伤%" or path like "%威胁安全%" or
 path like "%性骚扰%" or path like "%盗窃%" or path like  "%抢劫%"  or path like "%砸车%" or path like "%绑架%"
 or path like "%犯罪行为%" or path like "%公安局%") and path not like '%顺风车%' and path not like '%交通事故%'
and path not like '%失联%' and path not like '%延误行程%' and path not like '%出租车%' and  product_id in(3 ,4 ,7)
and (order_id is not null) and length(cast(order_id as string))>=6 and order_status in(5,7,11,12)
)a
join
(
select driver_id from service_security.driver_app
)b on a.driver_id=b.driver_id
on
(
select c.driver_id as driver_id,c.app as app,d.cnt  as num from
(
select driver_id,trim(app) as app from service_security.driver_app LATERAL VIEW explode(split(installed_app,','))  installed_app as app
)c
join
(
select trim(app) as app,count(app)  as cnt from service_security.driver_app LATERAL VIEW explode(split(installed_app,','))  installed_app as app group by app
)d
on  c.app=d.app
)b
on a.driver_id=b.driver_id

===================major===============
insert into table service_security.major_order_app
select  a.driver_id,b.app,b.num from
(
select distinct driver_id from major where  type=0 and (path like "%人伤%" or path like "%威胁安全%" or
 path like "%性骚扰%" or path like "%盗窃%" or path like  "%抢劫%"  or path like "%砸车%" or path like "%绑架%"
 or path like "%犯罪行为%" or path like "%公安局%") and path not like '%顺风车%' and path not like '%交通事故%'
and path not like '%失联%' and path not like '%延误行程%' and path not like '%出租车%' and  product_id in(3 ,4 ,7)
and (order_id is not null) and length(cast(order_id as string))>=6 and order_status in(5,7,11,12)
)a
join
(
select c.driver_id as driver_id,c.app as app,d.cnt  as num from
(
select driver_id,trim(app) as app from service_security.driver_app LATERAL VIEW explode(split(installed_app,','))  installed_app as app
)c
join
(
select trim(app) as app,count(app)  as cnt from service_security.driver_app LATERAL VIEW explode(split(installed_app,','))  installed_app as app group by app
)d
on  c.app=d.app
)b
on a.driver_id=b.driver_id;

insert into table service_security.major_order_app
select app ,num, cast(num/13467 as double) as rate
from (
select e.app,count(*) as num from
(
select trim(col5) as app from
(
select a.driver_id,b.installed_app as app from
(
select distinct driver_id from major where  type=0 and (path like "%人伤%" or path like "%威胁安全%" or
 path like "%性骚扰%" or path like "%盗窃%" or path like  "%抢劫%"  or path like "%砸车%" or path like "%绑架%"
 or path like "%犯罪行为%" or path like "%公安局%") and path not like '%顺风车%' and path not like '%交通事故%'
and path not like '%失联%' and path not like '%延误行程%' and path not like '%出租车%' and  product_id in(3 ,4 ,7)
and (order_id is not null) and length(cast(order_id as string))>=6 and order_status in(5,7,11,12)
)a
left outer join driver_app b on a.driver_id=b.driver_id
)c lateral view explode(split(c.app,','))  d AS col5
)e group by e.app order by num desc
)f;



====================================

Create table service_security.major_worksheet_order(
ID bigint COMMENT '工单ID',
order_id bigint COMMENT'订单ID',
driver_id bigint  COMMENT'司机ID',
passenger_id bigint COMMENT '乘客ID',
normal_distance double COMMENT '正常行驶距离',
start_dest_distance double COMMENT '预估路面距离',
distance_rate double COMMENT '实际/预估',
real_time double COMMENT'实际时间',
forecast_time double COMMENT '预估时间',
time_rate double COMMENT'实际/预估'
)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://mycluster-tj/user/common_plat_security/data/service_security/major_worksheet_order';
=========================================
insert into table service_security.major_worksheet_order
select
b.id,
c.order_id,
c.driver_id,
c.passenger_id,
c.normal_distance,
c.start_dest_distance/1000 as start_dest_distance,
round(c.normal_distance/cast(c.start_dest_distance/1000.0 as double),4)as distance_rate,
(case when c.finish_time ='0000-00-00 00:00:00'  then 0  else
cast( (unix_timestamp(c.finish_time)-unix_timestamp(c.begin_charge_time) )/60 as double)end) as real_time,
c.forecast_time,
(case when c.finish_time ='0000-00-00 00:00:00'  then 0  else
round(cast( (unix_timestamp(c.finish_time)-unix_timestamp(c.begin_charge_time) )/60 as double)/c.forecast_time,4)
end) as time_rate
from
(select id from major_worksheet_id)a
join
(select distinct id,order_id  from pdw.g_service_worksheet)b
on  a.id=b.id
join(
select order_id,driver_id,passenger_id,normal_distance,start_dest_distance,forecast_time,begin_charge_time,finish_time
from gulfstream_dw.dw_v_order_base --订单基础表
)c
on  c.order_id=b.order_id;

where order_id in (
select b.order_id from

)

select
order_id,
driver_id,
passenger_id,
round(normal_distance/cast(start_dest_distance/1000.0 as double),4)as distance_rate,
round(cast( (unix_timestamp(finish_time)-unix_timestamp(begin_charge_time) )/60 as double)/forecast_time,4) as time_rate,
'$years' as year,
'$months' as month,
'$days' as day
from gulfstream_dw.dw_v_order_base --订单基础表
where concat_ws('-',year,month,day)='$start' and finish_time!='0000-00-00 00:00:00' and begin_charge_time !='0000-00-00 00:00:00'
and start_dest_distance!=0
distribute by (year,month,day);"




insert into table service_security.n_noraml
select
b.app,
b.num
from
(
select a.app ,a.num from major_order_app a where a.app not in (select app from normal_app)
)b


Create table service_security.n_noraml(
app string,
num int
)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'hdfs://mycluster-tj/user/common_plat_security/data/service_security/n_noraml';



