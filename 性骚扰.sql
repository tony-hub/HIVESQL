create table service_security.sexual_harassment_driver(
driver_id STRING COMMENT'性骚扰司机ID',
product_id int COMMENT'产品线ID'
)
partitioned by(
`year` string,
`month` string,
`day` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/sexual_harassment_driver'


select c.driver_id,c.product_id
from
(select distinct id,order_id ,dict_id,complaint_result
from pdw.g_service_worksheet --电话投诉工单表
where concat_ws('-',year,month,day)='2017-07-01' and complaint_result=1 
and valid=1 and order_id!=0
)a
join
(   --查询有“投诉”字段的工单
select distinct id,path_name
from pbs_dw.ods_g_service_dict --客服问题分类字典表
where concat_ws('-',year,month,day)='2017-07-01' and path_name like '%投诉%'
and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' and 
path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
)b
on (a.dict_id=b.id)
join
(
select distinct order_id,driver_id,product_id
from gulfstream_dw.dw_v_order_base
where concat_ws('-',year,month,day)='2017-07-01' and product_id in (1,2,3,4,6,7) 
and order_status in(5,7,11,12) and length(cast(order_id as string))>=6
)c
on (a.order_id=c.order_id)
union all
(select driver_id from xxx where concat_ws('-',year,month,day)='$start1'
)d






select distinct id,path_name
from pbs_dw.ods_g_service_dict --客服问题分类字典表
where concat_ws('-',year,month,day)='2017-07-01' and path_name like '%性骚扰%'
and (path_name like '%专车%' or path_name like '%快车%');


and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' 
and path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
and path_name not like '%小巴%' and path_name not like '%企业%' and path_name not like '%Uber%'
==========性骚扰=================
insert overwrite table  service_security.sexual_harassment_driver
partition(year,month,day)
select distinct driver_id,
product_id,
'2017' as year,
'06' as month,
'28'as day
from service_security.major_com_y where 
(path like "%性骚扰%")  and path not like '%顺风车%'
and path not like '%交通事故%' and path not like '%失联%' and path not like '%延误行程%' 
and path not like '%出租车%' and product_id in (1,2,6,3,4,7)
and content  not like '%司机描述%'
and (order_id is not null) and length(cast(order_id as string))>=6;
=========重大投诉=====
select count(distinct driver_id) from service_security.major_com where concat_ws('-',year,month,day)<='2017-06-15' and concat_ws('-',year,month,day)>='2015-09-01' and 
(path like "%人伤%" or path like "%威胁安全%" or path like "%性骚扰%" or path like "%盗窃%" or 
path like  "%抢劫%"  or path like "%砸车%" or path like "%绑架%" or path like "%犯罪行为%" or 
path like "%公安局%")  and path not like '%顺风车%' and path not like '%交通事故%' 
and path not like '%失联%' and path not like '%延误行程%' and path not like '%出租车%' 
and (product_id=3 or product_id=4 or product_id=7) and (order_id is not null) 
and length(cast(order_id as string))>=6;

select distinct d.driver_id from
(
select 
c.driver_id
from
(select distinct id,order_id, dict_id,complaint_result
from pdw.g_service_worksheet --电话投诉工单表
where concat_ws('-',year,month,day)='2017-06-19' and complaint_result=1 
and valid=1 and order_id!=0
)a
join
(   --查询有“投诉”字段的工单
select distinct id,path_name
from pbs_dw.ods_g_service_dict --客服问题分类字典表
where concat_ws('-',year,month,day)='2017-06-19' and path_name like '%投诉%'
and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' and 
path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
)b
on (a.dict_id=b.id)
join
(
select distinct order_id,driver_id
from gulfstream_dw.dw_v_order_base
where concat_ws('-',year,month,day)='2017-06-19' and product_id in (3,4,7) 
and order_status in(5,7,11,12) 
)c
on (a.order_id=c.order_id)
union all
select driver_id from service_security.sexual_harassment_driver 
where concat_ws('-',year,month,day)='2017-06-18'
)d



select c.driver_id
from
(
select distinct order_id,driver_id
from gulfstream_dw.dw_v_order_base
where concat_ws('-',year,month,day)='2017-07-11' and product_id in (3,4,7) 
)c
join
(select distinct id,order_id,dict_id,complaint_result
from pdw.g_service_worksheet --电话投诉工单表
where concat_ws('-',year,month,day)>='2017-07-11' and order_id!=0
)a 
on (a.order_id=c.order_id) 
join
(   --查询有“投诉”字段的工单
select distinct id,path_name
from pbs_dw.ods_g_service_dict --客服问题分类字典表
where concat_ws('-',year,month,day)='2017-07-' and path_name like '%投诉%'
and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' and 
path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
)b
on (a.dict_id=b.id)


select * from 
(select distinct id,order_id,dict_id,complaint_result
from pdw.g_service_worksheet --电话投诉工单表
where concat_ws('-',year,month,day)>='2017-07-10' and complaint_result=1 
and valid=1 and order_id!=0
)a 
join
(   --查询有“投诉”字段的工单
select distinct id,path_name
from pbs_dw.ods_g_service_dict --客服问题分类字典表
where concat_ws('-',year,month,day)='2017-07-10' and path_name like '%投诉%'
and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' and 
path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
)b
on (a.dict_id=b.id)
join(
select distinct order_id,driver_id
from gulfstream_dw.dw_v_order_base
where concat_ws('-',year,month,day)='2017-07-10' and product_id in (1,2,3,4,6,7) 
)c
on a.order_id=c.order_id
;

select count(*) ,count(gender),count(age) from gulfstream_dw.dw_v_driver_base
where concat_ws('-',year,month,day)='2017-07-10'


curl -d "caller=\"defensor\"&driverId=$1&mode={\"needDriverBiz\":1,\"needDriverBasic\":1}&productId=2" "http://100.69.238.59:8000/cplat/uranusapi/driver/getDriver"


select 
distinct d.driver_id,
d.product_id,d.contents
from
(select c.driver_id,c.product_id,b.contents
from
(
select distinct order_id,driver_id,product_id
from gulfstream_dw.dw_v_order_base
where concat_ws('-',year,month,day)='2017-07-10' and product_id in (1,2,3,4,6,7) and order_status in(5,7,11,12)
)c
join
(
select distinct id,order_id,dict_id,complaint_result,content
from pdw.g_service_worksheet where concat_ws('-',year,month,day)>='2017-07-10' and order_id!=0
)a 
on (a.order_id=c.order_id) 
join
(
select distinct id,path_name,contents from pbs_dw.ods_g_service_dict
where concat_ws('-',year,month,day)='2017-07-10' and path_name like '%投诉%'
and path_name like '%性骚扰%'and path_name not like '%顺风车%'  and path_name not like '%交通事故%' and 
path_name not like '%失联%' and path_name not like '%延误行程%' and path_name not like '%出租车%'
)b
)d

select * from gulfstream_ods.g_order_create
where  concat_ws('-',year,month,day)='2016-08-01'  and param['bubble_id'] is not null and param['oid'] is not null

select a.content from 
(select driver_id,content from major_com_y)a
join (select driver_id from sexual_harassment_driver where concat_ws('-',year,month,day)='2017-06-28')b
on a.driver_id=b.driver_id;