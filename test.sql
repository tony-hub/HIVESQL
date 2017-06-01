select
(case when cic.driver_id is null then doi.driver_id else cic.driver_id end) as driver_id,

(case when driver.one_star_orders is null then 0 else driver.one_star_orders end )+
(case when doi.one_star_orders is null then 0 else doi.one_star_orders end )as one_star_orders,

(case when driver.two_star_orders is null then 0 else driver.two_star_orders end )+
(case when doi.two_star_orders is null then 0 else doi.two_star_orders end )as two_star_orders,

(case when driver.three_star_orders is null then 0 else driver.three_star_orders end )+
(case when doi.three_star_orders is null then 0 else doi.three_star_orders end)as three_star_orders,

(case when driver.four_star_orders is null then 0 else driver.four_star_orders end)+
(case when doi.four_star_orders is null then 0 else doi.four_star_orders end)as four_star_orders,

(case when driver.five_star_orders is null then 0 else driver.five_star_orders end)+
(case when doi.five_star_orders is null then 0 else doi.five_star_orders end)as five_star_orders,

(case when punish.punish_num is null then 0 else punish.punish_num end )+
(case when doi.forbidden_num is null then 0 else doi.forbidden_num end)as punish_num,
$years,
$months,
$days
from
(
select driver_id, one_star_orders, two_star_orders, three_star_orders, four_star_orders,
five_star_orders
from  gulfstream_dw.dw_m_driver_order where concat_ws('-',year,month,day)='$start'
)driver
join(
select driver_id from service_security.complaint_info_com
where concat_ws('-',year,month,day)='$start'
)cic
on cic.driver_id=driver.driver_id
LEFT OUTER  JOIN
(
select driver_id,punish_num from service_security.driver_punish
where concat_ws('-',year,month,day)='$start'
)punish
on punish.driver_id=cic.driver_id
full outer join
(
select driver_id, one_star_orders, two_star_orders, three_star_orders, four_star_orders,
five_star_orders,forbidden_num
from service_security.driver_order_info where concat_ws('-',year,month,day)=''
)doi
on doi.driver_id=cic.driver_id
distribute by(year,month,day);


create table if not exists service_security.redup_normal_info_bak
like  service_security.normal_info_bak
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/redup_normal_info_bak';


create table if not exists service_security.im_info_normal
like  service_security.im_info
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/im_info_normal';


select
im_tbl.order_id,
cic.order_status,
im_tbl.contents,
im_tbl.uid,
im_tbl.peer_uid
year,
month,
day
from
(
select
dlc_decode(param['oids']) as order_id,param['uid'] as uid,param['peer_uid'] as peer_uid,concat_ws(':',param['role'],param['contents']) as contents,
param['timestamp'] as tms
From beatles_ods.imbroker
where param['prod'] in('258','260') and param['optype']='sendmsg'
and param['oids'] is not null and  concat_ws('-',year,month,day)='2015-09-01'
group by param['oids'],param['role'],param['contents'],param['timestamp']
)im_tbl
join
(
select order_status,order_id,year,month,day from
redup_normal_info_bak where concat_ws('-',year,month,day)='$start'
)cic
on cic.order_id=im_tbl.order_id
distribute by(year,month,day);

select
param['oids'] as order_id,param['uid'] as uid,param['peer_uid'] as peer_uid,concat_ws(':',param['role'],param['contents']) as contents,
param['timestamp'] as tms
From beatles_ods.imbroker
where param['prod'] in('258','260') and param['optype']='sendmsg'
and param['oids'] is not null and  concat_ws('-',year,month,day)='2015-09-01'
group by param['oids'],param['uid'],param['peer_uid'],param['role'],param['contents'],param['timestamp']





select count(*),concat_ws('-',year,month,day) from driver_order_info where concat_ws('-',year,month,day)<='2016-05-01' group by year,month ,day;
