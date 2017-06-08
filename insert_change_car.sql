select
im_tbl.optype,
im_tbl.order_id,
im_tbl.role,
im_tbl.prod,
im_tbl.uid,
im_tbl.peer_nick,
im_tbl.peer_uid,
im_tbl.contents,
year,
month,
day
from
(
select
param['optype'] as optype ,dlc_decode(param['oids']) as order_id,
param['role'] as role,param['prod'] as prod,param['uid'] as uid,
param['peer_nick'] as peer_nick,param['peer_uid'] as peer_uid,
param['contents']as contents From beatles_ods.imbroker
where param['prod'] in('258','260') and param['optype']='sendmsg'
and param['oids'] is not null and concat_ws('-',year,month,day)='2017-05-19'
)im_tbl
join complaint_info_com
on complaint_info_com.order_id=im_tbl.order_id

select orderid,concat_ws(':',role,contents) as test from change_car_info where orderid='12725139187' group by orderid;

select orderid,(case when role=2 then cast(concat_ws('2:',collect_set(contents)) as string)else cast(concat_ws('1:',collect_set(contents)) as string)end) as test from change_car_info where orderid='12725139187' group by orderid,test;


Create table service_security.im_rsc(
ORDERID STRING COMMENT'订单id',
ORDER_STATUS int COMMENT '订单状态',
PATH STRING COMMENT'投诉内容分类路径',
CONTENT STRING COMMENT '问题描述',
COMPLAINT_RESULT tinyint COMMENT'投诉结果',
CONTENTS STRING COMMENT'聊天内容 '
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/im_info'

select a.orderid,concat_ws(';',a.contents) as contents from (
select orderid,concat_ws(':',order_status,contents)as contents from im_info_normal  group by orderid,order_status,contents)a group by a.orderid;

select
driver.driver_id,
driver.one_star_orders,
driver.two_star_orders,
driver.three_star_orders,
driver.four_star_orders,
driver.five_star_orders
from
(
select driver_id, sum(one_star_orders) as one_star_orders,sum(two_star_orders) as two_star_orders,
sum(three_star_orders) as three_star_orders,sum(four_star_orders) as four_star_orders,
sum(five_star_orders) as five_star_orders
from  gulfstream_dw.dw_m_driver_order where concat_ws('-',year,month,day)<='2015-09-07'
group by driver_id
)driver
join(
select driver_id from service_security.major_complaint_info_com
where concat_ws('-',year,month,day)='$start'
)cic
on cic.driver_id=driver.driver_id
