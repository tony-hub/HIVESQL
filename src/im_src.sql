Create table service_security.im_info_normal(
ORDERID STRING COMMENT'订单id',
order_status INT COMMENT'订单状态',
CONTENTS STRING COMMENT'聊天内容 ',
TIME_STAMP STRING COMMENT '发送时间',
UID STRING COMMENT '发送方的uid',
PEER_UID STRING COMMENT'接收方id'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/im_info_normal'

#!/bin/bash
start=$1
spark-sql --driver-memory 8g --conf spark.driver.maxResultSize=12g --queue pingtaijishubu-gonggongpingtai.commonapi -e"
set hive.exec.dynamic.partition.mode=nonstrict;
source /home/common_plat_security/dlc/hiveset.hql;
set hive.auto.convert.join=true;
add jar /home/common_plat_security/dlc/decodeId.jar;
create temporary function dlc_decode as 'com.company.decodeId.decode';

INSERT INTO TABLE SERVICE_SECURITY.im_info_normal
PARTITION (year,month,day)
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
and param['oids'] is not null and  concat_ws('-',year,month,day)='$start'
group by param['oids'],param['role'],param['contents'],param['timestamp']
)im_tbl
join
(
select order_status,order_id,year,month,day from
normal_info_bak where concat_ws('-',year,month,day)='$start'
)cic
on cic.order_id=im_tbl.order_id
distribute by(year,month,day);"

