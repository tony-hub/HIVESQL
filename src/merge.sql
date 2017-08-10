create table SERVICE_SECURITY.sex_complaint_y
(
driver_id bigint  comment'司机ID',
order_id bigint comment'订单ID',
passenger_id bigint comment '乘客ID',
product_id int comment '产品ID',
scene_status int comment '状态',
score double comment'score',
actual_level double comment'actual_level',
level double comment 'level',
cmp_type string comment 'cmp_type',
cmp_type_txt string comment 'cmp_type_txt',
content string comment 'content',
verify_status int comment'verify_status',
cnt int comment 'cnt',
one_star_orders int comment'一星订单数',
two_star_orders int comment'二星订单数',
three_star_orders int comment'三星订单数',
four_star_orders int comment '四星订单数',
five_star_orders int comment'五星订单数'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
location 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/sex_complaint_y'


INSERT OVERWRITE TABLE SERVICE_SECURITY.sex_complaint_y
PARTITION (year,month,day)
select 
a.driver_id,
a.order_id,
a.passenger_id,
a.product_id,
a.scene_status,
a.score,
a.actual_level,
a.level,
a.cmp_type,
a.cmp_type_txt,
a.content,
a.verify_status,
a.cnt,
b.one_star_orders,
b.two_star_orders,
b.three_star_orders,
b.four_star_orders,
b.five_star_orders,
b.year,
b.month,
b.day
from 
(
select driver_id,one_star_orders,two_star_orders,three_star_orders,
four_star_orders,five_star_orders,year,month,day from driver_order_info_y
)b
join
(
select driver_id,order_id,passenger_id,product_id,
scene_status,score,actual_level,level,cmp_type,cmp_type_txt,
content,verify_status,cnt from  sex_complaint
)a
on a.driver_id=b.driver_id
distribute by(year,month,day);
