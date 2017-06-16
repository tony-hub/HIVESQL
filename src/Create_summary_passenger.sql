Create table service_security.passenger_order_info(
pas_id BIGINT COMMENT'乘客ID',
passenger_complaint_orders STRING COMMENT'快专车乘客投诉司机数',
driver_complaint_orders STRING COMMENT '快专车司机投诉乘客数',
total_cancel_orders BIGINT COMMENT'乘客取消订单总数'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION   'hdfs://mycluster-tj/user/common_plat_security/data/service_security/passenger_order_info';
alter table driver_order_info set
LOCATION   'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_order_info_yes';