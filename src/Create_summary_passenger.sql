Create table service_security.passenger_order_info_y(
pas_id BIGINT COMMENT'乘客ID',
passenger_complaint_orders STRING COMMENT'快专车乘客投诉司机数',
driver_complaint_orders STRING COMMENT '快专车司机投诉乘客数',
cancel_before_count BIGINT COMMENT'乘客抢单前取消订单总数',
cancel_after_count BIGINT COMMENT'乘客抢单后取消订单总数'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
location 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/passenger_order_info_y'
