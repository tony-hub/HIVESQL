Create table service_security.driver_order_info_y(
driver_id BIGINT COMMENT'司机ID',
one_star_orders bigint COMMENT'一星订单数',
two_star_orders bigint COMMENT'二星订单数',
three_star_orders bigint COMMENT'三星订单数',
four_star_orders bigint COMMENT'四星订单数',
five_star_orders bigint COMMENT'五星订单数'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
ROW FORMAT SERDE   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
location 'hdfs://mycluster-tj/user/common_plat_security/warehouse/service_security.db/driver_order_info_y'
