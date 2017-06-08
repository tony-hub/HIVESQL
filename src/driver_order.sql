Create table service_security.driver_order_info(
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
LOCATION
  'hdfs://mycluster-tj/user/common_plat_security/data/service_security/driver_order_info'
