create table  service_security.userinfo_auth(
	uid bigint COMMENT 'ID',
	gender int COMMENT '性别，男1，女2',
	age int COMMENT '年龄，有效取值范围(0，100)',
	auth_state  int  COMMENT'0:未认证 1:认证中 2:认证通过 3:认证失败 4:认证通过 5:认证通过 6:认证通过 7:认证通过',
	age_level   int  COMMENT '90后=5, 80后=4, 70后=3, 60后=2, 50后=1'
)
PARTITIONED BY(
`year` STRING,
`month` STRING,
`day` STRING)
LOCATION 'hdfs://mycluster-tj/user/common_plat_security/data/service_security/userinfo_auth'