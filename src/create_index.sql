create index dlc_test
on table service_security.xi_an(driver_id)
as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
with Deferred rebuild
idxproperties('creator'='william')
in table test_t
comment'Test index';

