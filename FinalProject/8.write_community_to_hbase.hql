// First create hbase table
// hbase shell
// create 'community_hbase_alexliu','community'

//Serving Layer, Stored in HBase
create external table community_hbase_alexliu (
  Community_Area INT,
  Name STRING)

  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,community:Name')
TBLPROPERTIES ('hbase.table.name' = 'community_hbase_alexliu');

insert overwrite table community_hbase_alexliu
  select Community_Area,
  Name
  from community_area_alexliu;

//check
select * from community_hbase_alexliu limit 3;