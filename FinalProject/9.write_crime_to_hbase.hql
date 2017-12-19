//First create hbase table
// /usr/bin/hbase  shell
// create 'crime_hbase_alexliu','crime'

//Serving Layer, Stored in HBase

create external table crime_hbase_alexliu (
  Community_name_with_time string,
  Homecide_count int,
  Assault_count int,
  Robbery_count int,
  Battery_count int,
  Theft_count int,
  Burglary_count int,
  Prostitution_count int,
  Gambling_count int,
  Narcotics_count int,
  Kidnapping_count int,
  Other_crime_count int) 
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,crime:Homecide_count,crime:Assault_count,crime:Robbery_count,crime:Battery_count,crime:Theft_count,crime:Burglary_count,crime:Prostitution_count,crime:Gambling_count,crime:Narcotics_count,crime:Kidnapping_count,crime:Other_Crime_count')
TBLPROPERTIES ('hbase.table.name' = 'crime_hbase_alexliu');

insert overwrite table crime_hbase_alexliu
  select concat(Community_Name,'-',Year,'-',Month),
  Homecide_count, Assault_count,
  Robbery_count, Battery_count,
  Theft_count, Burglary_count,
  Prostitution_count, Gambling_count,
  Narcotics_count, Kidnapping_count,
  Other_Crime_count
  from crime_by_date_and_community_alexliu;

//check
select * from crime_hbase_alexliu limit 3;