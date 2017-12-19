//read community to hive
create external table community_area_alexliu(
  Community_Area INT,
  Name STRING,
  Population INT,
  Income DOUBLE,
  Requests DOUBLE,
  Latinos DOUBLE,
  Blacks DOUBLE,
  White DOUBLE,
  Asian DOUBLE,
  Other DOUBLE
  )
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,"
)
STORED AS TEXTFILE
  location '/inputs/alexliu0809/community';

//check result
 select * from community_area_alexliu limit 5;
