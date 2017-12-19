//read crime_csv to hive
create external table crime_csv_alexliu(
  ID STRING,
  Case_Number STRING,
  Block STRING,
  Primary_Type STRING,
  Description STRING,
  Location_Description STRING,
  Arrest BOOLEAN,
  Community_Area INT,
  Month STRING,
  Year STRING,
  Day String
  )
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/inputs/alexliu0809/crime';

//check result
select * from crime_csv_alexliu limit 3;