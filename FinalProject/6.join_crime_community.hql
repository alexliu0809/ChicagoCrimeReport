//combine community name with crime data
create table crime_and_community_name_alexliu (
    ID STRING,
    Case_Number STRING,
    Primary_Type STRING,
    Month STRING,
    Year STRING,
    Day STRING,
    Community_Name STRING,
    Community_Area INT
  ) stored as orc;

insert overwrite table crime_and_community_name_alexliu
  select crime.ID as ID, crime.Case_Number as Case_Number, crime.Primary_Type as Primary_Type, 
  crime.Month as Month, crime.Year as Year, crime.Day as Day, 
  community.Name as Community_Name, community.Community_Area as Community_Area
  from crime_csv_alexliu crime JOIN community_area_alexliu community
  on crime.Community_Area = community.Community_Area;

//check result
select * from crime_and_community_name_alexliu limit 5;
