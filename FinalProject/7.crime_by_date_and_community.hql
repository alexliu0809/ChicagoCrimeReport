//Count Crime, group by community name and time(year+month)
//Create Batch View

create table crime_by_date_and_community_alexliu(
  Community_Name STRING,
  Year STRING,
  Month STRING,
  
  Homecide_count INT,
  Assault_count INT,
  Robbery_count INT,
  Battery_count INT,
  Theft_count INT,
  Burglary_count INT,
  Prostitution_count INT,
  Gambling_count INT,
  Narcotics_count INT,
  Kidnapping_count INT,
  Other_Crime_count INT
  
  ) stored as orc;

insert overwrite table crime_by_date_and_community_alexliu

  select Community_Name,Year,Month,

  count(if(instr(Primary_Type, 'HOMICIDE')>0, 1, null)),
  count(if(instr(Primary_Type, 'ASSAULT')>0, 1, null)), 
  count(if(instr(Primary_Type, 'ROBBERY')>0, 1, null)), 
  count(if(instr(Primary_Type, 'BATTERY')>0, 1, null)), 
  count(if(instr(Primary_Type, 'THEFT')>0, 1, null)), 
  count(if(instr(Primary_Type, 'BURGLARY')>0, 1, null)), 
  count(if(instr(Primary_Type, 'PROSTITUTION')>0, 1, null)), 
  count(if(instr(Primary_Type, 'GAMBLING')>0, 1, null)), 
  count(if(instr(Primary_Type, 'NARCOTICS')>0, 1, null)), 
  count(if(instr(Primary_Type, 'KIDNAPPING')>0, 1, null)), 
  count(if(instr(Primary_Type, 'HOMICIDE')<=0 and instr(Primary_Type, 'ASSAULT')<=0 and instr(Primary_Type, 'ROBBERY')<=0
    and instr(Primary_Type, 'BATTERY')<=0 and instr(Primary_Type, 'THEFT')<=0 and instr(Primary_Type, 'BURGLARY')<=0 and instr(Primary_Type, 'ASSAULT')<=0
    and instr(Primary_Type, 'PROSTITUTION')<=0 and instr(Primary_Type, 'GAMBLING')<=0 and instr(Primary_Type, 'NARCOTICS')<=0 
    and instr(Primary_Type, 'KIDNAPPING')<=0 and instr(Primary_Type, 'HOMICIDE')<=0, 1, null))

  from crime_and_community_name_alexliu
  group by Community_Name, Year, Month;