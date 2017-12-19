# MPCS53013 BigData Final Project
Final Project

## Student Name:
Alex Enze Liu (alexliu0809@uchicago.edu)</br>

## Topic:
Chicago Crime Data Count For Specific Date.

## Data Source:
Chicago Crime Data and Chicago Community Code Data
### Crime Data:
From 2014-2017, contains crime type, crime time, block, community code, description, etc...Only useful information are subtracted from the raw data. The project is also streaming realtime crime data from chicago crime data API portal.

### Community Code Data:
A mapping from community code to community name so that it is more readable.

## Running Steps:
### Batch And Serving Layer
* 1. Get data ready and preprocess data
* 2. Ingest crime data using ingestCrime.sh
* 3. Create crime data from csv file using crime_data_csv.hql
* 4. Ingest name data using ingestCommunity.sh 
* 5. Create community data from csv file using community_data.hql
* 6. Join crime and community so that community name is known using join_crime_community.hql
* 7. Group by community and date using crime_by_date_and_community.hql
* 8. Write data to HBase using write_community_to_hbase.hql

### Speed Layer
* 9. Create speed layer tables using create_speed.txt
* 10. Stream Crime data from chicago API using streamCrime.sh and the source code from streamCrime
* 11. Ingest data from kafka and store into speed table in HBase using ingestCrime.sh and the source code from ingestCrime. (Note that the crime API is updating slowly so it is possible that you read the same data and write to HBase many times.)

### Depoly UI
* 12. Deploy UI on webserver.

### Update And Rotate
* 13. Update data using UpdateTable.txt