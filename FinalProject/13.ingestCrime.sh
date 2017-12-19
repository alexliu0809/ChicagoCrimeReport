# Copy the jar to cloud
gcloud compute scp uber-ingestCrime-0.0.1-SNAPSHOT.jar alexliu0809@mpcs530132017test-hgc1-0-20170924181411:~/ingestCrime/

# Reads from kafka and ingest to hbase
# Also use nohup
spark-submit --class IngestCrimes uber-ingestCrime-0.0.1-SNAPSHOT.jar mpcs530132017test-hgc1-0-20170924181411.c.mpcs53013-2017.internal:6667

# check in hbase shell
scan 'speed_crime_alexliu'
