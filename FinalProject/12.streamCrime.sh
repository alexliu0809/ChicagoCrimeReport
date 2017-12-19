# cp uber jar to cloud
gcloud compute scp uber-streamCrime-0.0.1-SNAPSHOT.jar alexliu0809@mpcs530132017test-hgc1-0-20170924181411:~/streamCrime/

# run uber jar that reads from api and writes to kafka
# Also use nohup
java -cp uber-streamCrime-0.0.1-SNAPSHOT.jar edu.uchciago.mpcs53013.streamCrime.RealtimeCrime mpcs530132017test-hgc1-0-20170924181411.c.mpcs53013-2017.internal:6667

# Check 
/usr/hdp/currt/kafka-broker/bin/kafka-console-consumer.sh --zookeeper mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal:2181,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal:2181,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal:2181  --topic crime_alexliu --from-beginning
