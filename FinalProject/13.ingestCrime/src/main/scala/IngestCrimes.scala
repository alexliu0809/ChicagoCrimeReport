import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

//
//spark-submit --master local[2] --class IngestCrimes /home/mpcs53013/eclipse-workspace/ingestCrime/target/uber-ingestCrime-0.0.1-SNAPSHOT.jar localhost:9092
//

object IngestCrimes {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  
  // Use the following two lines if you are building for the cluster 
  hbaseConf.set("hbase.zookeeper.quorum","mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal")
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  
  // Use the following line if you are building for the VM
  //hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val speed_table = hbaseConnection.getTable(TableName.valueOf("speed_crime_alexliu")) //speed layer crime
  val community_table = hbaseConnection.getTable(TableName.valueOf("community_hbase_alexliu"))//hbase community table

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamCrimes")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set[String]("crime_alexliu") //kafka topic
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaCrimeRecord]))

    // How to read from an HBase table
    val batchStats = kfrs.map(kfr => {
      
      //First, get corresponding community info
      val community_result = community_table.get(new Get(Bytes.toBytes(kfr.community_area)))

      val community_name = Bytes.toString(community_result.getValue(Bytes.toBytes("community"), Bytes.toBytes("Name")))
      println("community_name")
      println(community_name)
      
      //Read speed layer data
      val speed_crime_result = speed_table.get(new Get(Bytes.toBytes(community_name+'-'+kfr.year+'-'+kfr.month)))
	  
	  //If there is no result
	  val old_crime_data = if(speed_crime_result == null || speed_crime_result.getRow() == null) CrimeData(community_name+'-'+kfr.year+'-'+kfr.month)
	  else CrimeData(community_name+'-'+kfr.year+'-'+kfr.month,
	    Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Homecide_count"))).toInt, //no to string
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Assault_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Robbery_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Battery_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Theft_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Burglary_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Prostitution_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Gambling_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Narcotics_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Kidnapping_count"))).toInt,
        Bytes.toString(speed_crime_result.getValue(Bytes.toBytes("crime"), Bytes.toBytes("Other_crime_count"))).toInt)
	  
	  println("old_crime")
	  println(old_crime_data)
	  
	  var new_crime_data = old_crime_data
	  
	  val HOMICIDE = kfr.primary_type.contains("HOMICIDE")
	  val ASSAULT = kfr.primary_type.contains("ASSAULT")
	  val ROBBERY = kfr.primary_type.contains("ROBBERY")
	  val BATTERY = kfr.primary_type.contains("BATTERY")
	  val THEFT = kfr.primary_type.contains("THEFT")
	  val BURGLARY = kfr.primary_type.contains("BURGLARY")
	  val PROSTITUTION = kfr.primary_type.contains("PROSTITUTION")
	  val GAMBLING = kfr.primary_type.contains("GAMBLING")
	  val NARCOTICS = kfr.primary_type.contains("NARCOTICS")
	  val KIDNAPPING = kfr.primary_type.contains("KIDNAPPING")
	  val OTHER = !HOMICIDE && !ASSAULT && !ROBBERY && !BATTERY && !THEFT && !BURGLARY && !PROSTITUTION && !GAMBLING && !NARCOTICS && !KIDNAPPING
	  
	  if (HOMICIDE) new_crime_data.Homecide_count += 1
	  if (ASSAULT) new_crime_data.Assault_count += 1
	  if (ROBBERY) new_crime_data.Robbery_count += 1
	  if (BATTERY) new_crime_data.Battery_count += 1
	  if (THEFT) new_crime_data.Theft_count += 1
	  if (BURGLARY) new_crime_data.Burglary_count += 1
	  if (PROSTITUTION) new_crime_data.Prostitution_count += 1
	  if (GAMBLING) new_crime_data.Gambling_count += 1
	  if (NARCOTICS) new_crime_data.Narcotics_count += 1
	  if (KIDNAPPING) new_crime_data.Kidnapping_count += 1
	  if (OTHER) new_crime_data.Other_crime_count += 1
	  
	  println("new_crime")
	  println(new_crime_data)
	  
	  //Finally, write the updated info of this data back to hbase
      var theput = new Put(Bytes.toBytes(community_name+'-'+kfr.year+'-'+kfr.month))
      //put it into hbase. Save as string. Parse it by yourself when you read it.
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Homecide_count"), Bytes.toBytes(new_crime_data.Homecide_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Assault_count"), Bytes.toBytes(new_crime_data.Assault_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Robbery_count"), Bytes.toBytes(new_crime_data.Robbery_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Battery_count"), Bytes.toBytes(new_crime_data.Battery_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Theft_count"), Bytes.toBytes(new_crime_data.Theft_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Burglary_count"), Bytes.toBytes(new_crime_data.Burglary_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Prostitution_count"), Bytes.toBytes(new_crime_data.Prostitution_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Gambling_count"), Bytes.toBytes(new_crime_data.Gambling_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Narcotics_count"), Bytes.toBytes(new_crime_data.Narcotics_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Kidnapping_count"), Bytes.toBytes(new_crime_data.Kidnapping_count.toString))
      theput.add(Bytes.toBytes("crime"), Bytes.toBytes("Other_crime_count"), Bytes.toBytes(new_crime_data.Other_crime_count.toString))
      
      speed_table.put(theput)
      
    })
    
    // Your homework is to get a speed layer working
    //
    // In addition to reading from HBase, you will likely want to
    // either insert into HBase or increment existing values in HBase
    // You can do these just like the above, but instead of using a
    // Get object, you use a Put or Increment objects as documented here:
    //
    // http://javadox.com/org.apache.hbase/hbase-client/1.1.2/org/apache/hadoop/hbase/client/Put.html
    // http://javadox.com/org.apache.hbase/hbase-client/1.1.2/org/apache/hadoop/hbase/client/Increment.html
    //
    // One nuisance is that you can only increment by a Long, so
    // I have rebuilt our tables with Longs instead of Doubles
    
    // Print the current status. See if it is updated.
    batchStats.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}