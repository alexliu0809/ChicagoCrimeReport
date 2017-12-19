package edu.uchciago.mpcs53013.streamCrime;

//
//spark-submit --master local[2] --class RealtimeCrime /home/mpcs53013/eclipse- workspace/SSWC/target/uber-SSWC-0.0.1- SNAPSHOT.jar localhost:9092
//
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.PasswordAuthentication;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
/*
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
{  
   ":@computed_region_43wa_7qmu":"45",
   ":@computed_region_6mkv_f3dw":"4299",
   ":@computed_region_awaf_s7ux":"4",
   ":@computed_region_bdys_3d7i":"562",
   ":@computed_region_d3ds_rm58":"67",
   ":@computed_region_d9mm_jgwp":"25",
   ":@computed_region_rpca_8um6":"5",
   ":@computed_region_vrxf_vc4k":"26",
   "arrest":true,
   "beat":"1524",
   "block":"054XX W RICE ST",
   "case_number":"JA526401",
   "community_area":"25",
   "date":"2017-11-26T23:53:00.000",
   "description":"SIMPLE",
   "district":"015",
   "domestic":false,
   "fbi_code":"08B",
   "id":"11160347",
   "iucr":"0460",
   "latitude":"41.895796081",
   "location":{  
      "type":"Point",
      "coordinates":[  
         -87.761401563,
         41.895796081
      ]
   },
   "location_description":"ALLEY",
   "longitude":"-87.761401563",
   "primary_type":"BATTERY",
   "updated_on":"2017-12-03T15:49:45.000",
   "ward":"37",
   "x_coordinate":"1139962",
   "y_coordinate":"1905137",
   "year":"2017"
}
 */
// Inspired by http://stackoverflow.com/questions/14458450/what-to-use-instead-of-org-jboss-resteasy-client-clientrequest
public class RealtimeCrime {
	static class Task extends TimerTask {
		private Client client;
		Random generator = new Random();
		// We are just going to get a random sampling of flights from a few airlines
		// Getting all flights would be much more expensive!
		private String api_token = "ee8mpb4C9xkshoCNYV85JeHeg";
		public CrimeResponse[] getWeatherResponse() {
			Invocation.Builder bldr
			  = client.target("https://data.cityofchicago.org/resource/6zsd-86xi.json?$$app_token="+api_token).request("application/json");
			try {
				return bldr.get(CrimeResponse[].class);
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
			return null;  // Sometimes the web service fails due to network problems. Just let it try again
		}

		// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		Properties props = new Properties();
		String TOPIC = "crime_alexliu"; //set your topic
		KafkaProducer<String, String> producer;
		
		public Task() {
			client = ClientBuilder.newClient();
			// enable POJO mapping using Jackson - see
			// https://jersey.java.net/documentation/latest/user-guide.html#json.jackson
			client.register(JacksonFeature.class); 
			props.put("bootstrap.servers", bootstrapServers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
		}

		@Override
		public void run() {
			//Get crime response
			CrimeResponse[] responses = getWeatherResponse();
			
			//Only Stream the first one.
			CrimeResponse response = responses[0];
			
			if(response == null || response.getCommunity_area() == null || response.getPrimary_type() == null || response.getDate() == null)
				return;
			
			//System.out.println(response.toString());
			
			ObjectMapper mapper = new ObjectMapper();

//			for(detail arrival : response.getFleetArrivedResult().getArrivals()) {
				ProducerRecord<String, String> data;
				try {
					CrimeRecord w = new CrimeRecord(
							response.getPrimary_type(),
							response.getCommunity_area(), 
							response.getDate());
					
					
					data = new ProducerRecord<String, String>
					(TOPIC, 
					 mapper.writeValueAsString(w));
					producer.send(data); 
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//			}
		
		}

	}
	
	//Dont User 9092 on cluster
	static String bootstrapServers = new String("localhost:9092");

	public static void main(String[] args) {
		if(args.length > 0)  // This lets us run on the cluster with a different kafka
			bootstrapServers = args[0];
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new Task(), 0, 300*1000);
	}
}

