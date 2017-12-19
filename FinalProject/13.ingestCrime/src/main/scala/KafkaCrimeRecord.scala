import scala.reflect.runtime.universe._


case class KafkaCrimeRecord(
    primary_type: String,
    community_area: String, 
    year: String, 
    month: String)