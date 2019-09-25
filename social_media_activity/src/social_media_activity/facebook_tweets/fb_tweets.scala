package social_media_activity.facebook_tweets
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._


object fb_tweets {
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("Social_Media").enableHiveSupport().getOrCreate
  val hc = new HiveContext(sc)
  
  def create_tbl (TABLE_NAME :String): Unit = {
  hc.sql("create table if not exists aravindr.social_media_activity (id  string,facbook_appid  string,facbook_appkey  string,facebook_feed  string,facbook_emotions  string,twitter_tweets  string,google_place_id  string,system_mac_id  string,longitude  string,latitude  string,date_posted  string,country  string,city string) partitioned by (data_dt string) row format delimited fields terminated by '^' stored as textfile")  
  }
  
  def init_load (load_type: String,data_dt : String,table_name : String){
    
    import spark.implicits._
    
    val fb_data= spark.table("aravindr.facebook_feeds").
    filter($"data_dt"==="2019-06-01").select ($"id",$"appid",$"appkey",$"facebook_feed",$"emotions")
    val twit_data = spark.table("aravindr.twitter_tweets").filter($"data_dt"==="2019-06-01").select($"id", $"tweets", $"google_place_id", $"system_mac_id", $"longitude", $"latitude", $"date_posted", $"country", $"city")
    val cus_Data = spark.table("aravindr.customer_data").filter($"data_dt"==="2019-06-01").select($"userid", $"name", $"city", $"email", $"gender")
    val final_data = fb_data.as("fb_feed").join(twit_data.as("twii"),col("fb_feed.id") === col("twii.id") , "inner").drop($"twii.id")
    /*val finall = final_data.as("fb_tweets").join("cus_Data.Userid"), col("fb_tweets.id") === col("cus_Data"),"left outer")
    */
    final_data.coalesce(10).createOrReplaceTempView("social_media_acti")
  
    
    hc.sql("Insert overwrite table aravindr.social_media_activity partition (data_dt = '2019-06-02' ) select * from social_media_acti")
  }
  
  def delta_load (load_type: String,data_dt : String,table_name : String){
  
  }
  
  
  def decision_tree (load_type: String, data_dt :String,table_name: String) = load_type.toUpperCase()  match {
    
    case "INITIAL" => create_tbl(table_name)
                      init_load(load_type,data_dt,table_name)
    case "DELTA" => delta_load(load_type,data_dt,table_name)
    case _ => throw new IllegalArgumentException("Invalid load_type passed")
  }
  
  def main (args : Array[String])
  {
    var load_type = args(0).toString().toLowerCase()
    var data_dt = args(1).toString()
    var table_name=args(2).toString()
    
    decision_tree(load_type,data_dt,table_name)
  }
  
  }