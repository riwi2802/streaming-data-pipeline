package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Table}
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.User
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.hadoop.hbase.util.Bytes


/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  val hdfsUrl = "hdfs://hbase02.hourswith.expert:8020"
  val bootstrapServers = "b-2-public.hwe-kafka-cluster.l384po.c8.kafka.us-west-2.amazonaws.com:9196,b-1-public.hwe-kafka-cluster.l384po.c8.kafka.us-west-2.amazonaws.com:9196,b-3-public.hwe-kafka-cluster.l384po.c8.kafka.us-west-2.amazonaws.com:9196"
  val username = "hwe"
  val password = "1904labs"
  val hdfsUsername = "rwilliams" // TODO: set this to your handle

  //Use this for Windows
  val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"



  //case class for objective 2

  case class ReviewObj(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String,
                       product_title: String, product_category: String, star_rating: String, helpful_votes: String,
                       total_votes: String, vine: String, verified_purchase: String, review_headline: String,
                       review_body: String, review_date:String)

  case class EnrichedObj(user : User, reviewObj: ReviewObj)

  case class User(birthdate: String, mail: String, name: String, sex: String, username: String)


  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)
  implicit def bytesToString(bytes: Array[Byte]):String = Bytes.toString(bytes)



  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", "3")
        .appName(jobName)
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val ds = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "1")
        .option("startingOffsets","earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      // TODO: implement logic here
      //split
      def splitTab(str : String): Array[String] = str.split("\t")
      val review = ds.map(row => {
        val reviewRow = splitTab(row)
           ReviewObj(marketplace = reviewRow(0),
          customer_id = reviewRow(1),
          review_id = reviewRow(2),
          product_id = reviewRow(3),
          product_parent = reviewRow(4),
          product_title = reviewRow(5),
          product_category = reviewRow(6),
          star_rating = reviewRow(7),
          helpful_votes = reviewRow(8),
          total_votes = reviewRow(9),
          vine = reviewRow(10),
          verified_purchase = reviewRow(11),
          review_headline = reviewRow(12),
          review_body = reviewRow(13),
          review_date = reviewRow(14)
        )
      })


      //Hbase get request for every review message

      val enrichedReview = review.mapPartitions(partition => {

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "hbase02.hourswith.expert:2181")
        val connection = ConnectionFactory.createConnection(conf)
        val table: Table  = connection.getTable(TableName.valueOf("rwilliams:users"))

        val iter = partition.map(review => {

          val get = new Get(review.customer_id).addFamily("f1")
          val result = table.get(get)

          val userValue = User(birthdate = result.getValue("f1", "birthdate"),
            mail = result.getValue("f1", "mail"),
            name = result.getValue("f1", "name"),
            sex = result.getValue("f1", "sex"),
            username = result.getValue("f1", "username")
          )

          (review, Bytes.toString(result.getValue("f1", "customer_id")))

          EnrichedObj(userValue, review)

          //at very end when I return it do a select

        }).toList.iterator

        connection.close()

        iter

      }).select("userValue.*","review.*")





      // Write output to console
      //change result to review below

//      val query = review.writeStream
//        .outputMode(OutputMode.Append())
//        .format("console")
//        .option("truncate", false)
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()




//       Write output to HDFS
      val query = review.writeStream
        .outputMode(OutputMode.Append())
        .format("json")
        .option("path", s"/user/${hdfsUsername}/reviews_json")
        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()
      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def getScramAuthString(username: String, password: String) = {
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }
}
