package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.hadoop.mapred.JobConf

object Main extends App {

  println("==================================================")
  println("|        Distributed Vandalism Detection           |")
  println("==================================================")

  // Spark configuration and Spark context :
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("XMLProcess")
  val sc = new SparkContext(sparkConf)

  // Streaming records:
  val jobConf = new JobConf()
  jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
  jobConf.set("stream.recordreader.begin", "<revision>") // start Tag
  jobConf.set("stream.recordreader.end", "</revision>") // End Tag
  org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, "hdfs://localhost:9000/mydata/Sample3.xml") // input path from Hadoop

  // read data and save in RDD as block
  val wikiData = sc.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
  println("Hello1" + " " + wikiData.count)
  val RevisionTagewikidata = wikiData.map { case (x, y) => (x.toString()) }
  println("Hello2" + " " + RevisionTagewikidata.count)
  //RevisionTagewikidata.take(7).foreach(println)

  val algorithm = VandalismDetection
  import algorithm._

  // ABend the revision in one line string
  val RevisioninOneString = RevisionTagewikidata.map(line => abendRevision(line))
  println("Hello13" + " " + RevisioninOneString.count)
  RevisioninOneString.foreach(println)

  //    // Function for build the Property - value
  val RevisionMap = RevisioninOneString.map(line => build_Revision_Map(line))
  println("Hello14" + " " + RevisionMap.count)
  RevisionMap.foreach(println)

  val AbendedArray = RevisionMap.map(line => AbendArrayListinOneString(line))
  println("Hello15" + " " + AbendedArray.count)
  //AbendedArray.foreach(println)

  val FlatedRDDarraylists = AbendedArray.flatMap(line => line.split("\\s+"))
  println("Hello16" + " " + FlatedRDDarraylists.count)
  //    FlatedRDDarraylists.foreach(println)

  val finalRDD = FlatedRDDarraylists.filter(x => !x.isEmpty)
  println("Hello17" + " " + finalRDD.count)
  //    finalRDD.foreach(println)

  //----------------------------DF------------------------------------------
  //  Create SQLContext Object:
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  //Create an Encoded Schema in a String Format:
  val schemaString = "RevisionID Property Value"

  //Generate schema:
  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

  //Apply Transformation for Reading Data from Text File
  val rowRDD = finalRDD.map(_.split("::")).map(e ⇒ Row(e(0), e(1), e(2)))

  //Apply RowRDD in Row Data based on Schema:
  val RevisionDF = sqlContext.createDataFrame(rowRDD, schema)

  //Store DataFrame Data into Table
  RevisionDF.createOrReplaceTempView("PropertyValue")

  //Select Query on DataFrame
  val dfr = sqlContext.sql("SELECT * FROM PropertyValue")
  dfr.show()

  sc.stop();

}