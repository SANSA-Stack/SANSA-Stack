package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.hadoop.mapred.JobConf
import java.util.Scanner
import net.sansa_stack.ml.spark.outliers.vandalismdetection.VandalismDetection._

object Main extends App {

  println("==================================================")
  println("|        Distributed Vandalism Detection           |")
  println("==================================================")

  // Spark configuration and Spark context :
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("XMLProcess")
    val sc = new SparkContext(sparkConf)

    println("For RDF XML enter 1 and for Normal XML enter 2")

    val in: Scanner = new Scanner(System.in);
    val num: Integer = in.nextInt();

    //RDF XML file :*********************************************************************************************************
    if (num == 1) {

      println("RDF XML .........!!!!!!")
      // Streaming records:RDFXML file :
      val jobConf_Record = new JobConf()
      val jobConf_Prefixes = new JobConf()

      jobConf_Record.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
      jobConf_Record.set("stream.recordreader.begin", "<rdf:Description") // start Tag
      jobConf_Record.set("stream.recordreader.end", "</rdf:Description>") // End Tag

      jobConf_Prefixes.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
      jobConf_Prefixes.set("stream.recordreader.begin", "<rdf:RDF") // start Tag
      jobConf_Prefixes.set("stream.recordreader.end", ">") // End Tag

      org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Record, "hdfs://localhost:9000/mydata/Germany.rdf") // input path from Hadoop
      org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf_Prefixes, "hdfs://localhost:9000/mydata/Germany.rdf") // input path from Hadoop

      //------------ RDF XML Record
      // read data and save in RDD as block- RDFXML Record
      val RDFXML_Dataset_Record = sc.hadoopRDD(jobConf_Record, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
      println("HelloRecords" + " " + RDFXML_Dataset_Record.count)

      // Convert the block- RDFXML Record to String DataType
      val RDFXML_Dataset_Record_AsstringBlock = RDFXML_Dataset_Record.map { case (x, y) => (x.toString()) }
      println("HelloRecords" + " " + RDFXML_Dataset_Record_AsstringBlock.count)
      RDFXML_Dataset_Record_AsstringBlock.foreach(println)

      //-------------RDF XML Prefixes
      // read data and save in RDD as block- RDFXML Prefixes
      val RDFXML_Dataset_Prefixes = sc.hadoopRDD(jobConf_Prefixes, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
      println("HelloPrefixes" + " " + RDFXML_Dataset_Prefixes.count)
      RDFXML_Dataset_Prefixes.foreach(println)

      // Convert the block- RDFXML Prefixes to String DataType
      val RDFXML_Dataset_AsstringPrefixes = RDFXML_Dataset_Prefixes.map { case (x, y) => (x.toString()) }
      println("HelloPrefixes" + " " + RDFXML_Dataset_AsstringPrefixes.count)
      RDFXML_Dataset_AsstringPrefixes.foreach(println)

      val pref = RDFXML_Dataset_AsstringPrefixes.reduce((a, b) => a + "\n" + b)

      println("Hello100000000000" + pref)

      val Triples = RDFXML_Dataset_Record_AsstringBlock.map(record => RecordParse(record, pref))
      println("HelloRecords" + " " + Triples.count)
      Triples.foreach(println)

      val finalRDD_RDF = Triples.filter(x => !x.isEmpty)
      println("Hello17" + " " + finalRDD_RDF.count)

      val TR = finalRDD_RDF.map(line => arrayListTOstring(line))
      println("HelloRecords" + " " + TR.count)
      TR.foreach(println)
      //----------------------------DF for RDF XML ------------------------------------------
      //  Create SQLContext Object:
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      import org.apache.spark.sql.functions._ // for UDF

      //Create an Encoded Schema in a String Format:
      val schemaString = "Subject Predicate Object"

      //Generate schema:
      val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

      //Apply Transformation for Reading Data from Text File
      val rowRDD = TR.map(_.split(" ")).map(e ⇒ Row(e(0), e(1), e(2)))

      //Apply RowRDD in Row Data based on Schema:
      val RDFTRIPLE = sqlContext.createDataFrame(rowRDD, schema)

      //Store DataFrame Data into Table
      RDFTRIPLE.registerTempTable("SPO")

      //Select Query on DataFrame
      val dfr = sqlContext.sql("SELECT * FROM SPO")
      dfr.show()

      // NOrmal XML Example WikiData: ***************************************************************************************************
    } else if (num == 2) {
      println("Normal XML .........!!!!!!")

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
      RevisionTagewikidata.take(7).foreach(println)
      
	  

	 	  
      // ABend the revision in one line string
      val RevisioninOneString = RevisionTagewikidata.map(line => abendRevision(line))
      println("Hello13" + " " + RevisioninOneString.count)
      RevisioninOneString.foreach(println)

      // Function for build the Property - value
      val RevisionMap = RevisioninOneString.map(line => build_Revision_Map(line))
      println("Hello14" + " " + RevisionMap.count)
      //    RevisionMap.foreach(println)

      val AbendedArray = RevisionMap.map(line => AbendArrayListinOneString(line))
      println("Hello15" + " " + AbendedArray.count)
      //AbendedArray.foreach(println)

      val FlatedRDDarraylists = AbendedArray.flatMap(line => line.split("\\s+"))
      println("Hello16" + " " + FlatedRDDarraylists.count)
      //    FlatedRDDarraylists.foreach(println)

      val finalRDD = FlatedRDDarraylists.filter(x => !x.isEmpty)
      println("Hello17" + " " + finalRDD.count)
      //        finalRDD.foreach(println)

      // filter for NA values, IDItem, Comment because  I will not apply on any thing
      val finalRDD_NA = finalRDD.filter(line => !line.contains("::NA"))
      println("Hello18" + " " + finalRDD_NA.count)

      val finalRDD_NoCooment = finalRDD_NA.filter(line => !line.contains("comment::"))
      println("Hello19" + " " + finalRDD_NoCooment.count)
      //               finalRDD_NoCooment.foreach(println)
      val finalRDD_NOItemID = finalRDD_NoCooment.filter(line => !line.contains("id_Item"))
      println("Hello110" + " " + finalRDD_NOItemID.count)
//      finalRDD_NOItemID.foreach(println)

      val RDDWithVectore = finalRDD_NOItemID.map(line => build_VectorFeature(line))
      println("Hello" + " " + RDDWithVectore.count)
//      RDDWithVectore.foreach(println)
      
      //----------------------------DF for Normal XML ------------------------------------------

         //  Create SQLContext Object:
            val sqlContext = new org.apache.spark.sql.SQLContext(sc)
            import sqlContext.implicits._
            import org.apache.spark.sql.functions._ // for UDF
            //Create an Encoded Schema in a String Format:
            val schemaString = "RevisionID Property Value Vectors"
      
            //Generate schema:
            val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
      
            //Apply Transformation for Reading Data from Text File
            val rowRDD = RDDWithVectore.map(_.split("::")).map(e ⇒ Row(e(0), e(1), e(2), e(3)))
      
            //Apply RowRDD in Row Data based on Schema:
            val RevisionDF = sqlContext.createDataFrame(rowRDD, schema)
      
            //Store DataFrame Data into Table
            RevisionDF.registerTempTable("PropertyValue")
      
            //Select Query on DataFrame
            val dfr = sqlContext.sql("SELECT * FROM PropertyValue")
            dfr.show()
      




     

    } else {

      print("Your Option is Wrong")

    }

    //*******************************************
    sc.stop();

}