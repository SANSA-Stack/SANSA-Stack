package net.sansa_stack.rdf.spark.model.hdt

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang


/*
* @author Abakar Bouba
*/

/*
  --input
  /home/abakar/IdeaProjects/RDF-Data-Compression-N-Triples-in-SANSA-Stack-using-Scala-and-Spark/src/main/resources/Input-Data/Small/dbpedia/sample.nt
  --compressed-input-dir
  /home/abakar/IdeaProjects/RDF-Data-Compression-N-Triples-in-SANSA-Stack-using-Scala-and-Spark/src/main/resources/compressedQuery/dbpedia
  --output-dir
  /home/abakar/IdeaProjects/RDF-Data-Compression-N-Triples-in-SANSA-Stack-using-Scala-and-Spark/src/main/resources/dbpedia_compressed
*/

class TripleOps {

  val logger = LoggerFactory.getLogger(TripleOps.getClass)
  var tripleFactTable:DataFrame=null;
  var objectDF:DataFrame=null;
  var predicateDF:DataFrame=null;
  var subjectDF:DataFrame=null;



  val spark=SparkSession.builder().getOrCreate()

  /**
    * Function returns the Schema of Indexed Triple Fact table.
    * @return StructType
    */
  def tripleSchema:StructType = {
    StructType(
      Seq( StructField(name = TripleOps.SUBJECT_COL, dataType = StringType, nullable = false),
        StructField(name = TripleOps.OBJECT_COL, dataType = StringType, nullable = false),
        StructField(name = TripleOps.PREDICATE_COL, dataType = StringType, nullable = false)));
  }

  /**
    * Function returns the Schema of Dictionary Dataframe.
    * @return Schema of Dictionary
    */
  def dictionarySchema={
    StructType(
      Seq( StructField(name = TripleOps.NAME_COL, dataType = StringType, nullable = false),
        StructField(name = TripleOps.INDEX_COL, dataType = LongType, nullable = false)));
  }

  /**
    * Function converts RDD[graph.Triple] to DataFrame [Subject,Object,Predicate] by extracting SOP  value from each record
    * @param triple: Input raw RDD[graph.Triple]
    * @return Returns DataFrame [Subject,Object,Predicate]
    */
  def convertRDDGraphToDF(triple:RDD[graph.Triple])={
    logger.info("Converting RDD[Graph] to DataFrame[Row].")
    spark.createDataFrame(triple.map(t=> Row(t.getSubject.toString,t.getObject.toString(),t.getPredicate.toString())),tripleSchema)
  }


  /**
    * Registered input Subject, Object and Predicate Dataframe as Spark in-memory Views
    * @param subjectDictDF
    * @param objectDictDF
    * @param predicateDict
    */
  def registerDictionariesAsView(subjectDictDF:DataFrame,objectDictDF:DataFrame,predicateDict:DataFrame): Unit ={
    logger.info("Registering Dictionart DataFrame as SQL Views...")
    logger.info(s"Registering Subject Dictionary as ${TripleOps.SUBJECT_TABLE} view")
    subjectDictDF.createOrReplaceTempView(TripleOps.SUBJECT_TABLE)
    logger.info(s"Registering Object Dictionary as ${TripleOps.OBJECT_TABLE} view")
    objectDictDF.createOrReplaceTempView(TripleOps.OBJECT_TABLE)
    logger.info(s"Registering Predicate Dictionary as ${TripleOps.PREDICATE_TABLE} view" )
    predicateDict.createOrReplaceTempView(TripleOps.PREDICATE_TABLE)
  }


  /**
    * Return Dataframe of Index + Subject by retrieving the unique subjects from RDD[Triple] and zip it with undex
    * @param triples
    * @return DataFrame [Row[Index,Subject]]
    */
  def getDistinctSubjectDictDF(triples:RDD[graph.Triple])={
    spark.createDataFrame(triples.map(_.getSubject.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),dictionarySchema).cache();
  }

  /**
    * Return Dataframe of Index + Predicate by retrieving the unique predicate from RDD[Triple] and zip it with undex
    * @param triples
    * @return DataFrame [Row[Index,Predicate]]
    */
  def getDistinctPredicateDictDF(triples:RDD[graph.Triple])={
    spark.createDataFrame(triples.map(_.getPredicate.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),dictionarySchema).cache();
  }

  /**
    * Return Dataframe of Index + Object by retrieving the unique objects from RDD[Triple] and zip it with undex
    * @param triples
    * @return DataFrame [Row[Index,Object]]
    */
  def getDistinctObjectDictDF(triples:RDD[graph.Triple])={
    spark.createDataFrame(triples.map(_.getObject.toString()).distinct().zipWithIndex().map(t=> Row(t._1,t._2)),dictionarySchema).cache();
  }


  /**
    * Reads the input rdf file and returns spark RDD[Triple]
    * @param input Input RDF file path
    * @return returns RDD[Tripe]
    */
  def readRDFFromFile(input:String)  ={
    val triples: RDD[graph.Triple] = spark.rdf(Lang.NTRIPLES)(input)
    triples
  }


  /**
    * This is key function of TripleOps that read RDF file and create Dictionaries and Index Table and register them as Spark In memory Table
    * @param input Input RDF File Path [Either One of the input is require]
    * @param compressedDir Input compressed-directory Path to read compressed data directly [Either One of the input is require]
    * @param registerAsTable If true, it register all the DF as Spark table
    * @return Returns the Tuple4 [IndexDataFrame,SubjectDictDataFrame,ObjectDictDataFrame,PredicateDictDataFrame]
    */
  def createOrReadDataSet(input:String,compressedDir:String,registerAsTable:Boolean=true) ={
    if(input != null && !input.isEmpty) {
      val rddGraph: RDD[graph.Triple] =readRDFFromFile(input)
      logger.info(s"Rdd[Graph] is created. ${rddGraph.count()}")

      val triplesDF=convertRDDGraphToDF(rddGraph)
      triplesDF.printSchema()
      triplesDF.cache()

      objectDF=getDistinctObjectDictDF(rddGraph)
      objectDF.printSchema()

      predicateDF=getDistinctPredicateDictDF(rddGraph)
      predicateDF.printSchema()

      subjectDF=getDistinctSubjectDictDF(rddGraph)
      subjectDF.printSchema()

      logger.info(subjectDF.count() +"  "+ objectDF.count()+"  "+ predicateDF.count())
      registerDictionariesAsView(subjectDF,objectDF,predicateDF);

      logger.info(s"Registering Triples DataFrame as ${TripleOps.TRIPLE_RAW_TABLE} view" )
      triplesDF.createOrReplaceTempView(TripleOps.TRIPLE_RAW_TABLE)
      spark.catalog.listTables()
      logger.info(s"select ${TripleOps.SUBJECT_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.SUBJECT_COL}, ${TripleOps.PREDICATE_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.PREDICATE_COL},${TripleOps.OBJECT_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.OBJECT_COL} from ${TripleOps.TRIPLE_RAW_TABLE} join ${TripleOps.SUBJECT_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.SUBJECT_COL}=${TripleOps.SUBJECT_TABLE}.${TripleOps.NAME_COL} " +
        s"join ${TripleOps.OBJECT_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.OBJECT_COL}=${TripleOps.OBJECT_TABLE}.${TripleOps.NAME_COL} " +
        s"join ${TripleOps.PREDICATE_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.PREDICATE_COL}=${TripleOps.PREDICATE_TABLE}.${TripleOps.NAME_COL}")

      //Creating Fact table from Subject,Predicate and Object index. Fact table contains unique ID of Subject/Object/Predicate
      tripleFactTable= spark.sqlContext.sql(s"select ${TripleOps.SUBJECT_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.SUBJECT_COL}, ${TripleOps.PREDICATE_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.PREDICATE_COL},${TripleOps.OBJECT_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.OBJECT_COL} from ${TripleOps.TRIPLE_RAW_TABLE} join ${TripleOps.SUBJECT_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.SUBJECT_COL}=${TripleOps.SUBJECT_TABLE}.${TripleOps.NAME_COL} " +
        s"join ${TripleOps.OBJECT_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.OBJECT_COL}=${TripleOps.OBJECT_TABLE}.${TripleOps.NAME_COL} " +
        s"join ${TripleOps.PREDICATE_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.PREDICATE_COL}=${TripleOps.PREDICATE_TABLE}.${TripleOps.NAME_COL}");

      logger.info(s"Registering Triples Fact Tables as ${TripleOps.TRIPLE_TABLE} " )
      if(registerAsTable)
        tripleFactTable.createOrReplaceTempView(TripleOps.TRIPLE_TABLE)
    }
    else if(compressedDir!=null && !compressedDir.isEmpty){

      tripleFactTable=spark.read.schema(tripleSchema).csv(compressedDir+TripleOps.TRIPLE_DIR)
      subjectDF=spark.read.schema(dictionarySchema).csv(compressedDir+TripleOps.SUBJECT_DIR)
      objectDF=spark.read.schema(dictionarySchema).csv(compressedDir+TripleOps.OBJECT_DIR)
      predicateDF=spark.read.schema(dictionarySchema).csv(compressedDir+TripleOps.PREDICATE_DIR)
      if(registerAsTable)
      {
        registerDictionariesAsView(subjectDF,objectDF,predicateDF)
        tripleFactTable.createOrReplaceTempView(TripleOps.TRIPLE_TABLE)
      }

    }
    else{
      logger.error("Input File or Compressed Directory is not provided. Please provide any one input.")
    }
    (tripleFactTable,subjectDF,objectDF,predicateDF)
  }

  /**
    * Function saves the Index and Dictionaries Dataframes into given location
    * @param outputDir Path to be written
    * @param mode SaveMode of Write
    */
  def saveAsCSV(outputDir:String, mode:SaveMode): Unit =
  {
    logger.info(s"Writing DataSet into ${outputDir}");
    tripleFactTable.write.mode(mode).csv(outputDir + TripleOps.TRIPLE_DIR)
    subjectDF.write.mode(mode).csv(outputDir + TripleOps.SUBJECT_DIR)
    objectDF.write.mode(mode).csv(outputDir + TripleOps.OBJECT_DIR)
    predicateDF.write.mode(mode).csv(outputDir + TripleOps.PREDICATE_DIR)
  }

}



object TripleOps {
  //Constants Output directory names
  val TRIPLE_DIR="/triples"
  val OBJECT_DIR="/object"
  val PREDICATE_DIR="/predicate"
  val SUBJECT_DIR="/subject"

  //Spark table names
  val OBJECT_TABLE="object"
  val SUBJECT_TABLE="subject"
  val PREDICATE_TABLE="predicate"
  val TRIPLE_RAW_TABLE="tripleRaw"
  val TRIPLE_TABLE="triple_fact"

  val INDEX_COL="index"
  val NAME_COL="name"

  val SUBJECT_COL="s"
  val OBJECT_COL="o"
  val PREDICATE_COL="p"
  val logger = LoggerFactory.getLogger(TripleOps.getClass)


  def main(args: Array[String]) {
    val inputRDFFile="/home/abakar/IdeaProjects/RDF-Data-Compression-N-Triples-in-SANSA-Stack-using-Scala-and-Spark/src/main/resources/Input-Data/Small/dbpedia/sample.nt"
    val compressedInputDir=null
    val outputDir="/home/abakar/IdeaProjects/Output"
    //Initialized the spark session
    val spark = SparkSession.builder
      .appName(s"Data Compression")
      .master("local[*]")
      // .master("spark://172.18.160.16:3090")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val tripleOpsObject=new TripleOps;
    tripleOpsObject.createOrReadDataSet(inputRDFFile,compressedInputDir)

    var sTime=System.currentTimeMillis()
    logger.info("Subject Dict Count: "+ tripleOpsObject.subjectDF.count())
    logger.info("Object Dict Count: "+ tripleOpsObject.objectDF.count())
    logger.info("Predicate Dict Count: "+ tripleOpsObject.subjectDF.count())
    logger.info("Index Count: "+ tripleOpsObject.tripleFactTable.count())
    logger.info(s" Count completed in ${System.currentTimeMillis()-sTime} ms.")

    logger.info("**** Starting Write Process...")
    sTime=System.currentTimeMillis()
    tripleOpsObject.saveAsCSV(outputDir,SaveMode.Overwrite)
    logger.info(s" Count completed in ${System.currentTimeMillis()-sTime} ms.")

  }

}
