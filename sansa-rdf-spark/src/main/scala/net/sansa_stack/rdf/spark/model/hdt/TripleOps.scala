package net.sansa_stack.rdf.spark.model.hdt

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


class TripleOps {
  var tripleFactTable : DataFrame = _
  var objectDF : DataFrame = _
  var predicateDF : DataFrame = _
  var subjectDF : DataFrame = _
  private val spark : SparkSession = SparkSession.builder().getOrCreate()

  /**
    * Function returns the Schema of Indexed Triple Fact table.
    * @return StructType
    */
  def tripleSchema : StructType = {
    StructType(
      Seq( StructField(name = TripleOps.SUBJECT_COL, dataType = StringType, nullable = false),
        StructField(name = TripleOps.OBJECT_COL, dataType = StringType, nullable = false),
        StructField(name = TripleOps.PREDICATE_COL, dataType = StringType, nullable = false)))
  }

  /**
    * Function returns the Schema of Dictionary Dataframe.
    * @return Schema of Dictionary
     */
  def dictionarySchema : StructType = {
    StructType(
      Seq( StructField(name = TripleOps.NAME_COL, dataType = StringType, nullable = false),
        StructField(name = TripleOps.INDEX_COL, dataType = LongType, nullable = false)))
  }

    /**
    * Function converts RDD[graph.Triple] to DataFrame [Subject,Object,Predicate] by extracting SOP  value from each record
    * @param triple: Input raw RDD[graph.Triple]
    * @return Returns DataFrame [Subject,Object,Predicate]
      */
  def convertRDDGraphToDF(triple : RDD[graph.Triple]) : DataFrame = {
    spark.createDataFrame(triple.map(t => Row(t.getSubject.toString, t. getObject.toString(), t.getPredicate.toString())), tripleSchema)
  }


   /**
    * Registered input Subject, Object and Predicate Dataframe as Spark in-memory Views
    * @param subjectDictDF Dictionary Dataframe of Subject
    * @param objectDictDF Dictionary Dataframe of Object
    * @param predicateDict Dictionary Dataframe of Predicate
    */
  def registerDictionariesAsView(subjectDictDF : DataFrame, objectDictDF : DataFrame, predicateDict : DataFrame) : Unit = {
    subjectDictDF.createOrReplaceTempView(TripleOps.SUBJECT_TABLE)
    objectDictDF.createOrReplaceTempView(TripleOps.OBJECT_TABLE)
    predicateDict.createOrReplaceTempView(TripleOps.PREDICATE_TABLE)
  }


  /**
    * Return Dataframe of Index + Subject by retrieving the unique subjects from RDD[Triple] and zip it with undex
    * @param triples RDD[Triple] conversion of input file
    * @return DataFrame Subject dictionary of [index,subject]
    */
  def getDistinctSubjectDictDF(triples : RDD[graph.Triple]) : DataFrame = {
    spark.createDataFrame(triples.map(_.getSubject.toString()).distinct().zipWithIndex().map(t => Row(t._1, t._2)), dictionarySchema).cache()
  }

  /**
    * Return Dataframe of Index + Predicate by retrieving the unique predicate from RDD[Triple] and zip it with undex
    * @param triples RDD[Triple] conversion of input file
    * @return DataFrame Predicate dictionary of [index,Prediate]
    */
  def getDistinctPredicateDictDF(triples : RDD[graph.Triple]) : DataFrame = {
    spark.createDataFrame(triples.map(_.getPredicate.toString()).distinct().zipWithIndex().map(t => Row(t._1, t._2)), dictionarySchema).cache()
  }

  /**
    * Return Dataframe of Index + Object by retrieving the unique objects from RDD[Triple] and zip it with undex
    * @param triples RDD[Triple] conversion of input file
    * @return DataFrame Object dictionary of [index , object]
    */
  def getDistinctObjectDictDF(triples : RDD[graph.Triple]) : DataFrame = {
    spark.createDataFrame( triples.map(_.getObject.toString()).distinct().zipWithIndex().map(t => Row(t._1, t._2)), dictionarySchema).cache()
  }


  /**
    * Reads the input rdf file and returns spark RDD[Triple]
    * @param input Input RDF file path
    * @return returns RDD[Tripe]
    */
  def readRDFFromFile( input : String) : RDD[graph.Triple] = {
    val triples : RDD[graph.Triple] = spark.rdf(Lang.NTRIPLES)(input)
    triples
  }


  /**
    * This is key function of TripleOps that read RDF file and create Dictionaries and Index Table and register them as Spark In memory Table
    * @param input Input RDF File Path [Either One of the input is require]
    * @param compressedDir Input compressed-directory Path to read compressed data directly [Either One of the input is require]
    * @param registerAsTable If true, it register all the DF as Spark table
    * @return Returns the Tuple4 [IndexDataFrame,SubjectDictDataFrame,ObjectDictDataFrame,PredicateDictDataFrame]
    */
  def createOrReadDataSet(input : String, compressedDir : String, registerAsTable : Boolean = true) : (DataFrame, DataFrame, DataFrame, DataFrame) = {
    if (input != null && !input.isEmpty) {
      val rddGraph : RDD[graph.Triple] = readRDFFromFile(input)

      val triplesDF = convertRDDGraphToDF(rddGraph)
      triplesDF.cache()

      objectDF = getDistinctObjectDictDF(rddGraph)

      predicateDF = getDistinctPredicateDictDF(rddGraph)

      subjectDF = getDistinctSubjectDictDF(rddGraph)

      registerDictionariesAsView(subjectDF, objectDF, predicateDF)

      triplesDF.createOrReplaceTempView(TripleOps.TRIPLE_RAW_TABLE)

      // Creating Fact table from Subject,Predicate and Object index. Fact table contains unique ID of Subject/Object/Predicate
      tripleFactTable = spark.sqlContext.sql(s"select ${TripleOps.SUBJECT_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.SUBJECT_COL}, ${TripleOps.PREDICATE_TABLE}.${TripleOps.INDEX_COL} as " +
        s"${TripleOps.PREDICATE_COL}, ${TripleOps.OBJECT_TABLE}.${TripleOps.INDEX_COL} as ${TripleOps.OBJECT_COL} from ${TripleOps.TRIPLE_RAW_TABLE} " +
        s"join ${TripleOps.SUBJECT_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.SUBJECT_COL} " +
        s"=${TripleOps.SUBJECT_TABLE}.${TripleOps.NAME_COL} " +
        s"join ${TripleOps.OBJECT_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.OBJECT_COL}=${TripleOps.OBJECT_TABLE}.${TripleOps.NAME_COL} " +
        s"join ${TripleOps.PREDICATE_TABLE} on ${TripleOps.TRIPLE_RAW_TABLE}.${TripleOps.PREDICATE_COL}=${TripleOps.PREDICATE_TABLE}.${TripleOps.NAME_COL}")

      if (registerAsTable) {
        tripleFactTable.createOrReplaceTempView(TripleOps.TRIPLE_TABLE)
      }
    }
    else if ( compressedDir != null && !compressedDir.isEmpty) {

      tripleFactTable = spark.read.schema(tripleSchema).csv(compressedDir + TripleOps.TRIPLE_DIR)
      subjectDF = spark.read.schema(dictionarySchema).csv(compressedDir + TripleOps.SUBJECT_DIR)
      objectDF = spark.read.schema(dictionarySchema).csv(compressedDir + TripleOps.OBJECT_DIR)
      predicateDF = spark.read.schema(dictionarySchema).csv(compressedDir + TripleOps.PREDICATE_DIR)
      if(registerAsTable)
      {
        registerDictionariesAsView(subjectDF, objectDF, predicateDF)
        tripleFactTable.createOrReplaceTempView(TripleOps.TRIPLE_TABLE)
      }

    }
    else {
      throw  new IllegalArgumentException("Input File or Compressed Directory is not provided. Please provide any one input.")
    }
    (tripleFactTable, subjectDF, objectDF, predicateDF)
  }


  /**
    * Function saves the Index and Dictionaries Dataframes into given location
    * @param outputDir Path to be written
    * @param mode SaveMode of Write
    */
  def saveAsCSV(outputDir : String, mode : SaveMode) : Unit =
  {
    tripleFactTable.write.mode(mode).csv(outputDir + TripleOps.TRIPLE_DIR)
    subjectDF.write.mode(mode).csv(outputDir + TripleOps.SUBJECT_DIR)
    objectDF.write.mode(mode).csv(outputDir + TripleOps.OBJECT_DIR)
    predicateDF.write.mode(mode).csv(outputDir + TripleOps.PREDICATE_DIR)
  }

}



object TripleOps {
  // Constants Output directory names
  val TRIPLE_DIR = "/triples"
  val OBJECT_DIR = "/object"
  val PREDICATE_DIR = "/predicate"
  val SUBJECT_DIR = "/subject"

  // Spark SQL Temp view names
  val OBJECT_TABLE = "object"
  val SUBJECT_TABLE = "subject"
  val PREDICATE_TABLE = "predicate"
  val TRIPLE_RAW_TABLE = "tripleRaw"
  val TRIPLE_TABLE = "triple_fact"

  val INDEX_COL = "index"
  val NAME_COL = "name"

  val SUBJECT_COL = "s"
  val OBJECT_COL = "o"
  val PREDICATE_COL = "p"
}
