/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package dataCreator

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * Different settings for the DataSetGenerator
 * TODO: implement reading of settings from config file.
 */
object Settings {
  val sparkContext = loadSparkContext()
  var sqlContext = loadSqlContext()
  
  // ScaleUB (Scale Upper Bound) controls the storage overhead.
  // Set it to 1 (default) to store all possible ExtVP tables. Reduce this value
  // to avoid storage of ExtVP tables having size bigger than ScaleUB * (size of
  // corresponding VP table). In this way, we avoid storage of the biggest 
  // tables, which are most ineffective at the same time, since they are 
  // not able to significantly improve the selectivity of correponding triple 
  // pattern
  var ScaleUB = 1:Float
  
  // database name
  //val baseName = "WatDiv1000M_test"
  // the database directory in HDFS
  var workingDir = ""
  // path to the input RDF file  
  var inputRDFSet = ""
  // path to Parquet file for the Triple Table
  var tripleTable = ""
  // path to the directory for all VP tables 
  var vpDir = ""
  // path to the directory for all ExtVP tables 
  var extVpDir = ""
  
  def loadUserSettings(inFilePath:String, 
                   inFileName:String, 
                   scale:Float) = {
    this.ScaleUB = scale
    this.workingDir = inFilePath
    this.inputRDFSet = inFilePath + inFileName
    this.tripleTable = this.workingDir + "base.parquet" 
    this.vpDir = this.workingDir + "VP/" 
    this.extVpDir = this.workingDir + "ExtVP/" 
  }
  
  /**
   * Create SparkContext.
   * The overview over settings: 
   * http://spark.apache.org/docs/latest/programming-guide.html
   */   
  def loadSparkContext(): SparkContext = {
    
    val conf = new SparkConf().setAppName("DataSetsCreator")
                              .set("spark.executor.memory", "20g")
                              .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                              //.set("spark.sql.autoBroadcastJoinThreshold", "-1")
                              .set("spark.sql.parquet.filterPushdown", "true")
                              .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                              .set("spark.storage.blockManagerSlaveTimeoutMs", "3000000")                              
                              //.set("spark.sql.shuffle.partitions", "200")
                              .set("spark.storage.memoryFraction", "0.5")
    new SparkContext(conf)
  }
  
  /**
   * Create SQLContext.
   */
 
  def loadSqlContext(): SQLContext = {
    val context = new org.apache.spark.sql.SQLContext(sparkContext)
    import context.implicits._
    context    
  }
}

