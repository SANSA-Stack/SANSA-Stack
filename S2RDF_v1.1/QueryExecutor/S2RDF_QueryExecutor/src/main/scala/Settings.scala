/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package queryExecutor

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * Different settings for the Query Executor
 * TODO: implement reading of settings from config file.
 */
object Settings {
  
  // Settings to change
  val testLoop = 1;
  val partNumberExtVP = 50
  val partNumberVP = 200
 
  val sparkContext = loadSparkContext();
  val sqlContext = loadSqlContext();  
  
  var databaseDir = ""
  var queryFile = ""
  var resultFile = ""
  var resultFileTimes = ""
  
  def loadUserSettings(dbDir:String, 
                       qrFile:String) = {    
    this.databaseDir = dbDir
    if (dbDir.charAt(dbDir.length - 1) != '/')
      this.databaseDir += "/"      
    this.queryFile = qrFile
    var p = qrFile.lastIndexOf("/")
    if (p < 0)
    {
      this.resultFile = "./results.txt";
      this.resultFileTimes = "./resultTimes.txt";
    } else {
      this.resultFile = qrFile.substring(0, p) + "/results.txt";
      this.resultFileTimes = qrFile.substring(0, p) + "/resultTimes.txt";
    }
  }
   
  def loadSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("SparkTestFramework")
                              .set("spark.executor.memory", "16g")
                              .set("spark.sql.inMemoryColumnarStorage.compressed", "true")                              
                              .set("spark.sql.autoBroadcastJoinThreshold", "-1")
                              .set("spark.sql.parquet.filterPushdown", "true")
                              //.set("spark.storage.memoryFraction", "0.4")
                              //.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
                              .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                              .set("spark.sql.shuffle.partitions", "50")
                              //.set("spark.driver.allowMultipleContexts", "true")
                              
    new SparkContext(conf);
  }
  
  def loadSqlContext(): SQLContext = {
    val context = new org.apache.spark.sql.SQLContext(sparkContext)
    import context.implicits._ 
    context;    
  }
}
