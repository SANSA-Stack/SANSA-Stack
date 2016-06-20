/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package queryExecutor

import concurrent._
import scala.sys.process._
import java.util.Calendar
import collection.mutable.HashMap
import collection.mutable.ListBuffer

object QueryExecutor {
  private var _cachedTables = HashMap[String, String]();
  private val _sc = Settings.sparkContext
  private val _sqlContext = Settings.sqlContext
  private var _queryList:ListBuffer[Query] = null
  
  private var allFirstExecution = true;
  
  /**
   * Parse input compositeQueryFile (set of queries)
   * TODO: make support for single queries
   */
  def parseQueryFile() = {
    var resQueries = ListBuffer[Query]();
    val queries = scala.io.Source.fromFile(Settings.queryFile)
                                 .mkString
                                 .replaceAll("\\$\\$", "__")
                                 .split(">>>>>>");   
    for (query <- queries if query.length > 0){
      var parts = query.split("\\+\\+\\+\\+\\+\\+");
      var sqlQuery = parts(0);
      var queryName = sqlQuery.substring(0, sqlQuery.indexOf("\n"))
      sqlQuery = sqlQuery.substring(sqlQuery.indexOf("\n")+1)      
      var qStat = parts(1).substring(parts(1).indexOf("\n")+1);
      var tableStats = qStat.split("------\n")
      var tables = HashMap[String, Table]();
      
      for (tableStat <- tableStats if tableStat.length() > 0) {
        var bestTable = tableStat.substring(0, tableStat
                                               .indexOf("\n"))
                                               .split("\t");
        var tableName = bestTable(0);
        var bestId = bestTable(1);
        var tableType = bestTable(2);
        var tablePath = bestTable(3);
        var table = new Table(tableName, tableType, tablePath);
        tables(tableName) = table;
      }
      
      var resQuery = new Query(queryName, sqlQuery, qStat, tables);
      resQueries+=resQuery;
    }
    _queryList = resQueries;
  }
  
  /**
   * Generate path to VP/ExtVP tabel in HDFS
   */
  private def getPath(tType:String, tPath: String): String = {
    var res: String = "";
    
    if (tType=="VP"){
      res = Settings.databaseDir+"VP/"+tPath.replace("/","")+".parquet";
    } else if (tType=="SO" || tType=="OS" || tType=="SS"){
      res = Settings.databaseDir+"ExtVP/"+tType+"/"+tPath+".parquet";
    }
    
    res;
  }
  
  /**
   * Get a list of query names corresponding to all input queries
   */
  private def extructQueryNames(queries:ListBuffer[Query])
                :ListBuffer[String] = {
    var resNames = ListBuffer[String]();
     
    for (query <- queries){
      resNames+=query.queryName
    }
    
    resNames;
  }
  
  /**
   * Returns query objects corresponded to given query-name
   */
  private def getQueryByName(name:String, queries:ListBuffer[Query]):Query = {
    var res:Query = null
    
    for (query <- queries){
      if (query.queryName == name){
        
        res = query;
      }
    }
    
    res;    
  }
  
  /**
   *  Just adds given line to given file
   */
  private def writeLineToFile(line:String, fileName:String)= {    
    val fw = new java.io.FileWriter(fileName, true)
    try {
      fw.write( line+"\n")
    }
    finally fw.close() 
  }
  
  /**
   * removes all information from result line except pure time information
   */
  private def toJustTimesLine(line:String):String = {
    var res = line;
    
    while (res.contains("ms")){
      res = (res.substring(0, res.indexOf("ms")) 
            + res.substring(res.indexOf(")") + 1));
    }
    
    res
  }
  
  /**
   * This function prints obtained results into two csv files 
   * One file contains results as [time]ms([resultSize])
   * the other contains results as [time] (better for reuse in excell)
   * Every row in files correponds to one query
   * Every column in files correponds to one test case
   */
  def printResults(results:HashMap[String, String]) = {    
    var resMap = HashMap[String, HashMap[String, String]]();
    
    // Map different testcases for same query together 
    for (res <- results.keySet){
      var distName = res.split("--")
      if (resMap contains distName(0)){
        resMap(distName(0))(distName(1))=results(res);
      } else {
        resMap(distName(0))= HashMap[String, String]()
        resMap(distName(0))(distName(1))=results(res);
      }      
    }

    // print results as CSV
    
    // print date
    var date = "" + Calendar.getInstance().getTime()
    writeLineToFile(date, Settings.resultFile)
    writeLineToFile(date, Settings.resultFileTimes)
    
    // every line corresponds to one query 
    // every column corresponds to one test case
    for (res <- resMap.keySet.toList.sorted){
      var lineToPrint=res;
      for (testCase <- resMap(res).keySet.toList.sorted){
        lineToPrint = (lineToPrint 
                      + "\t" + resMap(res)(testCase)
                      + " [" + testCase+"]")
      }
      // print to result file 
      writeLineToFile(lineToPrint, Settings.resultFile)
      // print to result file containing only times
      writeLineToFile(toJustTimesLine(lineToPrint),
                             Settings.resultFileTimes)
    }
  }
  
  /**
   * Extract all names of the tests, which use ExtVP for execution
   */
  private def extractAllExtVPTestNames(qNames:ListBuffer[String])
                :ListBuffer[String] = {
    var queryNames = ListBuffer[String]();
    for (qr <-qNames) if (qr.contains("SO") 
                                  || qr.contains("OS")
                                  || qr.contains("SS")){
      queryNames+=qr
    }
    queryNames
  }
  
  /**
   * Extract all names of the tests, wich use only VP for execution
   */
  private def extractAllVPTestNames(qNames:ListBuffer[String])
                :ListBuffer[String] = {
    var queryNames = ListBuffer[String]();
    for (qr <-qNames) if (!(qr.contains("SO") 
                                  || qr.contains("OS")
                                  || qr.contains("SS"))){
      queryNames += qr
    }
    queryNames
  }
  
  /**
   * Preload tables containing in query to main memory 
   */
  private def preloadTables(qr:Query, actualPostfix:String) = {
    // Cache Tables
    for (tableStat <- qr.tables.values){
      var fileName = getPath(tableStat.tType, tableStat.tPath);        
      if (_cachedTables contains fileName){
        qr.query = qr.query.replaceAll(tableStat.tName,
                                       _cachedTables(fileName))
      } else {
        println("\tLoad Table "+tableStat.tName+" from "+fileName + "-> ")
        var table:org.apache.spark.sql.DataFrame = null;

        // repartition the tables in query        
        if (actualPostfix.contains("SO") && Settings.partNumberExtVP != 200)
          table = _sqlContext.parquetFile(fileName)
                  .repartition(Settings.partNumberExtVP);
        else if (Settings.partNumberVP != 200)
          table = _sqlContext.parquetFile(fileName)
                  .repartition(Settings.partNumberVP);
        else
          table = _sqlContext.parquetFile(fileName)
        table.registerTempTable(tableStat.tName);
        _sqlContext.cacheTable(tableStat.tName)

        var start = System.currentTimeMillis;            
        var size = table.count();
        var time = System.currentTimeMillis - start;
        println("\t\tCached "+size+" Elements in "+time+"ms");

        _cachedTables(fileName) = tableStat.tName;
      }
    }   
  }
  
  /**
   * Remove odl tables of old queries from main memory
   */
  private def unloadTables(){
    for (tableStat <- _cachedTables.values){
      print("\tUncache Table " + tableStat+"->");
      var start = System.currentTimeMillis;
      _sqlContext.dropTempTable(tableStat);
      var time = System.currentTimeMillis - start;
      println(" Uncached  in " + time + "ms");
    }
    _cachedTables = HashMap[String, String]();
    _sqlContext.clearCache();
  }
  
  private def avoidDoubleTableReferencing(qr: Query) = {
    for (tableStat <- qr.tables.values){
      var fileName = getPath(tableStat.tType, tableStat.tPath);        
      if (_cachedTables contains fileName){
        qr.query = qr.query.replaceAll(tableStat.tName, 
                                       _cachedTables(fileName))
      }
    }
  }
  /**
   * Execute a single test
   */
  private def executeQuery(qr:Query):String = {
    println()      
    print("\t Run test-> ")
    var resString=""
    var resSize:Long = 0
    for (tId <- 1 to Settings.testLoop){                         
      var start = System.currentTimeMillis
      var temp = _sqlContext.sql(qr.query)
      resSize = temp.count()
      var time = System.currentTimeMillis - start;
      temp = null;
      if (tId > 1) resString+="/"
      resString += time;
    }
    resString + "ms ("+resSize+")"
  }
  
  def runTests() = {   
    var temp = extructQueryNames(_queryList).sorted;
    var queryNames = ListBuffer[String]()
    queryNames ++= extractAllExtVPTestNames(temp)
    queryNames ++= extractAllVPTestNames(temp)
    
    var results = HashMap[String, String]()
    var testSet = ""
    for (queryN <- queryNames) if (queryN.contains("SO")){      
      
      var query = getQueryByName(queryN, _queryList)
      
      // Example queryName IL5-1-U-1--SO-OS-SS-VP__WatDiv1M
      var actualPrefix = queryN.substring(0, queryN.indexOf("--"))
      // = IL5-1-U-1
      var actualPostfix = queryN.substring(queryN.indexOf("--")+2)
      // = SO-OS-SS-VP__WatDiv1M
      var actualTestSet = queryN.substring(0, queryN.indexOf("-", queryN.indexOf("-")+1))      
      // = IL5-1

      // HACK for Selectivity Testing-Queries      
      if (queryN.indexOf("-") == queryN.indexOf("--"))
        actualTestSet = actualPrefix
      
      // HACK for yago    
      if (actualPostfix.contains("yago"))
      {
        actualTestSet = queryN.substring(0)
      }
      
      println("pr-"+actualPrefix+"pf-"+actualPostfix+"at"+actualTestSet)
      println("Test "+query.queryName+":");
      
      // Uncache Tables
      if (testSet.length>0 && actualTestSet!=testSet){
        unloadTables()
      }
      
      // cache tables
      if (testSet.length==0 || actualTestSet!=testSet){
        testSet = actualTestSet;
        preloadTables(query, actualPostfix)
      }
      
      avoidDoubleTableReferencing(query)

      // Print query plane
      println("HaLLO")
      println(_sqlContext.sql("""""" + query.query + """""")
                          .queryExecution
                          .executedPlan)
      println("HaLL1")
      // Run Tests
      // Execute a dummy execution, since all first execution is always slower
      // than the following executions
      if (allFirstExecution){ 
        var temp = executeQuery(query) 
        allFirstExecution = false
      }
      results(query.queryName) = executeQuery(query)
      println(results(query.queryName))
    }    
    //print results    
    printResults(results)
  }
}
