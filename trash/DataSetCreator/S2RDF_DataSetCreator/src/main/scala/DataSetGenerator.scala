/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */
 
package dataCreator
import collection.mutable.HashMap
/**
 * DataSetGenerator creates for an input RDF dataset its reprsentations as 
 * Triple Table, Vertical Partiitoning and Extended Vertical Partitioning in 
 * HDFS.
 * TT has to be created before VP and VP before ExtVP, since VP is used for 
 * ExtVP generating and TT is used for creation of VP and ExtVP.
 * 
 * The informations about created tables are saved to the statistics files using
 * StatisticWriter
 */
object DataSetGenerator {
  
  // Spark initialization
  private val _sc = Settings.sparkContext
  private val _sqlContext = Settings.sqlContext
  import _sqlContext.implicits._
  
  // number of triples in input dataset
  private var _inputSize = 0: Long
  // number of triples for every VP table  
  private var _vpTableSizes = new HashMap[String, Long]()
  // set of unique predicates from input RDF dataset
  private var _uPredicates = null: Array[String]
  
  /**
   * generate all datasets (TT, VP, ExtVP)
   * It becomes as an input a varible containing string ("VP","SO","OS","SS")
   * Functions creates --> 
   *  TT and VP tables for "VP"
   *  Loads TT, VP to the main memory and creates SO for input string "SO"
   *  Loads TT, VP to the main memory and creates OS for input string "OS"
   *  Loads TT, VP to the main memory and creates SS for input string "SS"
   *  
   *  The program assumes that TT and VP are already generated for creation of
   *  SO,OS,SS
   */
  def generateDataSet(datasetType: String) = {
    
    // create or load TripleTable if already created
    if (datasetType == "VP") createTT() else loadTT()
    // extarct all unique predicates from TripleTable
    // necessary for VP/ExtVP generation
    _uPredicates = _sqlContext.sql("select distinct pred from triples")
                              .map(t => t(0).toString())
                              .collect()

    StatisticWriter.init(_uPredicates.size, _inputSize)
    
    // create or load Vertical Partitioning if already exists
    if (datasetType == "VP") createVP() else loadVP()
        
    // if we create/recreate VP than we gonna later probably create/recreate 
    // ExtVP. Out of this reason we remove ExtVP directory containing old tables
    // and create it empty again
    if (datasetType == "VP"){
      Helper.removeDirInHDFS(Settings.extVpDir)
      Helper.createDirInHDFS(Settings.extVpDir)
    }
    // create Extended Vertical Partitioning table set definded by datasetType
    if (datasetType == "SO") createExtVP("SO")
    else if (datasetType == "OS") createExtVP("OS")
    else if (datasetType == "SS") createExtVP("SS")
  }

  // Triple Table schema
  case class Triple(sub: String, pred: String, obj: String)

  /**
   * Generate TripleTable and save it to Parquet file in HDFS.
   * The table has to be cached, since it is used for generation of VP and ExtVP
   */
  private def createTT() = {      
    val df = _sc.textFile(Settings.inputRDFSet)
                         .map(_.split("\t"))
                         .map(p => Triple(p(0), p(1), p(2)))
                         .toDF()
    // Commented out due to execution problem for dataset of 1 Bil triples
    // We do not need it anyway if the input dataset is correct and has no
    // double ellements. It was not the case for WatDiv
    //                     .distinct  
    df.registerTempTable("triples")     
    _sqlContext.cacheTable("triples")
    _inputSize = df.count()
    
    // remove old TripleTable and save it as Parquet
    Helper.removeDirInHDFS(Settings.tripleTable)
    df.saveAsParquetFile(Settings.tripleTable)
  }
  
  /**
   * Loads TT table and caches it to main memory.
   * TT table is used for generation of ExtVP and VP tables
   */
  private def loadTT() = {  
    val df = _sqlContext.parquetFile(Settings.tripleTable);
    df.registerTempTable("triples")     
    _sqlContext.cacheTable("triples")
    _inputSize = df.count()
  }
  
  /**
   * Generates VP table for each unique predicate in input RDF dataset.
   * All tables have to be cached, since they are used for generation of ExtVP 
   * tables.
   */
  private def createVP() = {    
    // create directory for all vp tables
    Helper.removeDirInHDFS(Settings.vpDir)
    Helper.createDirInHDFS(Settings.vpDir)
    StatisticWriter.initNewStatisticFile("VP")

    // create and cache vpTables for all predicates in input RDF dataset
    for (predicate <- _uPredicates){      
      var vpTable = _sqlContext.sql("select sub, obj "
                                  + "from triples where pred='"+predicate+"'")          
      
      val cleanPredicate = Helper.getPartName(predicate)  
      vpTable.registerTempTable(cleanPredicate)
      _sqlContext.cacheTable(cleanPredicate)
      _vpTableSizes(predicate) = vpTable.count()
      
      vpTable.saveAsParquetFile(Settings.vpDir + cleanPredicate + ".parquet")
            
      // print statistic line
      StatisticWriter.incSavedTables()
      StatisticWriter.addTableStatistic("<" + predicate + ">", 
                                        -1, 
                                        _vpTableSizes(predicate)) 
    }
    
    StatisticWriter.closeStatisticFile()
  }
  
  /**
   * Loads VP tables and caches them to main memory.
   * VP tables are used for generation of ExtVP tables
   */
  private def loadVP() = {  
    for (predicate <- _uPredicates){      
      val cleanPredicate = Helper.getPartName(predicate)    
      var vpTable = _sqlContext.parquetFile(Settings.vpDir 
                                            + cleanPredicate 
                                            + ".parquet")          
            
      vpTable.registerTempTable(cleanPredicate)
      _sqlContext.cacheTable(cleanPredicate)
      _vpTableSizes(predicate) = vpTable.count()
    }
  }
  
  /**
   * Generates ExtVP tables for all (relType(SO/OS/SS))-relations of all 
   * VP tables to the other VP tables 
   */
  private def createExtVP(relType: String) = {

    // create directory for all ExtVp tables of given relType (SO/OS/SS)    
    Helper.createDirInHDFS(Settings.extVpDir+relType)
    StatisticWriter.initNewStatisticFile(relType)
    
    var savedTables = 0
    var unsavedNonEmptyTables = 0
    var createdDirs = List[String]()
    
    // for every VP table generate a set of ExtVP tables, which represent its
    // (relType)-relations to the other VP tables
    for (pred1 <- _uPredicates) {            

      // get all predicates, whose TPs are in (relType)-relation with TP
      // (?x, pred1, ?y)
      var relatedPredicates = getRelatedPredicates(pred1, relType)

      for (pred2 <- relatedPredicates) {                
        var extVpTableSize = -1: Long
        
        // we avoid generation of ExtVP tables corresponding to subject-subject
        // relation to it self, since such tables are always equal to the
        // corresponding VP tables
        if (!(relType == "SS" && pred1 == pred2)) {
          var sqlCommand = getExtVpSQLcommand(pred1, pred2, relType)
          var extVpTable = _sqlContext.sql(sqlCommand)
          extVpTable.registerTempTable("extvp_table")
          // cache table to avoid recomputation of DF by storage to HDFS       
          _sqlContext.cacheTable("extvp_table")
          extVpTableSize = extVpTable.count()  

          // save ExtVP table in case if its size smaller than
          // ScaleUB*size(corresponding VPTable)
          if (extVpTableSize < _vpTableSizes(pred1) * Settings.ScaleUB) {
            
            // create directory extVP/relType/pred1 if not exists
            if (!createdDirs.contains(pred1)) {
              createdDirs = pred1 :: createdDirs
              Helper.createDirInHDFS(Settings.extVpDir 
                                     + relType + "/" 
                                     + Helper.getPartName(pred1))
            }
            
            // save ExtVP table
            extVpTable.saveAsParquetFile(Settings.extVpDir 
                                         + relType + "/"
                                         + Helper.getPartName(pred1) + "/"
                                         + Helper.getPartName(pred2)
                                         + ".parquet")
            StatisticWriter.incSavedTables()
          } else {
            StatisticWriter.incUnsavedNonEmptyTables()
          }
          
          _sqlContext.uncacheTable("extvp_table")
          
        } else {
          extVpTableSize = _vpTableSizes(pred1)
        }

        // print statistic line
        // save statistics about all ExtVP tables > 0, even about those, which
        // > then ScaleUB.
        // We need statistics about all non-empty tables 
        // for the Empty Table Optimization (avoiding query execution for
        // the queries having triple pattern relations, which lead to empty
        // result)
        StatisticWriter.addTableStatistic("<" + pred1 + "><" + pred2 + ">", 
                                          extVpTableSize, 
                                          _vpTableSizes(pred1))        
      }
        
    }
    
    StatisticWriter.closeStatisticFile()
    
  }

  /**
   * Returns all predicates, whose triple patterns are in (relType)-relation 
   * with TP of predicate pred.
   */
  private def getRelatedPredicates(pred: String, relType: String)
                : Array[String] = {  
    var sqlRelPreds = ("select distinct pred "
                        + "from triples t1 "
                        + "left semi join "+Helper.getPartName(pred) + " t2 "
                        + "on")

    if (relType == "SS"){
      sqlRelPreds += "(t1.sub=t2.sub)"
    } else if (relType == "OS"){
      sqlRelPreds += "(t1.sub=t2.obj)"
    } else if (relType == "SO"){
      sqlRelPreds += "(t1.obj=t2.sub)"
    }  

    _sqlContext.sql(sqlRelPreds).map(t => t(0).toString()).collect()
  }
  
  /**
   * Generates SQL query to obtain ExtVP_(relType)pred1|pred2 table containing
   * all triples(pairs) from VPpred1, which are linked by (relType)-relation
   * with some other pair in VPpred2
   */
  private def getExtVpSQLcommand(pred1: String, 
                                 pred2: String, 
                                 relType: String): String = {
    var command = ("select t1.sub as sub, t1.obj as obj "
                    + "from " + Helper.getPartName(pred1) + " t1 "
                    + "left semi join " + Helper.getPartName(pred2) + " t2 "
                    + "on ")

    if (relType == "SS"){
      command += "(t1.sub=t2.sub)"
    } else if (relType == "OS"){
      command += "(t1.obj=t2.sub)"
    } else if (relType == "SO"){
      command += "(t1.sub=t2.obj)"
    }
    
    command
  }
}
