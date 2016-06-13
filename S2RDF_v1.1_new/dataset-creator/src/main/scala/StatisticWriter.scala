/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package dataCreator
/**
 * StatisticWriter records information about created tables using 
 * DataSetGenerator, it creates 4 statistic files for VP, ExtVP_SO, ExtVP_SS and 
 * ExtVP_OS tables respectively. 
 */
object StatisticWriter {
  
  // number of unique predicates
  private var _predicatesNum = 0: Int
  // number of triples in the input RDF set
  private var _inputSize = 0: Long
  // the name of the active statistic file, which is being written
  private var _statisticFileName = "": String
  
  private var _savedTables = 0: Int
  private var _unsavedNonEmptyTables = 0: Int
  private var _allPossibleTables = 0: Int
  
  /**
   * Initializes StatisticWriter
   */
  def init(predsNum: Int, inpSize: Long) = {
    _predicatesNum = predsNum
    _inputSize = inpSize
  }
  
  /**
   * Initializes recording of new statistic file.
   */
  def initNewStatisticFile(relType: String) = {
    _statisticFileName = ("stat_"+relType.toLowerCase+".txt")
    _savedTables = 0
    _unsavedNonEmptyTables = 0
    _allPossibleTables = if (relType == "VP") _predicatesNum 
                         else _predicatesNum * _predicatesNum
    
    val fw = new java.io.FileWriter(_statisticFileName, false)
    try {
      fw.write("\t" +relType+ " Statistic\n")
      fw.write("---------------------------------------------------------\n")
    }
    finally fw.close()     
  }

  /**
   * Puts the tail at the and of the written statistic files
   */
  def closeStatisticFile() = {
    val fw = new java.io.FileWriter(_statisticFileName, true)
    try {
      fw.write("---------------------------------------------------------\n")
      fw.write("Saved tabels ->" + _savedTables +"\n")
      fw.write("Unsaved non-empty tables ->" + _unsavedNonEmptyTables +"\n")
      fw.write("Empty tables ->" + (_allPossibleTables 
                                    - _savedTables
                                    - _unsavedNonEmptyTables) 
               +"\n")
    }
    finally fw.close() 
  }
  /**
   * Add new line to the actual statistic file
   */
  def addTableStatistic(tableName: String, sizeExtVpT: Long, sizeVpT: Long) = {    
    var statLine = tableName    
            
    if (sizeExtVpT > 0) {
      // ExtVP table statistic entry
      statLine += ("\t" + sizeExtVpT
                   + "\t" + sizeVpT
                   + "\t" + Helper.ratio(sizeExtVpT, sizeVpT)
                   + "\t" + Helper.ratio(sizeVpT, _inputSize))
    } else {
      // VP table statistic entry
      statLine += ("\t" + sizeVpT
                   + "\t" + _inputSize
                   + "\t" + Helper.ratio(sizeVpT, _inputSize))
    }
    
    val fw = new java.io.FileWriter(_statisticFileName, true)
    try {
      fw.write( statLine+"\n")
    }
    finally fw.close() 
  }
  /**
   * Increments the counter for the saved tables
   */
  def incSavedTables() = {
    _savedTables += 1
  }
  
  /**
   * Increments the counter for the unsaved tables, which are not empty, e.g. 
   * ExtVP tables having size bigger than ScaleUB * (Size of corresponding 
   * VP table)
   */
  def incUnsavedNonEmptyTables() = {
    _unsavedNonEmptyTables += 1
  }
}
