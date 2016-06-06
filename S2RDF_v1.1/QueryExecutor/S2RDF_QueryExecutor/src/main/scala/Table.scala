/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package queryExecutor

/**
 * Simple calss for SQL table description
 */
class Table(tN:String, tT:String, tP:String ) {
  
  // Table name (placeholder name)
  var tName:String = tN;
  // Table type (SO,OS,SS or VP)
  var tType:String = tT;
  // Path to the table in HDFS
  var tPath:String = tP;
  
}
