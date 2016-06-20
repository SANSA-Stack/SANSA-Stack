/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */
package queryExecutor

import collection.mutable.HashMap

/**
 * Simple class for query description
 */
class Query(qN:String, qr:String, stat:String, ts:HashMap[String, Table]) {
 
  var queryName: String = qN  
  // SQL query as string
  var query: String = qr
  // Table load instruction as string
  var statistic: String = stat  
  // The list of tables, which exist in query
  var tables:HashMap[String, Table] = ts
  
}
