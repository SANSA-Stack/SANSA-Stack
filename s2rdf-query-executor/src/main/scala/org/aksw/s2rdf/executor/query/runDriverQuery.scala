package org.aksw.s2rdf.executor.query

/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databaspackage org.aksw.s2rdf.executor.query

es and Information Systems
 * Uni Freiburg
 */
object runDriverQuery {
  def main(args:Array[String]) = {
    Settings.loadUserSettings(args(0), args(1))
    val queries = QueryExecutor.parseQueryFile()
//    QueryExecutor.runTests()
    queries.foreach{q=>
//      Settings.sqlContext.sql(q.query)
      QueryExecutor.run(q)
    }

  }
}