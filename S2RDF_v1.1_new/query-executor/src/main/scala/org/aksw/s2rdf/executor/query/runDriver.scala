import org.aksw.s2rdf.executor.query.Settings

import org.aksw.s2rdf.executor.query.QueryExecutor

/* Copyright package org.aksw.s2rdf.executor.query

Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */
object runDriver { 
  def main(args:Array[String]) = {
    Settings.loadUserSettings(args(0), args(1))
    QueryExecutor.parseQueryFile()
    QueryExecutor.runTests()
  }
}