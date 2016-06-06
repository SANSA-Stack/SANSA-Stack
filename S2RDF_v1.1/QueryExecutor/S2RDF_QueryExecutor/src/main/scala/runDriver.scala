/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

import queryExecutor.QueryExecutor
import queryExecutor.Settings
object runDriver { 
  def main(args:Array[String]) = {
    Settings.loadUserSettings(args(0), args(1))
    QueryExecutor.parseQueryFile();
    QueryExecutor.runTests();
  }
}