package net.sansa_stack.query.tests.util

import org.apache.jena.query.{Query, ResultSet}
import org.apache.jena.sparql.resultset.ResultSetCompare

object ResultSetCompareUtils {

  def resultSetEquivalent(query: Query,
                          resultsActual: ResultSet,
                          resultsExpected: ResultSet,
                          testByValue: Boolean = true): Boolean = {
    if (testByValue) {
      if (query.isOrdered) {
        ResultSetCompare.equalsByValueAndOrder(resultsExpected, resultsActual)
      } else {
        ResultSetCompare.equalsByValue(resultsExpected, resultsActual)
      }
    } else {
      if (query.isOrdered) {
        ResultSetCompare.equalsByTermAndOrder(resultsExpected, resultsActual)
      } else {
        ResultSetCompare.equalsByTerm(resultsExpected, resultsActual)
      }
    }
  }

}
