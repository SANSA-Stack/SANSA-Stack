package net.sansa_stack.query.spark.rdd.op

import org.apache.jena.query.Query
import org.apache.jena.sparql.syntax.ElementSubQuery

object RddOfRdfOpUtils {
  /**
   * Convert any SPARQL query form to ASK. ASK queries are returned unchanged.
   * Other forms are converted to SELECT and used as a sub select of the form
   * ASK { SELECT * { originalPattern } }
   *
   * @param query
   * @return
   */
  def enforceQueryAskType(query: Query): Query = {
    val result = if (query.isAskType) {
      query
    } else {
      val inner = query.cloneQuery()
      inner.setQuerySelectType()
      inner.setQueryResultStar(true)
      val outer = new Query()

      // Copy prefixes to the outer query and clear them on the inner one
      outer.setPrefixMapping(inner.getPrefixMapping)
      inner.getPrefixMapping.clearNsPrefixMap()

      outer.setQueryAskType()
      outer.setQueryPattern(new ElementSubQuery(inner))
      outer
    }

    result
  }
}
