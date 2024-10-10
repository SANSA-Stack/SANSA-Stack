package net.sansa_stack.query.spark.hdt

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.{Algebra, OpWalker}
import org.apache.jena.sparql.expr.Expr

/**
  * A SPARQL to SQL rewritter based on HDT schema.
  *
  * @author David Ibhaluobe, Gezim Sejdiu.
  */
object Sparql2SQL {

  /**
    * Map FILTER conditions over HDT schema.
    *
    * @param cond a FILTER condition.
    * @return a filter operator based on the HDT schema fields.
    */
  def filterHDT(cond: Expr): String =
    createFilterString(cond)

  /**
    * Return HDT column names.
    *
    * @param value one of SPO.
    * @return HDT column name.
    */
  def getColumnName(value: String): String = {
    if (value.trim.equalsIgnoreCase("?S")) {
      "subjects_hdt.name"
    } else if (value.trim.equalsIgnoreCase("?P")) {
      "predicates_hdt.name"
    } else if (value.trim.equalsIgnoreCase("?O")) {
      "objects_hdt.name"
    } else {
      ""
    }
  }

  /**
    * Return a complete FILTER expression.
    *
    * @param cond a FILTER operator.
    * @return a complete FILTER expression.
    */
  def createFilterString(cond: Expr): String = {
    val fName = cond.getFunction.getFunctionName(null)
    val argsList = cond.getFunction.getArgs

    if (fName.trim.equals("strstarts")) {
      getColumnName(argsList.get(0).toString) + s" like '${argsList.get(1).toString.replace("\"", "")}%'"
    } else if (fName.toUpperCase.trim.equals("STRLEN")) {
      s"length(${getColumnName(argsList.get(0).toString)})"

    } else if (fName.toUpperCase.trim.equals("SUBSTR")) {

      if (argsList.size() == 2) {
        s" substr(${getColumnName(argsList.get(0).toString)},${argsList.get(1).toString})"
      }
      else if (argsList.size() == 3) {
        s" substr(${getColumnName(argsList.get(0).toString)},${argsList.get(1).toString},${argsList.get(2).toString})"
      }
      else ""
    } else if (fName.toUpperCase.trim.equals("STRENDS")) {
      s"${getColumnName(argsList.get(0).toString)} like '%${argsList.get(1).toString.replace("\"", "")}'"
      ""
    } else if (fName.toUpperCase.trim.equals("CONTAINS")) {
      s"${getColumnName(argsList.get(0).toString)} like '%${argsList.get(1).toString.replace("\"", "")}%'"
    } else if (fName.toUpperCase.trim.equals("RAND")) {
      " rand() "
    } else if (fName.toUpperCase.trim.equals("IN")) {
      ""
    } else if (fName.toUpperCase.trim.equals("NOT IN")) {
      ""
    } else if (fName.toUpperCase.trim.equals("STRBEFORE")) {
      s" substr(${getColumnName(argsList.get(0).toString)},0, instr(${getColumnName(argsList.get(0).toString)},'${argsList.get(1).toString.replace("\"", "")}')-1) "
    } else if (fName.toUpperCase.trim.equals("STRAFTER")) {
      s" substr(${getColumnName(argsList.get(0).toString)},instr(${getColumnName(argsList.get(0).toString)},'${argsList.get(1).toString.replace("\"", "")}') + length('${argsList.get(1).toString.replace("\"", "")}')) "
    } else if (fName.toUpperCase.trim.equals("REPLACE")) {
      s" replace(${getColumnName(argsList.get(0).toString)},'${argsList.get(1).toString.replace("\"", "")}','${argsList.get(2).toString.replace("\"", "")}')"
    } else if (fName.toUpperCase.trim.equals("GE")) {
      if (argsList.get(0).isFunction) {
        createFilterString(argsList.get(0)) + " >= " + argsList.get(1).toString
      } else {
        getColumnName(argsList.get(0).toString) + " >= " + argsList.get(1).toString
      }
    } else if (fName.toUpperCase.trim.equals("GT")) {
      if (argsList.get(0).isFunction) {
        createFilterString(argsList.get(0)) + " >= " + argsList.get(1).toString
      } else {
        getColumnName(argsList.get(0).toString) + " >= " + argsList.get(1).toString
      }
    } else if (fName.toUpperCase.trim.equals("LT")) {
      if (argsList.get(0).isFunction) {
        createFilterString(argsList.get(0)) + " <= " + argsList.get(1).toString
      } else {
        getColumnName(argsList.get(0).toString) + " <= " + argsList.get(1).toString
      }
    } else if (fName.toUpperCase.trim.equals("EQ")) {
      if (argsList.get(0).isFunction) {
        createFilterString(argsList.get(0)) + " =  " + argsList.get(1).toString
      } else {
        getColumnName(argsList.get(0).toString) + " = " + argsList.get(1).toString
      }
    } else if (fName.trim.toUpperCase.equals("AND")) {
      createFilterString(argsList.get(0)) + " and " + createFilterString(argsList.get(1))
    } else if (fName.trim.toLowerCase().equals("OR")) {
      createFilterString(argsList.get(0)) + " or " + createFilterString(argsList.get(1))
    } else {
      throw new UnsupportedOperationException(s"Function not implemented $fName")
      ""
    }

  }

  /**
    * Check if COUNT clause is present on SPARQL query.
    *
    * @return true if present, false otherwise.
    */
  def isCountEnabled: Boolean = {
    var found = false
    for (i <- 0 until SparqlOpVisitor.aggregatorList.size()) {
      if (SparqlOpVisitor.aggregatorList.get(i).getAggregator.getName.equalsIgnoreCase("COUNT")) {
        found = true
      }
    }
    found
  }

  /**
    * A solution sequence which is transformed into
    * one involving only a subset of the variables
    * present into SPARQL query.
    *
    * @return a projection fields.
    */
  def getProjectionFields: String = {
    var result = ""

    if (isCountEnabled) {
      result = " count(*) "
    } else {
      for (i <- 0 until SparqlOpVisitor.varList.size()) {
        val name = SparqlOpVisitor.varList.get(i).getVarName

        if (name.equalsIgnoreCase("S")) {
          result += s"subjects_hdt.name as subject, "
        } else if (name.equalsIgnoreCase("O")) {
          result += s"objects_hdt.name as object, "
        } else if (name.equalsIgnoreCase("P")) {
          result += s"predicates_hdt.name as predicate, "
        }
      }
    }

    result.reverse.replaceFirst(",", "").reverse
  }

  /**
    * Map WHERE conditions into clauses.
    *
    * @return a SQL WHERE clause using HDT schema.
    */
  def getWhereCondition: String = {
    var tempStr = ""
    for (i <- 0 until SparqlOpVisitor.whereCondition.size()) {
      if (!SparqlOpVisitor.optional.get(i)) {
        if (!SparqlOpVisitor.subjects.get(i).toString().toLowerCase().contains("?s")) {
          tempStr += s" subjects_hdt.name='${SparqlOpVisitor.subjects.get(i)}' and"
        }
        if (!SparqlOpVisitor.objects.get(i).toString().toLowerCase().contains("?o")) {
          tempStr += s" objects_hdt.name='${SparqlOpVisitor.objects.get(i)}' and"
        }
        if (!SparqlOpVisitor.predicates.get(i).toString().toLowerCase().contains("?p")) {
          tempStr += s" predicates_hdt.name='${SparqlOpVisitor.predicates.get(i)}' and"
        }
      }
    }
    tempStr = tempStr.reverse.replaceFirst("dna", "").reverse

    if (SparqlOpVisitor.optional.contains(true)) {
      for (i <- 0 until SparqlOpVisitor.whereCondition.size()) {
        if (SparqlOpVisitor.optional.get(i)) {
          tempStr += " or ( "
          if (!SparqlOpVisitor.subjects.get(i).toString().toLowerCase().contains("?s")) {
            tempStr += s" subjects_hdt.name='${SparqlOpVisitor.subjects.get(i)}' and"
          }
          if (!SparqlOpVisitor.objects.get(i).toString().toLowerCase().contains("?o")) {
            tempStr += s" objects_hdt.name='${SparqlOpVisitor.objects.get(i)}' and"
          }
          if (!SparqlOpVisitor.predicates.get(i).toString().toLowerCase().contains("?p")) {
            tempStr += s" predicates_hdt.name='${SparqlOpVisitor.predicates.get(i)}' and"
          }
          tempStr = tempStr.reverse.replaceFirst("dna", "").reverse
          tempStr += " )"
        }
      }
    }

    if (tempStr.length > 5) {
      s" where ($tempStr)"
    }
    else {
      " where 1=1 "
    }

  }


  /**
    * Get DISTINCT clauses.
    *
    * @return a DISTINCT clause mapped to HDT schema.
    */
  def getDistinct: String = {
    if (SparqlOpVisitor.isDistinctEnabled) {

      var groupBy = ""
      for (i <- 0 until SparqlOpVisitor.varList.size()) {
        if (SparqlOpVisitor.subjects.contains(SparqlOpVisitor.varList.get(i))) {
          groupBy += s"subjects_hdt.name, "
        } else if (SparqlOpVisitor.objects.contains(SparqlOpVisitor.varList.get(i))) {
          groupBy += s"objects_hdt.name, "

        } else if (SparqlOpVisitor.predicates.contains(SparqlOpVisitor.varList.get(i))) {
          groupBy += s"predicates_hdt.name, "
        }
      }
      "group by " + groupBy.reverse.replaceFirst(",", "").reverse
    } else {
      ""
    }
  }

  /**
    * Get FILTER expression
    *
    * @return a FILTER condition
    */
  def getFilterCondition: String = {
    var strCondition = ""
    var logicalOp = ""

    for (i <- 0 until SparqlOpVisitor.filters.size()) {
      val cond = filterHDT(SparqlOpVisitor.filters.get(i))
      if (cond.length > 2) {
        strCondition += cond + " and "
      }

    }
    strCondition = strCondition.reverse.replaceFirst("dna", "").reverse
    if (strCondition.length > 5) s" $strCondition" else " 1=1"
  }


  /**
    * Return SQL query rewritten from SPARQL query and
    * mapped into HDT partition.
    *
    * @param queryStr a SPARQL query string.
    * @return SQL query.
    */
  def getQuery(queryStr: String): String = {
    SparqlOpVisitor.reset()

    val query = QueryFactory.create(queryStr)
    val op = Algebra.compile(query)
    OpWalker.walk(op, SparqlOpVisitor)

    val sql = s"select ${getProjectionFields}from hdt inner join subjects_hdt on hdt.s=subjects_hdt.index" +
      s" inner join objects_hdt on hdt.o=objects_hdt.index" +
      s" inner join predicates_hdt on hdt.p=predicates_hdt.index" +
      s" ${getWhereCondition} and ${getFilterCondition} ${getDistinct}"

    sql
  }

}
