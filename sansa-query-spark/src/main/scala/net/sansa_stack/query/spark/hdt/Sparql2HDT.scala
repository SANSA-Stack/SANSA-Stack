package net.sansa_stack.query.spark.hdt

import org.apache.jena.sparql.expr.Expr
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.OpWalker
import net.sansa_stack.rdf.spark.model.hdt._

object Sparql2HDT {

  val ops = SparqlOpVisitor

  def getHDTFilter(cond: Expr): String = {
    createFilterString(cond)
  }

  def getColumnName(value: String) = {
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

  def createFilterString(cond: Expr): String = {
    var fName = cond.getFunction.getFunctionName(null);
    val argsList = cond.getFunction.getArgs

    if (fName.trim.equals("strstarts")) {
      getColumnName(argsList.get(0).toString) + s" like '${argsList.get(1).toString.replace("\"", "")}%'"
    } else if (fName.toUpperCase.trim.equals("STRLEN")) {
      s"length(${getColumnName(argsList.get(0).toString())})"

    } else if (fName.toUpperCase.trim.equals("SUBSTR")) {

      if (argsList.size() == 2)
        s" substr(${getColumnName(argsList.get(0).toString)},${argsList.get(1).toString})"
      else if (argsList.size() == 3)
        s" substr(${getColumnName(argsList.get(0).toString)},${argsList.get(1).toString},${argsList.get(2).toString})"
      else
        ""
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
      throw new UnsupportedOperationException(s"Function not implemented ${fName}")
      ""
    }

  }

  def isCountEnabled(): Boolean = {
    var status = false
    for (i <- 0 to ops.aggregatorList.size() - 1) {
      if (ops.aggregatorList.get(i).getAggregator.getName.equalsIgnoreCase("COUNT")) {
        status = true;
      }
    }
    status
  }

  def getProjectionFields() = {
    var result = ""

    if (isCountEnabled) {
      result = " count(*) "
    } else {
      for (i <- 0 to ops.varList.size() - 1) {
        val name = ops.varList.get(i).getVarName

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

  def getWhereCondition(): String = {
    var tempStr = ""
    for (i <- 0 to ops.whereCondition.size() - 1) {
      if (!ops.optional.get(i)) {
        if (!ops.subjects.get(i).toString().toLowerCase().contains("?s")) {
          tempStr += s" subjects_hdt.name='${ops.subjects.get(i)}' and"
        }
        if (!ops.objects.get(i).toString().toLowerCase().contains("?o")) {
          tempStr += s" objects_hdt.name='${ops.objects.get(i)}' and"
        }
        if (!ops.predicates.get(i).toString().toLowerCase().contains("?p")) {
          tempStr += s" predicates_hdt.name='${ops.predicates.get(i)}' and"
        }
      }
    }
    tempStr = tempStr.reverse.replaceFirst("dna", "").reverse

    if (ops.optional.contains(true)) {
      for (i <- 0 to ops.whereCondition.size() - 1) {
        if (ops.optional.get(i)) {
          tempStr += " or ( "
          if (!ops.subjects.get(i).toString().toLowerCase().contains("?s")) {
            tempStr += s" subjects_hdt.name='${ops.subjects.get(i)}' and"
          }
          if (!ops.objects.get(i).toString().toLowerCase().contains("?o")) {
            tempStr += s" objects_hdt.name='${ops.objects.get(i)}' and"
          }
          if (!ops.predicates.get(i).toString().toLowerCase().contains("?p")) {
            tempStr += s" predicates_hdt.name='${ops.predicates.get(i)}' and"
          }
          tempStr = tempStr.reverse.replaceFirst("dna", "").reverse
          tempStr += " )"
        }
      }
    }

    if (tempStr.length > 5) { s" where (${tempStr})" }
    else { " where 1=1 " }

  }

  def getDistinct(): String = {
    if (ops.isDistinctEnabled) {

      var groupBy = ""
      for (i <- 0 to ops.varList.size() - 1) {
        if (ops.subjects.contains(ops.varList.get(i))) {
          groupBy += s"subjects_hdt.name, "
        } else if (ops.objects.contains(ops.varList.get(i))) {
          groupBy += s"objects_hdt.name, "

        } else if (ops.predicates.contains(ops.varList.get(i))) {
          groupBy += s"predicates_hdt.name, "
        }
      }
      "group by " + groupBy.reverse.replaceFirst(",", "").reverse
    } else {
      ""
    }
  }

  def getFilterCondition(): String = {
    var strCondition = ""
    var logicalOp = ""

    for (i <- 0 to ops.filters.size() - 1) {
      val cond = getHDTFilter(ops.filters.get(i));
      if (cond.length > 2) {
        strCondition += cond + " and "
      }

    }

    strCondition = strCondition.reverse.replaceFirst("dna", "").reverse
    println(strCondition)
    if (strCondition.length > 5) s" ${strCondition}" else " 1=1"
  }

  def getQuery(queryStr: String) = {

    ops.reset

    val query = QueryFactory.create(queryStr)
    val op = Algebra.compile(query)
    OpWalker.walk(op, ops)
    val result = s"select ${getProjectionFields()}from hdt inner join subjects_hdt on hdt.s=subjects_hdt.index" +
      s" inner join objects_hdt on hdt.o=objects_hdt.index" +
      s" inner join predicates_hdt on hdt.p=predicates_hdt.index" +
      s" ${getWhereCondition()} and ${getFilterCondition()} ${getDistinct()}"
    result
  }

}