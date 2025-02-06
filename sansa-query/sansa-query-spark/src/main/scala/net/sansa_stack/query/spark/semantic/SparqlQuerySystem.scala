package net.sansa_stack.query.spark.semantic

import java.io._
import java.util.{Scanner, StringTokenizer}

// import scala.collection.JavaConversions._
import com.google.common.collect.ArrayListMultimap
import net.sansa_stack.query.spark.semantic.utils.Helpers._
import net.sansa_stack.rdf.common.partition.utils.Symbols
import org.apache.spark.rdd._

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/*
 * QuerySystem - query on semantic partition data
 *
 * @partitionData - a RDD of n-triples (formatted).
 * @queryInputPath - query file path.
 * @queryResultPath - path for output result.
 * @numOfFilesPartition - total number of files to save the partition data.
 */
class QuerySystem(
  partitionData: RDD[String],
  queryInputPath: String)
  extends Serializable {
  var _selectVariables: Map[Int, ArrayBuffer[String]] = Map()
  var _whereVariables: Map[Int, ArrayBuffer[String]] = Map()
  var _WhereTriples: Map[Int, ArrayBuffer[String]] = Map()
  var _numOfWhereClauseTriples: Map[Int, Int] = Map()
  var _queriesLimit: Map[Int, Int] = Map()
  var _unionOp: Map[Int, Map[String, Boolean]] = Map()
  var _filterOp: Map[Int, String] = Map()
  var _queriesProcessTime: ArrayBuffer[Long] = ArrayBuffer()

  var workingTripleRDD: RDD[(String, List[String])] = _
  var workingPartialRDD: RDD[(String, List[String])] = _
  var unionOutputRDD: RDD[String] = _
  var outputRDD: RDD[String] = _
  val symbol = Symbols.symbol

  def run(): RDD[String] = {
    // parse queries
    for (qID <- this.fetchQueries.indices) {
      // refactor queries
      val refactoredQueries = this.refactorUnionQueries(this.fetchQueries(qID), qID)
      // start process time
      val startTime = System.nanoTime()

      // iterate refactored UNION queries
      for (qUID <- refactoredQueries.indices) {
        // case: UNION
        _unionOp = Map(qID -> Map(
          "isUnion" -> (refactoredQueries.length > 1),
          "first" -> qUID.equals(0),
          "last" -> qUID.equals(refactoredQueries.length - 1)))

        // parse query
        this.queryParser(refactoredQueries(qUID), qID)

        // query engine
        this.queryEngine(qID)
      }
    }
    outputRDD
  }

  // -------------------------------
  // Parse Queries & Store Variables
  // -------------------------------

  // fetch queries from input file
  def fetchQueries: ArrayBuffer[ArrayBuffer[String]] = {
    val file = new File(queryInputPath)
    val fileScanner = new Scanner(file)
    var queryList: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()

    // scan lines
    while (fileScanner.hasNext) {
      var line = fileScanner.nextLine.trim()

      // ignore empty lines
      if (line.nonEmpty) {
        // query should start with SELECT
        if (line.toUpperCase.startsWith("SELECT")) {
          var i = 0
          var singleQuery: ArrayBuffer[String] = ArrayBuffer()
          singleQuery += line

          // add elements until "}" found
          while (fileScanner.hasNext) {
            line = fileScanner.nextLine.trim()
            i += 1 // validate duplicate SELECT right after the first SELECT

            // ignore empty lines
            if (line.nonEmpty) {
              // if reach at the end
              if (!line.toUpperCase.startsWith("SELECT") || i.equals(1)) {
                singleQuery += line
              } else {
                // append query to the list
                queryList += singleQuery

                // re-initialize query
                singleQuery = ArrayBuffer()

                // next query SELECT
                singleQuery += line
              }
            }
          }

          // append query to the list
          queryList += singleQuery
        }
      }
    }

    queryList
  }

  // refactor UNION queries
  def refactorUnionQueries(query: ArrayBuffer[String], qID: Int): ArrayBuffer[ArrayBuffer[String]] = {
    var queriesList: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    var selectLine: String = ""
    var triplesList: ArrayBuffer[String] = ArrayBuffer()
    var singleQueryP1: ArrayBuffer[String] = ArrayBuffer()
    var singleQueryP2: ArrayBuffer[String] = ArrayBuffer()
    var singleQueryP3: ArrayBuffer[String] = ArrayBuffer()
    var j = 0
    var isEnd = false

    // iterate
    for (_ <- query.indices by j + 1) {
      var line = query(j)

      // common part: before triples
      if (line.toUpperCase.contains("SELECT") || line.toUpperCase.contains("WHERE")) {
        singleQueryP1 += line
        j += 1

        if (line.toUpperCase.contains("SELECT")) selectLine = line
      }

      // query: multi triple
      if (line.startsWith(this.symbol("curly-bracket-left")) && !line.endsWith(this.symbol("curly-bracket-right"))) {
        j += 1

        while (!line.startsWith(this.symbol("curly-bracket-right"))) {
          line = query(j)
          j += 1

          if (!line.startsWith(this.symbol("curly-bracket-right"))) {
            singleQueryP2 += line
            triplesList += line
          }
        }

        line = query(j)
        queriesList += singleQueryP1.union(singleQueryP2)
        singleQueryP2 = ArrayBuffer()
      }

      // query: single triple
      if (line.startsWith(this.symbol("curly-bracket-left")) && line.endsWith(this.symbol("curly-bracket-right"))) {
        val tokens = line.split(this.symbol("blank"))
        val triple = tokens(1) + this.symbol("blank") + tokens(2) + this.symbol("blank") + tokens(3) + this.symbol("blank") + tokens(4)

        singleQueryP2 += triple
        queriesList += singleQueryP1.union(singleQueryP2)
        triplesList += triple
        singleQueryP2 = ArrayBuffer()
        j += 1
      }

      // skip UNION
      if (line.toUpperCase.equals("UNION")) j += 1

      // common part: after triples
      if (line.equals(this.symbol("curly-bracket-right")) && !isEnd) {
        singleQueryP3 += line

        while (j < query.size - 1) {
          j += 1
          line = query(j)
          singleQueryP3 += line
        }

        // block entry after first time
        isEnd = true
      }
    }

    // set queries
    if (queriesList.isEmpty) {
      queriesList += query
    } else {
      for (i <- queriesList.indices) {
        queriesList(i) = queriesList(i) ++ singleQueryP3
      }

      // case: Union
      if (!selectLine.contains(this.symbol("asterisk"))) {
        val selectVariables = this.lineParser(selectLine).filter(_.nonEmpty)
        val whereVariables = this.fetchWhereVariables(triplesList).filter(_.nonEmpty)
        this.validateVariables(selectVariables, whereVariables, qID)
      }
    }

    queriesList
  }

  // parse queries
  def queryParser(query: ArrayBuffer[String], qID: Int): Unit = {
    var selectLine: String = ""
    var whereLines: ArrayBuffer[String] = ArrayBuffer()
    var filterLine: String = ""
    var isEnd = false

    // validate query and fetch SELECT and WHERE clauses
    for (i <- 0 to query.length if !isEnd) {
      var j = i
      var line = query(i)

      // clause: SELECT
      if (line.toUpperCase.startsWith("SELECT")) {
        // exception: more than one SELECT line
        if (selectLine.nonEmpty) {
          throw new IllegalStateException(s"Multiple SELECT lines detected: $line")
        }

        selectLine = line
      } else {
        // clause: WHERE
        if (line.toUpperCase.startsWith("WHERE")) {
          whereLines += line

          // store all WHERE lines
          while (!line.endsWith(this.symbol("curly-bracket-right"))) {
            j += 1
            line = query(j)

            // exception: more than one WHERE line
            if (line.toUpperCase.startsWith("WHERE")) {
              throw new IllegalStateException(s"Multiple WHERE lines detected: $line")
            }

            // case: WHERE or FILTER
            if (!line.toUpperCase.contains("FILTER")) {
              whereLines += line
            } else {
              // exception: more than one FILTER line
              if (filterLine.nonEmpty) {
                throw new IllegalStateException(s"Only one Filter Operator is allowed per query: $line")
              } else {
                if (line.toUpperCase.startsWith("FILTER") && line.toUpperCase.endsWith(this.symbol("round-bracket-right"))) {
                  val locationPoint1 = line.indexOf(this.symbol("round-bracket-left"))
                  val locationPoint2 = line.lastIndexOf(this.symbol("round-bracket-right"))
                  filterLine = line.substring(locationPoint1 + 1, locationPoint2)
                } else {
                  j += 1
                  line = query(j)

                  while (!line.toUpperCase.equals(this.symbol("round-bracket-right"))) {
                    filterLine += this.symbol("blank") + line
                    filterLine = filterLine.trim

                    j += 1
                    line = query(j)
                  }
                }

                filterLine = filterLine.trim.replaceAll(" +", this.symbol("blank")) // remove multiple spaces to a single space
              }
            }
          }

          // after WHERE clause
          for (k <- j + 1 until query.size) {
            line = query(k)

            // case: LIMIT
            if (line.toUpperCase.startsWith("LIMIT")) {
              // exception: more than one LIMIT line
              if (_queriesLimit.get(qID).isDefined) {
                throw new IllegalStateException(s"Only one LIMIT Operator is allowed per query: $line")
              }

              val locationPoint = line.lastIndexOf(this.symbol("blank")) // split line at location: LIMIT
              val newLine = line.substring(locationPoint) // skip: LIMIT
              val queryLimit = newLine.substring(1, newLine.length)

              // store LIMIT value
              if (Try(queryLimit.toInt).isSuccess) _queriesLimit = Map(qID -> queryLimit.toInt)
              else throw new IllegalStateException(s"Limit must be an Integer value! Supplied: $queryLimit")
            }

            // increment
            j += 1
          }

          isEnd = true
        } else throw new IllegalStateException("WHERE Clause not found!")
      }
    }

    // set values to variables
    val selectVariables = this.lineParser(selectLine).filter(_.nonEmpty)
    val whereVariables = this.fetchWhereVariables(whereLines).filter(_.nonEmpty)
    val WhereTriples = this.fetchWhereTriples(whereLines, whereVariables)
    val filterVariables = this.lineParser(filterLine)

    // append variables: SELECT clause
    _selectVariables += (qID -> selectVariables)

    // append variables: WHERE clause
    _whereVariables += (qID -> whereVariables)

    // append WHERE triples
    _WhereTriples += (qID -> WhereTriples)

    // validate SELECT clause variables
    if (!selectLine.contains(this.symbol("asterisk")) && !_unionOp(qID)("isUnion")) {
      this.validateVariables(selectVariables, _whereVariables(qID), qID)
    }

    // validate FILTER clause variables
    this.validateVariables(filterVariables, whereVariables, qID, "FILTER")

    // append FILTER
    _filterOp += (qID -> filterLine)

    // append number of clause in a query
    _numOfWhereClauseTriples += (qID -> WhereTriples.size)
  }

  // parse line and store SELECT and WHERE clause variables
  def lineParser(lineParse: String): ArrayBuffer[String] = {
    var line: String = lineParse
    var varList: ArrayBuffer[String] = ArrayBuffer()

    // case: SELECT *
    if (!line.contains(this.symbol("asterisk"))) {
      // split line at location: ?
      var locationPoint = line.indexOf(this.symbol("question-mark"))

      // one or more SELECT or WHERE variables
      while (locationPoint >= 0) {
        // skip: SELECT (left with all variables)
        line = line.substring(locationPoint)

        // split line at location: blank space
        locationPoint = line.indexOf(this.symbol("blank"))

        // when there is no more variables found after split
        if (locationPoint.equals(-1)) {
          // set location point to end of line
          locationPoint = line.length()
        }

        // set value to a variable
        var variable = line.substring(0, locationPoint)
        if (variable.endsWith(this.symbol("round-bracket-right"))) {
          variable = line.substring(0, locationPoint - 1)
        }

        // add variable to the list
        varList += variable

        // set next location point
        line = line.substring(locationPoint)
        locationPoint = line.indexOf(this.symbol("question-mark"))
      }
    } else varList += this.symbol("asterisk")

    // filter out duplicates
    varList = this.removeDuplicates(varList)

    varList
  }

  // fetch variables: WHERE clause
  def fetchWhereVariables(linesParse: ArrayBuffer[String]): ArrayBuffer[String] = {
    var varList: ArrayBuffer[String] = ArrayBuffer()

    // parse lines
    linesParse.foreach({ line =>
      val variableList = this.lineParser(line)

      // append variables to the list
      variableList.foreach(variable => {
        varList.append(variable)
      })
    })

    // filter out duplicates
    varList = this.removeDuplicates(varList)

    varList
  }

  // fetch triples: WHERE clause
  def fetchWhereTriples(whereLines: ArrayBuffer[String], whereVariables: ArrayBuffer[String]): ArrayBuffer[String] = {
    var varList: ArrayBuffer[String] = ArrayBuffer()

    // iterate WHERE lines
    whereLines.foreach(whereLine => {
      if (!whereLine.isEmpty) {
        var line = whereLine

        // remove WHERE
        if (line.toUpperCase.startsWith("WHERE")) {
          line = line.substring(5).trim()
        }

        // remove "{"
        if (line.startsWith(this.symbol("curly-bracket-left"))) {
          line = line.substring(1).trim()
        }

        // remove "}"
        if (line.startsWith(this.symbol("curly-bracket-right"))) {
          line = line.substring(1).trim()
        }

        // left with triples
        if (line.length() > 0) {
          // remove spaces
          line = line.substring(0, line.length()).trim()

          // remove "." at the end of triple line
          if (line.substring(line.length() - 1).contains(this.symbol("dot"))) {
            line = line.substring(0, line.length() - 1).trim()
          }

          // append triple to the list
          varList += line
        }
      }
    })

    // filter out duplicates
    varList = this.removeDuplicates(varList)

    varList
  }

  // remove duplicates
  def removeDuplicates(list: ArrayBuffer[String]): ArrayBuffer[String] = {
    var varList: ArrayBuffer[String] = ArrayBuffer()

    // filter out duplicates from the list
    list.foreach(item => {
      if (!varList.contains(item)) {
        varList += item
      }
    })

    varList
  }

  // validate SELECT or FILTER clause variables
  def validateVariables(variables: ArrayBuffer[String], whereVariables: ArrayBuffer[String], qID: Int, validateType: String = "SELECT"): Unit = {
    variables.foreach(variable => {
      if (!whereVariables.contains(variable)) {
        throw new IllegalStateException(s"Query No. $qID: $validateType variables must be in WHERE clause: $variable")
      }
    })
  }

  // ---------------
  // Process Queries
  // ---------------

  // query engine
  def queryEngine(qID: Int): Unit = {
    // validate number of WHERE clause triples
    if (_numOfWhereClauseTriples(qID).equals(1)) {
      // process first triple
      this.runFirstTriple(qID)
    } else {
      // process all triples of a query
      this.runAllTriplesOfQuery(qID)
    }
  }

  // ---------------------------------------------------
  // Process Query: with only one triple in WHERE clause
  // ---------------------------------------------------

  // process first triple
  def runFirstTriple(qID: Int, clauseNum: Int = 0, isRemainingTriples: Boolean = false, varJoinList: ArrayBuffer[String] = null): Unit = {
    // number of WHERE clause triples
    val numOfWhereClauseTriples = _numOfWhereClauseTriples(qID)

    // fetch WHERE clause
    val triple = _WhereTriples(qID)(clauseNum)

    // fetch SUBJECT, PREDICATE and OBJECT
    val tripleData = fetchTripleSPO(triple, symbol)
    val tripleSubject = tripleData(0)
    val triplePredicate = tripleData(1)
    val tripleObject = tripleData(2)

    // case: FILTER
    var isFilter = false
    var filterLine = ""
    if (_filterOp(qID).nonEmpty) {
      isFilter = true
      filterLine = _filterOp(qID)
    }

    // process partition data
    val tmpRDD = partitionData
      .flatMap(line => {
        val lineArray = line.split(this.symbol("space"))
        val output = for (i <- 1 until (lineArray.length - 1)) yield {
          var line: String = ""

          // odd numbers: PREDICATE
          if (!(i % 2).equals(0)) {
            if (lineArray(i).equals(triplePredicate)) {
              line = lineArray(0) + this.symbol("space") + lineArray(i) + this.symbol("space") + lineArray(i + 1)
            }
          }

          line
        }

        output
      })
      .filter(_.nonEmpty)
      .map(line => {
        val splitPartitionData = line.split(this.symbol("space"))
        val i: Int = 0
        var key: String = ""
        var value: List[String] = List()

        // SUBJECT
        val lineSubject = splitPartitionData(i)

        // check: SUBJECT
        if (tripleSubject.startsWith(this.symbol("question-mark")) || tripleSubject.equals(lineSubject)) {
          // OBJECT
          val lineObject = splitPartitionData(i + 2)

          // check: OBJECT
          if (tripleObject.startsWith(this.symbol("question-mark")) || tripleObject.equals(lineObject)) {
            if (numOfWhereClauseTriples.equals(1) || !isRemainingTriples) {
              if (numOfWhereClauseTriples.equals(1)) {
                // set output: query with only one WHERE clause triple
                key = this.setOnlyTripleOutput(
                  qID,
                  tripleSubject,
                  tripleObject,
                  lineSubject,
                  lineObject)

                // case: FILTER
                if (isFilter) {
                  value = List(
                    this.setFirstTripleOutput(
                      tripleSubject,
                      tripleObject,
                      lineSubject,
                      lineObject))
                } else value = List()
              } else {
                // set output: first triple of multi WHERE clause triples
                key = this.setFirstTripleOutput(
                  tripleSubject,
                  tripleObject,
                  lineSubject,
                  lineObject)

                value = List()
              }
            } else {
              // set output: query with more than one WHERE clause triples
              val keyValue = this.setRemainingTriplesOutput(
                tripleSubject,
                tripleObject,
                lineSubject,
                lineObject,
                varJoinList)

              // assign values
              for (k <- keyValue.keySet().asScala) {
                key = k
                value = keyValue.get(k).asScala.toList
              }
            }
          }
        }

        // (K, V) pair
        (key, value)
      })
      .filter(_._1.nonEmpty)

    // validate rdd
    if (tmpRDD.partitions.nonEmpty) {
      // only one WHERE clause triple query
      if (numOfWhereClauseTriples.equals(1)) {
        outputRDD = tmpRDD
          .filter(data => {
            var status = false

            // case: FILTER
            if (isFilter) {
              val (result, _) = this.applyFilter(filterLine, data._2.head)
              if (result) status = true
            } else status = true

            status
          })
          .map(data => {
            var line = data._1 // key is the output in case of just one triple query

            // case: FILTER
            // case: a || b and both values are true. show result two times
            if (isFilter) {
              val (_, duplicateCounter) = this.applyFilter(filterLine, data._2.head)
              if (duplicateCounter >= 1) {
                val cline = line
                for (_ <- 1 to duplicateCounter) {
                  line += this.symbol("newline") + cline
                }
              }
            }

            line
          })

        // case: UNION
        if (_unionOp(qID)("isUnion")) {
          if (_unionOp(qID)("first")) unionOutputRDD = outputRDD
          else unionOutputRDD = unionOutputRDD.union(outputRDD)
        }

        // display output
        if (_unionOp(qID)("last")) {

          // case: UNION
          if (_unionOp(qID)("isUnion")) {
            outputRDD = unionOutputRDD
          }

          // case: LIMIT
          if (_queriesLimit.get(qID).isDefined) {
            outputRDD = outputRDD
              .mapPartitions(_.take(_queriesLimit(qID)))
          } else {
            outputRDD = outputRDD
          }
        }
      }

      // multi WHERE clause triples query: set RDD
      if (numOfWhereClauseTriples > 1) {
        if (!isRemainingTriples) {
          workingPartialRDD = tmpRDD
        } else {
          workingTripleRDD = tmpRDD
        }
      }
    }
  }

  // set output: query with only one WHERE clause triple
  def setOnlyTripleOutput(qID: Int, tripleSubject: String, tripleObject: String, lineSubject: String, lineObject: String): String = {
    var key: String = ""

    // set output result
    _selectVariables(qID).foreach(selectVariable => {
      // case: SELECT *
      if (!selectVariable.equals(this.symbol("asterisk"))) {
        // equal variables: SUBJECT and OBJECT with SELECT variables
        if (tripleSubject.equals(selectVariable) && tripleObject.equals(selectVariable)) {
          // equal variables: SUBJECT and OBJECT
          if (tripleSubject.equals(tripleObject)) {
            val subjectURI = new String(lineSubject.getBytes(), 0, lineSubject.getBytes().length)
            if (key.nonEmpty) {
              key += this.symbol("space") + subjectURI
            } else {
              key = subjectURI
            }
          }
        } else {
          // equal variable: SUBJECT with SELECT variables
          if (tripleSubject.equals(selectVariable)) {
            val subjectURI = new String(lineSubject.getBytes(), 0, lineSubject.getBytes().length)
            if (key.nonEmpty) {
              key += this.symbol("space") + subjectURI
            } else {
              key = subjectURI
            }
          } else {
            // equal variable: OBJECT with SELECT variables
            if (tripleObject.equals(selectVariable)) {
              val objectURI = new String(lineObject.getBytes(), 0, lineObject.getBytes().length)
              if (key.nonEmpty) {
                key += this.symbol("space") + objectURI
              } else {
                key = objectURI
              }
            }
          }
        }
      } else {
        if (tripleSubject.contains(this.symbol("question-mark"))) {
          val subjectURI = new String(lineSubject.getBytes(), 0, lineSubject.getBytes().length)
          key = subjectURI
        }
        if (tripleObject.contains(this.symbol("question-mark"))) {
          val objectURI = new String(lineObject.getBytes(), 0, lineObject.getBytes().length)
          key += this.symbol("space") + objectURI
        }
      }
    })

    key
  }

  // ---------------------------------------------------------
  // Process Query: with only multiple triples in WHERE clause
  // ---------------------------------------------------------

  // process all triples of a query
  def runAllTriplesOfQuery(qID: Int): Unit = {
    // process first triple
    this.runFirstTriple(qID)

    // iterate the remaining clauses
    for (i <- 1 until _WhereTriples(qID).length) {
      val clauseNum = i

      // fetch variable join list
      val varJoinList: ArrayBuffer[String] = fetchVarJoinList(qID, clauseNum)

      // process remaining triples
      this.runRemainingTriples(qID, clauseNum, varJoinList)
    }

    // display multi triples output
    this.displayMultiTriplesOutput(_selectVariables(qID), qID)
  }

  // set output: query with more than one WHERE clause triples
  def setFirstTripleOutput(tripleSubject: String, tripleObject: String, lineSubject: String, lineObject: String): String = {
    var key: String = ""

    // equal variables: SUBJECT and OBJECT with "?"
    if (tripleSubject.startsWith(this.symbol("question-mark")) && tripleObject.startsWith(this.symbol("question-mark"))) {
      if (tripleSubject.equals(tripleObject)) {
        key = tripleSubject + this.symbol("blank") + lineSubject
      } else {
        key = tripleSubject + this.symbol("blank") + lineSubject + this.symbol("blank") + tripleObject + this.symbol("blank") + lineObject
      }
    } else {
      // equal variable: SUBJECT with "?"
      if (tripleSubject.startsWith(this.symbol("question-mark"))) {
        key = tripleSubject + this.symbol("blank") + lineSubject
      }

      // equal variable: OBJECT with "?"
      if (tripleObject.startsWith(this.symbol("question-mark"))) {
        key = tripleObject + this.symbol("blank") + lineObject
      }
    }

    key
  }

  // fetch variable join list
  def fetchVarJoinList(qID: Int, clauseNum: Int): ArrayBuffer[String] = {
    val foundVarListFromTriples: ArrayBuffer[String] = ArrayBuffer()
    val varJoinList: ArrayBuffer[String] = ArrayBuffer()

    // fetch variables from triples (until processing triple)
    for (i <- 0 until clauseNum) {
      val triple = _WhereTriples(qID)(i)

      // fetch SUBJECT and OBJECT
      val tripleData = fetchTripleSPO(triple, symbol)
      val tripleSubject = tripleData(0)
      val tripleObject = tripleData(2)

      // SUBJECT
      if (tripleSubject.startsWith(this.symbol("question-mark"))) {
        if (!foundVarListFromTriples.contains(tripleSubject)) {
          foundVarListFromTriples.append(tripleSubject)
        }
      }

      // OBJECT
      if (tripleObject.startsWith(this.symbol("question-mark"))) {
        if (!foundVarListFromTriples.contains(tripleObject)) {
          foundVarListFromTriples.append(tripleObject)
        }
      }
    }

    // current triple
    val triple = _WhereTriples(qID)(clauseNum)

    // fetch SUBJECT and OBJECT
    val tripleData = fetchTripleSPO(triple, symbol)
    val tripleSubject = tripleData(0)
    val tripleObject = tripleData(2)

    // SUBJECT
    if (tripleSubject.startsWith(this.symbol("question-mark"))) {
      if (foundVarListFromTriples.contains(tripleSubject)) {
        varJoinList.append(tripleSubject)
      }
    }

    // OBJECT
    if (tripleObject.startsWith(this.symbol("question-mark"))) {
      if (foundVarListFromTriples.contains(tripleObject)) {
        if (!varJoinList.contains(tripleObject)) {
          varJoinList.append(tripleObject)
        }
      }
    }

    varJoinList
  }

  // process remaining triples
  def runRemainingTriples(qID: Int, clauseNum: Int, varJoinList: ArrayBuffer[String]): Unit = {
    // process remaining triple
    this.runFirstTriple(qID, clauseNum = clauseNum, isRemainingTriples = true, varJoinList = varJoinList)

    // set output: work on processed triples
    this.setRemainingPartialOutput(varJoinList)

    // join two RDD
    val tempRDD: RDD[(String, (List[String], List[String]))] = workingTripleRDD.join(workingPartialRDD)

    // iterate output keys
    workingPartialRDD = tempRDD
      .flatMap(line => {
        val key2 = line._1
        val valuesList = line._2._1 ::: line._2._2 // concatenate two Lists
        val triplesList: ArrayBuffer[String] = ArrayBuffer()
        val partialsList: ArrayBuffer[String] = ArrayBuffer()

        // iterate key values
        valuesList.foreach(value => {
          val str = value.substring(0, 1)

          // check triple
          if (str.equals("t")) {
            triplesList.append(value)
          } else {
            partialsList.append(value)
          }
        })

        // set output
        val output = for {
          i <- triplesList.indices
          j <- partialsList.indices
        } yield {
          val key1 = partialsList(j).substring(1) + this.symbol("blank") + triplesList(i).substring(1)
          val key = key1 + this.symbol("blank") + key2

          // (K, V) pair
          (key, null)
        }
        output
      })
  }

  // set output: work on processed triples
  def setRemainingPartialOutput(varJoinList: ArrayBuffer[String]): Unit = {
    workingPartialRDD = workingPartialRDD
      .map(line => {
        var key: String = ""
        var value: String = "p"
        val processLine = line._1
        val itr = new StringTokenizer(processLine)

        while (itr.hasMoreTokens) {
          val variable = itr.nextToken

          // check variable in variable join list
          if (varJoinList.contains(variable)) {
            if (!key.length().equals(0)) {
              key = key + this.symbol("blank")
            }

            key = key + variable + this.symbol("blank") + itr.nextToken
          } else {
            if (!value.length().equals(0)) {
              value = value + this.symbol("blank")
            }

            value = value + this.symbol("blank") + variable + this.symbol("blank") + itr.nextToken
          }
        }

        // (K, V) pair
        (key, List(value))
      })
  }

  // set output: query with more than one WHERE clause triples
  def setRemainingTriplesOutput(tripleSubject: String, tripleObject: String, lineSubject: String, lineObject: String, varJoinList: ArrayBuffer[String]): ArrayListMultimap[String, String] = {
    val keyValue: ArrayListMultimap[String, String] = ArrayListMultimap.create[String, String]()

    // equal variables: SUBJECT and OBJECT with "?"
    if (tripleSubject.startsWith(this.symbol("question-mark")) && tripleObject.startsWith(this.symbol("question-mark"))) {
      if (tripleSubject.equals(tripleObject)) {
        keyValue.put(tripleSubject + this.symbol("blank") + lineSubject, "t")
      } else {
        if (varJoinList.size > 1) {
          keyValue.put(tripleSubject + this.symbol("blank") + lineSubject + this.symbol("blank") + tripleObject + this.symbol("blank") + lineObject, "t")
        } else {
          if (varJoinList(0).equals(tripleSubject)) {
            keyValue.put(tripleSubject + this.symbol("blank") + lineSubject, "t " + tripleObject + this.symbol("blank") + lineObject)
          } else {
            keyValue.put(tripleObject + this.symbol("blank") + lineObject, "t " + tripleSubject + this.symbol("blank") + lineSubject)
          }
        }
      }
    } else {
      // equal variable: SUBJECT with "?"
      if (tripleSubject.startsWith(this.symbol("question-mark"))) {
        keyValue.put(tripleSubject + this.symbol("blank") + lineSubject, "t")
      }

      // equal variable: OBJECT with "?"
      if (tripleObject.startsWith(this.symbol("question-mark"))) {
        keyValue.put(tripleObject + this.symbol("blank") + lineObject, "t")
      }
    }

    keyValue
  }

  // display multi triples output
  def displayMultiTriplesOutput(selectVariables: ArrayBuffer[String], qID: Int): Unit = {
    // case: FILTER
    var isFilter = false
    var filterLine = ""
    if (_filterOp(qID).nonEmpty) {
      isFilter = true
      filterLine = _filterOp(qID)
    }

    // output RDD
    var tmpRDD = workingPartialRDD
      .filter(line => {
        val processLine = line._1
        var isFound = false
        var status = true

        // case: FILTER
        if (isFilter) {
          val (result, duplicateCounter) = this.applyFilter(filterLine, processLine)
          if (result || duplicateCounter >= 1) isFound = true

          // filter status: false
          if (!isFound) status = false
        }

        status
      })
      .map(line => {
        val processLine = line._1
        val itr = new StringTokenizer(processLine)
        var outputResult: String = ""

        // check tokens
        while (itr.hasMoreTokens) {
          val variable = itr.nextToken

          // if variable exists in SELECT variables
          if (selectVariables.contains(variable) || selectVariables.contains(this.symbol("asterisk"))) {
            val next = itr.nextToken()

            // add space
            if (outputResult.length() > 0) {
              outputResult = outputResult.concat(this.symbol("space"))
            }

            // append value
            outputResult = outputResult.concat(next)
          } else itr.nextToken()
        }

        // case: FILTER
        // case: a || b and both values are true. show result two times
        if (isFilter) {
          val (_, duplicateCounter) = this.applyFilter(filterLine, processLine)
          if (duplicateCounter >= 1) {
            val line = outputResult
            for (_ <- 1 to duplicateCounter) {
              outputResult += this.symbol("newline") + line
            }
          }
        }

        outputResult
      })

    if (tmpRDD.partitions.nonEmpty) {
      // case: UNION
      if (_unionOp(qID)("isUnion")) {
        if (_unionOp(qID)("first")) {
          unionOutputRDD = tmpRDD
        } else {
          unionOutputRDD = unionOutputRDD.union(tmpRDD)
        }
      }

      // output result to file
      if (_unionOp(qID)("last")) {

        // case: UNION
        if (_unionOp(qID)("isUnion")) {
          tmpRDD = unionOutputRDD
        }

        // case: LIMIT
        if (_queriesLimit.get(qID).isDefined) {
          tmpRDD = tmpRDD
            .mapPartitions(_.take(_queriesLimit(qID)))
        } else {
          tmpRDD = tmpRDD
        }
      }
    }
    outputRDD = tmpRDD
  }

  // apply FILTER on the query
  def applyFilter(filterLine: String, processLine: String): (Boolean, Int) = {
    val filterLineSplit1: Array[String] = filterLine.split("&&|\\|\\|")
    val filterLineSplit2: Array[String] = filterLine.split(this.symbol("blank"))
    var resultList: ArrayBuffer[Boolean] = ArrayBuffer()
    var logicalOpList: ArrayBuffer[String] = ArrayBuffer()
    var result: Boolean = false
    var duplicateCounter: Int = 0

    // store logical operators: && ||
    for (i <- filterLineSplit2.indices) {
      if (filterLineSplit2(i).matches("&&|\\|\\|")) {
        logicalOpList += filterLineSplit2(i)
      }
    }

    // FILTER logic
    if (filterLineSplit1.nonEmpty) {
      for (i <- filterLineSplit1.indices) {
        val lineSplit = filterLineSplit1(i).trim.split(this.symbol("blank"))
        // valid case: 1, 3, 5, 7, 9, 11, 13 ... (odd numbers)
        if (lineSplit.length.equals(1) || lineSplit.length.equals(3)) {
          // FILTER comparison
          if (lineSplit.length.equals(3)) {
            var content = ""

            // case: lang
            if (lineSplit(0).startsWith("lang") || lineSplit(0).startsWith("datatype")) {
              var index = 0
              if (lineSplit(0).startsWith("lang")) {
                index = 5
              } else {
                index = 9
              }

              content = lineSplit(0).substring(index, lineSplit(0).length - 1)
            } else content = lineSplit(0)

            // validate variable
            if (!content.startsWith(this.symbol("question-mark"))) {
              throw new IllegalStateException(s"FILTER - Wrong Variable: $filterLine")
            } else {
              // validate comparison area
              if (lineSplit(1).nonEmpty) {
                var a = content
                var b = lineSplit(2)
                val operator = lineSplit(1)

                // fetch value
                var locationPoint = processLine.indexOf(a)
                val value = processLine.substring(locationPoint + a.length + 1)
                locationPoint = value.indexOf(this.symbol("blank"))
                if (locationPoint.equals(-1)) {
                  a = value
                } else {
                  a = value.substring(0, locationPoint)
                }

                // case: lang
                if (lineSplit(0).startsWith("lang")) {
                  if (a.contains(this.symbol("at")) && !a.contains("http")) {
                    val data = a.split(this.symbol("at"))
                    if (data.nonEmpty && data(1).nonEmpty && data(1).length.equals(2)) {
                      a = data(1).toLowerCase
                    }
                  }

                  // "ES" to es
                  b = lineSplit(2).substring(1, lineSplit(2).length - 1).toLowerCase
                }

                // case: datatype
                if (lineSplit(0).startsWith("datatype")) {
                  if (a.contains(this.symbol("up-arrows")) && a.contains(this.symbol("hash"))) {
                    val data = a.split(this.symbol("hash"))
                    if (data.nonEmpty && data(1).nonEmpty) {
                      a = data(1)
                    }
                  }
                }

                // call comparison function
                resultList += this.filterComparison(a, b, operator)
              } else throw new IllegalStateException(s"FILTER - Empty Operator: $filterLine")
            }
          } else { // FILTER functions
            // validate filter area
            if (lineSplit(0).nonEmpty &&
              lineSplit(0).contains(this.symbol("round-bracket-left")) &&
              lineSplit(0).endsWith(this.symbol("round-bracket-right"))) {
              val filterFunction = lineSplit(0)
              val locationPoint = filterFunction.indexOf(this.symbol("round-bracket-left"))
              var fName = filterFunction.substring(0, locationPoint)
              var isNot = false

              // case: ! operator
              if (filterFunction.contains(this.symbol("exclamation-mark"))) {
                fName = filterFunction.substring(1, locationPoint)
                isNot = true
              }

              // call FILTER function
              val output = this.filterFunctions(fName, filterFunction, processLine)

              // case: ! operator
              if (isNot) {
                resultList += !output
              } else {
                resultList += output
              }
            } else throw new IllegalStateException(s"FILTER - Wrong Function: $filterLine")
          }
        } else throw new IllegalStateException(s"FILTER - Wrong Statement: $filterLine")
      }

      // evaluate result
      if (logicalOpList.nonEmpty && resultList.length > logicalOpList.length) {
        for (i <- logicalOpList.indices) {
          if (i.equals(0)) {
            if (logicalOpList(0).equals("&&")) {
              result = resultList(i) && resultList(i + 1)
            } else {
              if (resultList(i) && resultList(i + 1)) {
                result = resultList(i) || resultList(i + 1)
                duplicateCounter += 1
              } else result = resultList(i) || resultList(i + 1)
            }
          } else {
            if (logicalOpList(i).equals("&&")) {
              result = result && (resultList(i) && resultList(i + 1))
            } else {
              if (result && resultList(i) && resultList(i + 1)) {
                result = result || (resultList(i) || resultList(i + 1))
                duplicateCounter += 1
              } else result = result || (resultList(i) || resultList(i + 1))
            }
          }
        }
      } else if (resultList.nonEmpty) result = resultList(0)
      else result = true
    }

    (result, duplicateCounter)
  }

  // FILTER comparison
  def filterComparison(a: String, b: String, operator: String): Boolean = {
    val result: Boolean = operator match {
      case "<" => a < b
      case ">" => a > b
      case "=" | "==" => a.equals(b)
      case ">=" => a > b || a.equals(b)
      case "<=" => a < b || a.equals(b)
      case "!=" => !a.equals(b)
      case _ => throw new IllegalStateException(s"FILTER - Wrong Operator Found: $operator")
    }

    result
  }

  // FILTER functions
  def filterFunctions(fName: String, filterFunction: String, processLine: String): Boolean = {
    var bool = false
    val data = fetchFilterFunctionData(fName, filterFunction, processLine, symbol)
    val variable = data(0)
    val value = data(1)

    val result: Boolean = fName match {
      case "isURI" =>
        if (value.startsWith(symbol("less-than")) &&
          value.endsWith(symbol("greater-than")) &&
          value.contains("http")) { bool = true }

        bool
      case "isBlank" =>
        if (variable.startsWith("_:")) bool = true

        bool
      case "isLiteral" =>
        if (value.contains(this.symbol("up-arrows"))) bool = true

        bool
      case _ => throw new IllegalStateException(s"FILTER - Wrong functions found: $fName")
    }

    result
  }
}
