package net.sansa_stack.query.spark.dof.tensor

import net.sansa_stack.query.spark.dof.bindings._
import net.sansa_stack.query.spark.dof.node._
import net.sansa_stack.query.spark.dof.triple.Reader
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.sparql.core.Var
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object NSetAccumulatorParam extends AccumulatorParam[Set[Node]] {
  def zero(initialValue: Set[Node]): Set[Node] = Set[Node]()

  def addInPlace(s1: Set[Node], s2: Set[Node]): Set[Node] = s1 ++ s2
}

object RDDTensor {
  def apply(spark: SparkSession, reader: Reader):
    RDDTensor = new RDDTensor(spark, reader)
}

class RDDTensor(spark: SparkSession, reader: Reader)
  extends Tensor[RDD[Triple], String, RDD[Seq[Long]], ResultRDD[String]](spark, reader) {

  def readData: RDD[Triple] = reader.read

  def buildTensor: RDD[Seq[Long]] = {
    // 1. object
    // Count shows that distinct() on RDD[Nodes] looses entity.
    // distinct() on RDD[String] does not
    // println(data.count)
    val dataO = data.map(t => (t.getObject.toString(), (t.getSubject.toString(), t.getPredicate.toString())))
    val jointO = dataO.join(spo.getObject.rddStr)
    // println(jointO.count)
    // 2. subjects TODO: keep indexes with values?
    val dataS = jointO.map(t => (t._2._1._1, (t._2._1._2, t._2._2)))
    val jointS = dataS.join(spo.getSubject.rddStr)
    // println(jointS.count)
    // 3. predicate TODO: keep indexes with values?
    val dataP = jointS.map(t => (t._2._1._1, (t._2._2, t._2._1._2)))
    val jointP = dataP.join(spo.getPredicate.rddStr)
    // println(jointP.count)
    // add indexes and join them as last column in tensor
    // 17*10^6 indexes - do we really need all of them?
    // val tensor = jointP.zipWithIndex.map(t => Seq(t._1._2._2, t._1._2._1._1, t._1._2._1._2, t._2))
    val tensor = jointP.map(t => Seq(t._2._2, t._2._1._1, t._2._1._2))

    tensor
  }

  def buildSPO: SPO[Node] = {
    var seq = Seq.empty[RDD[Node]]
    Helper.getNodeMethods.foreach(func => {
      seq = seq :+ data.map(Helper.getNode(_, func))
    })
    new SPO(seq)
  }

  def getSpoField(method: String): NodeIndexed[Node] =
    spo.getClass.getMethod(method).invoke(spo).asInstanceOf[NodeIndexed[Node]]

  def transform(tensor: RDD[Seq[Long]], index: Int):
    RDD[(Long, Seq[Long])] = tensor.map(row => (row(index), row))

  /*
   * Build tensor traversing on triple variables/constants
   */
  def traverse(triple: Triple, mapV: VariableMap[String]): RDD[Seq[Long]] = {
    var traversed = tensor
    Helper.nodeMethodsZip.foreach {
      case (method, index) =>
        val node = Helper.getNode(triple, method)
        val nodeS = node.toString
        // Helper.log(method + " " + index + " " + node)
        // println(node)
        val entity = getSpoField(method)
        var rdd = sparkContext.emptyRDD[(Long, String)]

        if (node.isVariable) { // get values from mapV if they exist
          var values = mapV.get(node)
          if (mapV.isEmpty(values)) { // if values do not exist, use the whole corresp.entity for ?X
            rdd = entity.reverserddStr
          } else { // otherwise we have to map values to its indexes from the entity
            rdd = entity.rddStr.join(values.map(v => (v, v))).map(f => f._2)
          }
        } else { // not variable, then filter indexes from the corresp. column in spo
          rdd = entity.reverserddStr.filter(item => item._2 == nodeS)
        }

        // rdd.foreach(l => Helper.log(l.toString))
        // use row(index) as key to apply join()
        traversed = transform(traversed, index).join(rdd).map(row => row._2._1)
        // temp.foreach(Helper.log(_))
    }
    // traversed.foreach(println)
    traversed
  }

  /*
   * Update mapV with RDD from tensor values for triple variables
   */
  def updateMapV(triple: Triple, mapV: VariableMap[String], tensor: RDD[Seq[Long]]): Unit = {
    Helper.nodeMethodsZip.foreach {
      case (method, index) =>
        val node = Helper.getNode(triple, method)
        if (node.isVariable) {
          val indexes = tensor.map(row => { val r = row(index); (r, r) })
          val values = getSpoField(method).reverserddStr.join(indexes).map(v => v._2._1)
          mapV.update(node, values.distinct) // distinct() used because of problems with query #6,7 etc.
        }
    }
  }

  /*
   * Join obtained result with previous ones
   */
  def mapTensorIndexesToNodes(triple: Triple, tensor: RDD[Seq[Long]]): RDD[VarNodeMap[String]] = {
    var result: RDD[(Long, VarNodeMap[String])] = null
    val tensorZip = tensor.zipWithIndex()
    Helper.nodeMethodsZip.foreach {
      case (method, index) =>
        val node = Helper.getNode(triple, method)
        if (node.isVariable) {
          val v = node.asInstanceOf[Var]
          val rdd = getSpoField(method).reverserddStr // get corresp rdd
          val temp = tensorZip.map(row => (row._1(index), row))
            .join(rdd) // map tensor index to nodes for corresp column
            .map(f => (f._2._1._2, Map[Var, String]() + (v -> f._2._2)))
          // temp.foreach(f=>Helper.log(f._2.toString))
          if (result == null) { // if nothing done yet, create a map
            result = temp
          } else { // otherwise add to an existing map
            result = result
              .join(temp)
              .map(item => (item._1, item._2._1 ++ item._2._2))
          }
        }
    }
    // result.foreach(f=>Helper.log(f.toString))
    result.map(f => f._2)
  }

  def process(triple: Triple, mapV: VariableMap[String]): RDD[VarNodeMap[String]] = {
    val tensor = traverse(triple, mapV)
    updateMapV(triple, mapV, tensor)
    val result = mapTensorIndexesToNodes(triple, tensor)

    result
  }

  def mapWithKeys(result: Result[ResultRDD[String]], keys: List[Var]):
    RDD[(List[String], VarNodeMap[String])] = result.rdd.map(item => (keys.map(key => item(key)), item))

  def saveResult(
    triple: Triple,
    result: Result[ResultRDD[String]],
    current: ResultRDD[String]): Result[ResultRDD[String]] = {
    val currentKeys = Case.getEmptyRowVarMap(triple).keys.toList
    return saveResult(result, new Result(current, currentKeys))
  }

  def saveResult(
    result: Result[ResultRDD[String]],
    current: Result[ResultRDD[String]]): Result[ResultRDD[String]] = {
    if (result == null) return current

    val keysIntersection = result.keys.intersect(current.keys)
    val resultItems = mapWithKeys(result, keysIntersection)
    val currentItems = mapWithKeys(current, keysIntersection)
    return new Result(
      resultItems.join(currentItems).map(item => item._2._1 ++ item._2._2),
      result.keys.toSet.union(current.keys.toSet).toList)
  }

  /*
   * transform from RDD[Map[Var,Node)] to RDD[(List(Keys), Set(Var))]
   * to use for further union by the same keys
   * Use List[Key] instead of Set[Key] to have the same order always
   * */
  def transform(result: Result[ResultRDD[String]], keysI: List[Var], keysU: List[Var]):
    RDD[(List[String], List[String])] = result.rdd.map(row => {
      // values that exist for keys intersection
      val valuesI = keysI.flatMap(key => {
        if (row.keySet.exists(_ == key)) {
          Some(row(key))
        } else {
          None
        }
      })
      (valuesI, row.values.toList.diff(valuesI))
    })

  def unionResult(
    result: Result[ResultRDD[String]],
    current: Result[ResultRDD[String]]): Result[ResultRDD[String]] = {
    if (result == null) return current
    val temp = new Result(result.rdd.union(current.rdd).distinct, result.keys.toSet.union(current.keys.toSet).toList)
    temp
  }

  def getEmptyRDD: RDD[String] = spark.sparkContext.emptyRDD

  def output(result: Result[ResultRDD[String]], resultVars: List[Var]):
    Array[String] = result.rdd
      .collect // collect only if test failed since it is a slow operation, the same as take, first, reduce
      .map(map => {
        resultVars.flatMap((v: Var) => {
          if (map.keySet.exists(_ == v)) {
            Some(map(v))
          } else {
            None
          }
        })
      })
      .map(_.mkString(" "))
      .sorted

  def compareResult(
    result: Result[ResultRDD[String]],
    expected: RDD[String],
    resultVars: List[Var]): Boolean = {
    // return true
    var start = Helper.start
    val rddString = result.rdd.map(row => {
      val resultRow = resultVars.flatMap((v: Var) => {
        if (row.keySet.exists(_ == v)) {
          Some(row(v))
        } else {
          None
        }
      })
      resultRow.mkString(" ")
    })

    val isEqual = isRDDEquals(expected, rddString)
    Helper.measureTime(start, s"\nResult vs. expected time=")
    isEqual
  }
}
