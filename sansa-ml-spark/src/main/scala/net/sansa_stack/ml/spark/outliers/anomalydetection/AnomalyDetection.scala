package net.sansa_stack.ml.spark.outliers.anomalydetection



import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import scala.collection.mutable
import scala.collection.mutable.HashSet
import org.apache.jena.graph.NodeFactory
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd._
/*
 *
 * AnomalyDetection - Anomaly detection of numerical data
 * @objList - list of numerical type.
 * @triplesType - List of rdf type's objects.
 * @return - cluster of similar subjects.
 */

class AnomalyDetection(nTriplesRDD: RDD[Triple], objList: List[String],
                       triplesType: List[String], JSimThreshold: Double,
                       listSuperType: List[String], sparkSession: SparkSession, hypernym: String, numPartition: Int) extends Serializable {
  def run(): RDD[(String, Set[(String, String, Object)])] = {

    // get all the triples whose objects are literal 
    //these literals also contains xsd:date as well as xsd:langstring 
    val getObjectLiteral = getObjectList()

    //remove the literal which has ^^xsd:date or xsd:langstring(only considering numerical)
    val removedLangString = getObjectLiteral.filter(f => searchedge(f.getObject.toString(), objList))

    //checking still object has only numerical data only
    val triplesWithNumericLiteral = triplesWithNumericLit(removedLangString)

    val mapSubWithTriples = propClustering(triplesWithNumericLiteral) //.persist

    //get triples of hypernym
    val getHypernym = getHyp()

    //filter rdf type having object value dbpedia and join with hyernym
    val rdfTypeDBwiki = rdfType(getHypernym) //.partitionBy(new HashPartitioner(2)).persist()

    //joining those subjects only who has rdf:ytpe and numerical literal 
    val rdfTypeWithSubject = mapSubWithTriples.join(rdfTypeDBwiki)
    val mapSubjectwithType = rdfTypeWithSubject.map(f => (f._1, f._2._2))
    val propwithSub = propwithsubject(triplesWithNumericLiteral)
    //cluster subjects on the basis of rdf type
    val jacardSimilarity = jSimilarity(triplesWithNumericLiteral, mapSubjectwithType, propwithSub, mapSubWithTriples)

    jacardSimilarity

  }

  def getHyp(): RDD[Triple] = nTriplesRDD.filter(f => f.getPredicate.toString().equals(hypernym))

  def getObjectList(): RDD[Triple] = nTriplesRDD.filter(f => f.getObject.isLiteral())

  def triplesWithNumericLit(objLit: RDD[Triple]): RDD[Triple] = objLit.filter(f => isNumeric(f.getObject.toString()))

  def propwithsubject(a: RDD[Triple]): RDD[(String, String)] = a.map(f => (getLocalName1(f.getSubject), getLocalName1(f.getPredicate)))
  def isNumeric(x: String): Boolean =
    {
      if (x.contains("^")) {
        val c = x.indexOf('^')
        val subject = x.substring(1, c - 1)

        if (isAllDigits(subject))
          true
        else
          false
      } else
        false
    }

  def isAllDigits(x: String): Boolean = {
    var found = false
    for (ch <- x) {
      if (ch.isDigit || ch == '.')
        found = true
      else if (ch.isLetter) {

        found = false
      }
    }

    found
  }

  def searchedge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      y.contains(subject)
    } else
      true
  }

  def rdfType(getHypernym: RDD[Triple]): RDD[(String, HashSet[String])] = {

    //filter triples with predicate as rdf:type
    val triplesWithRDFType = nTriplesRDD.filter(_.getPredicate.toString() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    val triplesWithDBpedia = triplesWithRDFType.filter(f => searchType(f.getObject.toString(), triplesType))

    val subWithType1 = triplesWithDBpedia.map(f =>
      // ...
      (getLocalName1(f.getSubject), getLocalName1(f.getObject))) //.partitionBy(new HashPartitioner(8)).persist()

    val initialSet1 = mutable.HashSet.empty[String]
    val addToSet1 = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets1 = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    val uniqueByKey1 = subWithType1.aggregateByKey(initialSet1)(addToSet1, mergePartitionSets1)

    val hyper1 = getHypernym.map(f =>
      (getLocalName1(f.getSubject), (getLocalName1(f.getObject) + ("hypernym")))) //.partitionBy(new HashPartitioner(8)).persist

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    val uniqueByKey = hyper1.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val joinOp = uniqueByKey union uniqueByKey1
    val initialSet2 = mutable.HashSet.empty[HashSet[String]]
    val addToSet2 = (s: mutable.HashSet[HashSet[String]], v: HashSet[String]) => s += v
    val mergePartitionSets2 = (p1: mutable.HashSet[HashSet[String]], p2: mutable.HashSet[HashSet[String]]) => p1 ++= p2
    val uniqueByKey2 = joinOp.aggregateByKey(initialSet2)(addToSet2, mergePartitionSets2)
    val Joinopgroup2 = uniqueByKey2.map(f => (f._1, f._2.flatMap(f => f)))

    Joinopgroup2

  }
  def getLocalName1(x: Node): String = {
    var a = x.toString().lastIndexOf("/")
    val b = x.toString().substring(a + 1)
    b
  }
  def searchType(x: String, y: List[String]): Boolean = {
    if (y.exists(x.contains)) {
      true
    } else
      false
  }
  implicit class Crossable[X](xs: Iterator[(String, (String, HashSet[String]))]) {
    def cross[Y](ys: Iterator[(String, (String, HashSet[String]))]) = for { x <- xs; y <- ys } yield (x, y)
  }
  def jSimilarity(TriplesWithNumericLiteral: RDD[Triple],
                  rdfTypeDBwiki: RDD[(String, HashSet[String])], propwithSubject: RDD[(String, String)], mapSubWithTriples: RDD[(String, mutable.Set[(String, String, Object)])]): RDD[(String, Set[(String, String, Object)])] = {

    nTriplesRDD.unpersist()
    val partitionedy = rdfTypeDBwiki.persist
    val broadcastVar = sparkSession.sparkContext.broadcast(partitionedy.collect()) //for 16.6 GB data, collect is 3.5 GB
    val joined = partitionedy.mapPartitions({ iter =>

      val y = broadcastVar.value
  
      for {
        x <- iter
        z <- y
        if x._1.toString() != z._1.toString()
      } yield (x, z)
    })

    partitionedy.unpersist()
    val x = joined.mapPartitions({ iter =>
      val y = iter.map(f => (f._1._1, (f._2._1, sim(f._1._2, f._2._2))))
      val z = y.filter(f => f._2._2 > JSimThreshold).map(f => (f._1, f._2._1))
      z
    })

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    val mergePartitionSets3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val uniqueByKey3 = x.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)

    val k = uniqueByKey3.map(f => ((f._2 += (f._1)).toSet)).map(a => (a, a))
      .aggregateByKey(Set[String]())((x, y) => y, (x, y) => x)
      .keys.distinct()

    val abc = k.repartition(40).persist()
    val simSubjectCart = abc.cartesian(abc).filter(f => f._1.intersect(f._2).size > 0)

    val subsetMembers = simSubjectCart.filter { case (set1, set2) => (set2.subsetOf(set1)) && (set1 -- set2).nonEmpty }
    val sdf = subsetMembers.map(f => (f._2))
    val superset1 = k.subtract(sdf)

    val ys = superset1.flatMap(f => (f.map(g => (g, f))))

    val smallLookup = sparkSession.sparkContext.broadcast(ys.collect.toMap)
    val joinSimSubTriples1 = mapSubWithTriples.flatMap {
      case (key, value) =>
        smallLookup.value.get(key).map { otherValue =>
          (key, (otherValue, value))
        }
    }
    //    val g=ys.join(mapSubWithTriples)

    val clusterOfSubjects = joinSimSubTriples1.map({
      case (s, (iter, iter1)) => ((iter).toSet, iter1)
    })

    val initialSet = mutable.HashSet.empty[mutable.Set[(String, String, Object)]]
    val addToSet = (s: mutable.HashSet[mutable.Set[(String, String, Object)]], v: mutable.Set[(String, String, Object)]) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[mutable.Set[(String, String, Object)]], p2: mutable.HashSet[mutable.Set[(String, String, Object)]]) => p1 ++= p2
    val uniqueByKey = clusterOfSubjects.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

   
    val propCluster = uniqueByKey.map({
      case (a, (iter)) => (iter.flatMap(f => f.map(f => f._2)).toSet, ((iter)))
    })

    val propDistinct = propCluster.flatMap {
      case (a, ((iter))) => a.map(f => (f, ((iter.flatMap(f => f).toSet))))
    }

    val clusterOfProp = propDistinct.map({
      case (a, (iter1)) => (a, iter1.filter(f => f._2.equals(a)))
    })

    clusterOfProp

  }
  def isContains(a: List[Node], b: List[Node]): Boolean = {
    if (a.forall(b.contains) || b.forall(a.contains)) {
      true
    } else
      false
  }

  def removeSupType(a: RDD[((String, HashSet[String]), (String, HashSet[String]))]): RDD[((String, HashSet[String]), (String, HashSet[String]))] = {

    val rdd1 = a.map(f => f._1._2.toSet)
    val rdd2 = a.map(f => f._2._2.toSet)
    val intersectRDD = a.map(f => f._1._2.toSet.intersect(f._2._2.toSet))
    val countRDD = intersectRDD.filter({
      case b => b.size > 0
    })
    if (!countRDD.isEmpty()
      && !intersectRDD.equals(rdd1)
      && !intersectRDD.equals(rdd2)) {

      val clusterOfProp = a.map({
        case ((a, iter), (b, iter1)) => ((a, iter.filter(f => !searchType(f.toString(), listSuperType))),
          (b, iter1.filter(f => !searchType(f.toString(), listSuperType))))
      })

      clusterOfProp
    } else
      a
  }
  def propClustering(triplesWithNumericLiteral: RDD[Triple]): RDD[(String, mutable.Set[(String, String, Object)])] = {

    val subMap = triplesWithNumericLiteral.map(f => (getLocalName1(f.getSubject),
      (getLocalName1(f.getSubject), getLocalName1(f.getPredicate), getNumber(f.getObject.toString())))) //.partitionBy(new HashPartitioner(8)) //make a function instead of using

    val initialSet = mutable.Set.empty[(String, String, Object)]
    val addToSet = (s: mutable.Set[(String, String, Object)], v: (String, String, Object)) => s += v
    val mergePartitionSets = (p1: mutable.Set[(String, String, Object)], p2: mutable.Set[(String, String, Object)]) => p1 ++= p2
    val uniqueByKey = subMap.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    uniqueByKey
  }

  def getNumber(a: String): Object = {
    val c = a.indexOf('^')
    val subject = a.substring(1, c - 1)

    subject

  }

  def sim(seq1: HashSet[String], seq2: HashSet[String]): Double = {
    var jSimilarity = 0.0
    var dbtype1: HashSet[String] = null
    var dbtype2: HashSet[String] = null
    val hyper1 = seq1.filter(p => p.contains("hypernym"))
    val hyper2 = seq2.filter(p => p.contains("hypernym"))
    //case of usa and India

    //USA= hypernym/states and India :- hypernym//Country
    if (hyper1 == hyper2 && !hyper1.isEmpty && !hyper2.isEmpty) {

      jSimilarity = 1.0
      jSimilarity
    } else {
      if (seq1.contains("hypernym")) {
        dbtype1 = seq1.dropRight(1)
      } else
        dbtype1 = seq1
      if (seq2.contains("hypernym")) {
        dbtype2 = seq2.dropRight(1)
      } else
        dbtype2 = seq2

      val intersect_cnt = dbtype1.toSet.intersect(dbtype2.toSet).size

      val union_count = dbtype1.toSet.union(dbtype2.toSet).size
      jSimilarity = intersect_cnt / (union_count).toDouble
      jSimilarity
    }
    jSimilarity
  }

  def iqr1(cluster: Seq[(String, String, Object)], anomalyListLimit: Int): Dataset[Row] = {

    //create sample data 

    var result: Dataset[Row] = null
    // var _partitionData: RDD[String] = _
    val KVcluster = sparkSession.sparkContext.parallelize(cluster.map(f => ((f._3.toString()).toDouble, f.toString())))
    val rowRDD1 = KVcluster.map(value => Row(value))
    val listofData = cluster.map(b => (b._3.toString()).toDouble).toList

    if (listofData.size > anomalyListLimit) {
      val k = sparkSession.sparkContext.makeRDD(listofData)
      //create sample data 
      //  println("sampleData=" + listofData)
      val c = listofData.sorted

      val rowRDD = sparkSession.sparkContext.makeRDD(c.map(value => Row(value)))
      val schema = StructType(Array(StructField("value", DoubleType)))
      val df = sparkSession.createDataFrame(rowRDD, schema)

      val schema1 = new StructType()
        .add(StructField("id", DoubleType, true))
        .add(StructField("val1", StringType, true))
      val dfWithoutSchema = sparkSession.createDataFrame(KVcluster).toDF("id", "dt")

      // calculate quantiles and IQR
      val quantiles = df.stat.approxQuantile("value",
        Array(0.25, 0.75), 0.0)
      //quantiles.foreach(println)

      val Q1 = quantiles(0)

      val Q3 = quantiles(1)

      val IQR = Q3 - Q1

      val lowerRange = Q1 - 1.5 * IQR

      val upperRange = Q3 + 1.5 * IQR

      val outliers = df.filter(s"value < $lowerRange or value > $upperRange").toDF("outlier")

      def matcher(row: Row): Boolean = row.getAs[Double]("id")
        .equals(row.getAs[Double]("outlier"))

      val join = dfWithoutSchema.crossJoin(outliers)

      result = join.filter(matcher _).distinct()
      // _result.select("dt").show(20, false)
      result.select("dt")
    }
    // result.show()
    result
  }

}
object AnomalyDetection {
  def apply(nTriplesRDD: RDD[Triple], objList: List[String], triplesType: List[String],
            JSimThreshold: Double, listSuperType: List[String], sparkSession: SparkSession, hypernym: String, numPartition: Int) = new AnomalyDetection(nTriplesRDD, objList, triplesType,
    JSimThreshold, listSuperType, sparkSession, hypernym, numPartition)
}
