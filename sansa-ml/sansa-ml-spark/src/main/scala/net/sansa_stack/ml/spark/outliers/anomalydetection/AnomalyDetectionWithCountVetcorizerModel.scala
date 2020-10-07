package net.sansa_stack.ml.spark.outliers.anomalydetection

import scala.collection.mutable
import scala.collection.mutable.HashSet

import org.apache.commons.math3.stat.descriptive._
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.apache.spark.{ HashPartitioner, RangePartitioner }
import org.apache.spark.ml.feature.{ MinHashLSH, _ }
import org.apache.spark.ml.linalg._
import org.apache.spark.rdd.{ RDD, _ }
import org.apache.spark.sql.{ SparkSession, _ }
import org.apache.spark.sql.functions.{ col, udf }
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import net.sansa_stack.ml.common.outliers.anomalydetection.Utils

/*
 *
 * AnomalyDetection - Anomaly detection of numerical data
 * @objList - list of numerical type.
 * @triplesType - List of rdf type's objects.
 * @return - cluster of similar subjects.
 */

class AnomalyDetectionWithCountVetcorizerModel(nTriplesRDD: RDD[Triple], objList: List[String],
                                               triplesType: List[String], JSimThreshold: Double,
                                               listSuperType: List[String], sparkSession: SparkSession, hypernym: String, numPartition: Int) extends Serializable {
  def run(): RDD[(Set[(String, String, Object)])] = {

    // get all the triples whose objects are literal
    // these literals also contains xsd:date as well as xsd:langstring
    val getObjectLiteral = getObjectList()

    // remove the literal which has ^^xsd:date or xsd:langstring(only considering numerical)
    val removedLangString = getObjectLiteral.filter(f => Utils.searchedge(f.getObject.toString(), objList))

    val removewiki = removedLangString.filter(f => (!f.getPredicate.toString().contains("wikiPageID")) &&
      (!f.getPredicate.toString().contains("wikiPageRevisionID")))

    // checking still object has only numerical data only
    val triplesWithNumericLiteral = triplesWithNumericLit(removewiki)

    val mapSubWithTriples = propClustering(triplesWithNumericLiteral) // .persist

    // get triples of hypernym
    val getHypernymTriples = getHyp()

    // filter rdf type having object value dbpedia and join with hyernym
    // val rdfTypeDBwiki = rdfType(getHypernym) //.partitionBy(new HashPartitioner(2)).persist()
    val rdfTypeDBwiki = rdfType(getHypernymTriples)

    // joining those subjects only who has rdf:ytpe and numerical literal
    val rdfTypeWithSubject = mapSubWithTriples.join(rdfTypeDBwiki)

    val mapSubjectwithType = rdfTypeWithSubject.map(f => (f._1, f._2._2))

    //  val propwithSub = propwithsubject(triplesWithNumericLiteral)
    // cluster subjects on the basis of rdf type
    val jacardSimilarity = jSimilarity(triplesWithNumericLiteral, mapSubjectwithType, mapSubWithTriples)

    jacardSimilarity

  }

  def getHyp(): RDD[Triple] = nTriplesRDD.filter(f => f.getPredicate.toString().equals(hypernym))

  def getObjectList(): RDD[Triple] = nTriplesRDD.filter(f => f.getObject.isLiteral())
  def triplesWithNumericLit(objLit: RDD[Triple]): RDD[Triple] = objLit.filter(f => Utils.isNumeric(f.getObject.toString()))
  def rdfType(getHypernym: RDD[Triple]): RDD[(String, HashSet[String])] = {

    // filter triples with predicate as rdf:type
    val triplesWithRDFType = nTriplesRDD.filter(_.getPredicate.toString() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    val triplesWithDBpedia = triplesWithRDFType.filter(f => Utils.searchType(f.getObject.toString(), triplesType))

    val subWithType1 = triplesWithDBpedia.map(f =>
        (Utils.getLocalName(f.getSubject), Utils.getLocalName(f.getObject)))
    val initialSet1 = mutable.HashSet.empty[String]
    val addToSet1 = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets1 = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    val uniqueByKey1 = subWithType1.aggregateByKey(initialSet1)(addToSet1, mergePartitionSets1)

    val hyper1 = getHypernym.map(f =>
        (Utils.getLocalName(f.getSubject), Utils.getLocalName(f.getObject) + ("hypernym")))
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

  def jSimilarity(
    TriplesWithNumericLiteral: RDD[Triple],
    rdfTypeDBwiki: RDD[(String, HashSet[String])], mapSubWithTriples: RDD[(String, mutable.Set[(String, String, Object)])]): RDD[(Set[(String, String, Object)])] = {

    nTriplesRDD.unpersist()
    import sparkSession.implicits._
    val partitionedy = rdfTypeDBwiki.persist
    val hashtoseq = partitionedy.map(f => (f._1, f._2.toSeq))
    val dfA = hashtoseq.toDF("id", "values")

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("values")
      .setOutputCol("features")
      .setVocabSize(100000000)
      .setMinDF(1)
      .fit(dfA)

    val kt = cvModel.transform(dfA) // .filter(isNoneZeroVector(col("features")))

    val mh = new MinHashLSH()
      .setNumHashTables(3) // tested with 100 on out4.nt file ..result in /home/rajjat/Desktop/recent_dataset/output_removed_boolean_udf.txt
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(kt)
    // val model1 = mh.fit(featurizedData)
    val dffilter = model.approxSimilarityJoin(kt, kt, 0.45)

    val opiu = dffilter.filter($"datasetA.id".isNotNull).filter($"datasetB.id".isNotNull)
      .filter(($"datasetA.id" =!= $"datasetB.id"))
      .select(
        col("datasetA.id").alias("id1"),
        col("datasetB.id").alias("id2"))

    val x1 = opiu.rdd // maimum time taken by this rdd
      .map(row => {
        val id = row.getString(0)
        val value = row.getString(1)
        (id, value)
      })

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    val mergePartitionSets3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val uniqueByKey3 = x1.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)

    x1.unpersist()

    val k = uniqueByKey3.map(f => ((f._2 += (f._1)).toSet)).map(a => (a, a))
      .aggregateByKey(Set[String]())((x, y) => y, (x, y) => x)
      .keys.distinct()

    val abc = k.repartition(50).persist()
    val simSubjectCart = abc.cartesian(abc).filter(f => f._1.intersect(f._2).size > 0)

    partitionedy.unpersist()
    // joined.unpersist()
    val subsetMembers = simSubjectCart.filter { case (set1, set2) => (set2.subsetOf(set1)) && (set1 -- set2).nonEmpty }
    val sdf = subsetMembers.map(f => (f._2))
    val superset1 = abc.subtract(sdf)

    val ys = superset1.flatMap(f => (f.map(g => (g, f))))

    val g = ys.join(mapSubWithTriples)

    val clusterOfSubjects = g.map({
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
      case (a, (iter1)) => (iter1.filter(f => f._2.equals(a)))
    })

    clusterOfProp

  }

  def propClustering(triplesWithNumericLiteral: RDD[Triple]): RDD[(String, mutable.Set[(String, String, Object)])] = {

    val subMap = triplesWithNumericLiteral.map(f => (Utils.getLocalName(f.getSubject),
      (Utils.getLocalName(f.getSubject), Utils.getLocalName(f.getPredicate), Utils.getNumber(f.getObject.toString())))) // .partitionBy(new HashPartitioner(8)) //make a function instead of using

    val initialSet = mutable.Set.empty[(String, String, Object)]
    val addToSet = (s: mutable.Set[(String, String, Object)], v: (String, String, Object)) => s += v
    val mergePartitionSets = (p1: mutable.Set[(String, String, Object)], p2: mutable.Set[(String, String, Object)]) => p1 ++= p2
    val uniqueByKey = subMap.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    uniqueByKey
  }

  def sim(seq1: HashSet[String], seq2: HashSet[String]): Double = {
    var jSimilarity = 0.0
    var dbtype1: HashSet[String] = null
    var dbtype2: HashSet[String] = null
    val hyper1 = seq1.filter(p => p.contains("hypernym"))
    val hyper2 = seq2.filter(p => p.contains("hypernym"))
    // case of usa and India

    // USA= hypernym/states and India :- hypernym//Country
    if (hyper1 == hyper2 && !hyper1.isEmpty && !hyper2.isEmpty) {

      jSimilarity = 1.0
      jSimilarity
    } else {
      if (seq1.contains("hypernym")) {
        dbtype1 = seq1.dropRight(1)
      } else {
        dbtype1 = seq1
      }
      if (seq2.contains("hypernym")) {
        dbtype2 = seq2.dropRight(1)
      } else {
        dbtype2 = seq2
      }

      val intersect_cnt = dbtype1.toSet.intersect(dbtype2.toSet).size

      val union_count = dbtype1.toSet.union(dbtype2.toSet).size
      jSimilarity = intersect_cnt / (union_count).toDouble
      jSimilarity
    }
    jSimilarity
  }

  def iqr1(cluster: Seq[(String, String, Object)], anomalyListLimit: Int): Dataset[Row] = {

    // create sample data

    var result: Dataset[Row] = null
    // var _partitionData: RDD[String] = _
    val KVcluster = sparkSession.sparkContext.parallelize(cluster.map(f => ((f._3.toString()).toDouble, f.toString())))

    val rowRDD1 = KVcluster.map(value => Row(value))
    val listofData = cluster.map(b => (b._3.toString()).toDouble).toList

    val k = sparkSession.sparkContext.makeRDD(listofData)
    // create sample data
    //  println("sampleData=" + listofData)
    val c = listofData.sorted

    val rowRDD = sparkSession.sparkContext.makeRDD(c.map(value => Row(value)))
    val schema = StructType(Array(StructField("value", DoubleType)))
    val df = sparkSession.createDataFrame(rowRDD, schema)

    val schema1 = new StructType()
      .add(StructField("id", DoubleType, true))
      .add(StructField("val1", StringType, true))
    val dfWithoutSchema = sparkSession.createDataFrame(KVcluster).toDF("id", "outliers")

    // calculate quantiles and IQR
    val quantiles = df.stat.approxQuantile(
      "value",
      Array(0.25, 0.75), 0.0)
    // quantiles.foreach(println)

    val Q1 = quantiles(0)

    val Q3 = quantiles(1)

    val IQR = Q3 - Q1

    val lowerRange = Q1 - 1.5 * IQR

    val upperRange = Q3 + 1.5 * IQR

    val outliers = df.filter(s"value < $lowerRange or value > $upperRange").toDF("anomaly")

    def matcher(row: Row): Boolean = row.getAs[Double]("id")
      .equals(row.getAs[Double]("anomaly"))

    if (!outliers.head(1).isEmpty) {
      val join = dfWithoutSchema.crossJoin(outliers)
      result = join.filter(matcher _).distinct()
      result.select("outliers").show(100, false)
      result
    }
    result
  }

  def iqr2(cluster: Seq[(String, String, Object)], anomalyListLimit: Int): Seq[(String, String, Object)] = {

    // create sample data

    val listofData = cluster.map(b => (b._3.toString()).toDouble).toArray

    val c = listofData.sorted

    val arrMean = new DescriptiveStatistics()
    genericArrayOps(c).foreach(v => arrMean.addValue(v))
    // Get first and third quartiles and then calc IQR
    val Q1 = arrMean.getPercentile(25)
    // println("Q1="+Q1)
    val Q3 = arrMean.getPercentile(75)
    // println("Q3="+Q3)
    val IQR = Q3 - Q1
    // println("IQR="+IQR)
    val lowerRange = Q1 - 1.5 * IQR
    // println("lowerRange="+lowerRange)
    val upperRange = Q3 + 1.5 * IQR
    //    println("upperRange="+upperRange)
    val yse = c.filter(p => (p < lowerRange || p > upperRange))

    val xde = cluster.filter(f => Utils.search(f._3.toString().toDouble, yse))

    xde
  }
}
object AnomalyDetectionWithCountVetcorizerModel {
  def apply(nTriplesRDD: RDD[Triple], objList: List[String], triplesType: List[String],
            JSimThreshold: Double, listSuperType: List[String], sparkSession: SparkSession,
            hypernym: String, numPartition: Int): AnomalyDetectionWithCountVetcorizerModel = new AnomalyDetectionWithCountVetcorizerModel(nTriplesRDD, objList, triplesType,
    JSimThreshold, listSuperType, sparkSession, hypernym, numPartition)
}
