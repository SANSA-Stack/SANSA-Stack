package net.sansa_stack.ml.spark.clustering.utils

import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

import net.sansa_stack.ml.spark.clustering.datatypes.AppConfig
import net.sansa_stack.rdf.spark.io.NTripleReader

class DataFiltering(val spark: SparkSession, val conf: AppConfig) extends Serializable {

  val dataRDD: RDD[Triple] = NTripleReader.load(spark, conf.dataset.input).persist()

  /**
    * Generate triples with related to poi in poiArray, method name not JavaBean format because of side effect with Unit return result
    * @param poiArray id of pois in Vienna
    * @param dataRDD RDD containing triples
    * @param spark SparkSession
    * @return
    */
  def get_triples(poiArray: Array[Long], dataRDD: RDD[Triple], spark: SparkSession) : (RDD[Triple], RDD[Triple]) = {
    // create an array of subjects related with each poi
    val subjects = ArrayBuffer[String]()
    for (i <- 0 until poiArray.length - 1) {
      subjects ++= createSubjects(poiArray(i))
    }
    // RDD[Triple] => RDD[(subject, Triple)]
    val dataRDDPair = dataRDD.map(f => (f.getSubject.getURI, f)).persist()
    // create RDD[(subject, subject)] from Array[subjects]
    val subjectsRDD = spark.sparkContext.parallelize(subjects.toSet.toList).map(f => (f, f)).persist()
    // get RDD[Triples] with subject in Array[subjects]
    val viennaTriples = subjectsRDD.join(dataRDDPair).map(f => f._2._2).persist()
    // find filtered Triples with prediction category, and get their object => RDD[Object]
    val viennaCatgoriesObjects = viennaTriples.filter(f => f.getPredicate.getURI.equals("http://example.org/def#category")).map(f => f.getObject.getURI).distinct().persist()
    // RDD[Object] => RDD[(Object, Object)]
    val viennaPoiCategoriesRDD = viennaCatgoriesObjects.map(f => (f, f)).persist()
    // RDD[(Object, Object)] => RDD[Triples], where Object is Subject in Triples
    val viennaCategoryTriples = viennaPoiCategoriesRDD.join(dataRDDPair).map(f => f._2._2)
    // RDD[Triples] => RDD[(Key, Triple)], where key=subject+predicate+object, because there are some duplicated triples in the tomtom data
    val temp = viennaCategoryTriples.map(f => (f.getSubject.getURI + f.getPredicate.getURI + f.getObject.toString(), f)).persist()
    // remove duplicated triples
    val categoryTriples = temp.reduceByKey((v1, v2) => v1).map(f => f._2).persist()
    (viennaTriples, categoryTriples)
  }

  /**
    * @param poiID id of a poi
    * @return an array of subject in RDF triples with related to this poi
    */
  def createSubjects(poiID: Long): ArrayBuffer[String] = {
    val subjects = ArrayBuffer[String]()
    val id = "http://example.org/id/poi/".concat(poiID.toString)
    subjects.+=(id)
    subjects.+=(id.concat("/address"))
    subjects.+=(id.concat("/phone"))
    subjects.+=(id.concat("/geometry"))
    subjects.+=(id.concat("/name"))
    subjects.+=(id.concat("/accuracy_info"))
    subjects.+=(id.concat("/brandname"))
    subjects
  }
}
