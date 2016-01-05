package org.dissect.rdf.spark.LODStats

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.collection.mutable.{ HashMap, HashSet }
import com.hp.hpl.jena.reasoner.rulesys.builtins.IsLiteral

/*
 *RDFStat
 */
class RDFStats(graph: Graph[String, String], triples: RDD[(String, String, String)]) extends Serializable {

  //val subjects: RDD[String] = triples.map(triple => triple._1)
  //val objects: RDD[String] = triples.map(triple => triple._3)
  //val predicate: RDD[String] = triples.map(triple => triple._2)

  val S: HashSet[String] = new HashSet[String]
  val M: HashMap[String, Int] = new HashMap[String, Int]
  val G: HashMap[String, String] = new HashMap[String, String]

  var count: Long = 0

  /*
   * 1.
   * Used Classes Criterion.
   * Output format: {'classname': usage count}
   */
  def UsedClasses(): Long = {
    val uc = triples.map(tr => (tr._2, tr._3))
      .filter(f =>
        f._1.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") & (isIRI(f._2)))
    /*  for (u <- uc)
      S.add(u._1)*/
    uc.collect.foreach(f => S.add(f._1))
    val c = uc.count
    c
  }

  /*
   * 2.
   * Class usage count
   * To count the usage of respective classes of a dataset, 
   * the filter rule that is used to analyze a triple is the same as in the first criterion. 
   */
  def ClassUsedCount() = {
    val uc = triples
      .filter(f =>
        f._2.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") & (isIRI(f._3)))
      .map(obj => (obj, 1))
      .reduceByKey(_ + _)
    //uc.foreach(elem => addAccum(M, (elem._1, 0)))
    uc.collect.foreach(elem => M.put(elem._1._3, elem._2))
  }

  def addAccum(acc: HashMap[String, Int], elem: (String, Int)): HashMap[String, Int] = {
    val (k1, v1) = elem
    acc += acc.find(_._1 == k1).map {
      case (k2, v2) => k2 -> (v1 + v2)
    }.getOrElse(elem)
    acc
  }

  /*
   * 3.
   * To get a set of classes that are defined within a dataset this criterion is being used. 
   * Filter rule: ?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
   */
  def ClassesDefined(): Long = {
    val uc = triples
      .filter(f =>
        ((f._2.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") & (isIRI(f._1))) &
          (f._3.equals("http://www.w3.org/2000/01/rdf-schema#Class") | f._3.equals("http://www.w3.org/2002/07/owl#Class"))))

    uc.collect.foreach(f => S.add(f._1))
    val c = uc.count
    c
  }

  /*
   * 4.
   * Gather hierarchy of classes seen
   */
  def ClassHierarchyDepth() = {
    val uc = triples
      .filter(f =>
        (isIRI(f._1)) && (isIRI(f._3)) && f._2.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf"))
    uc.collect.foreach(f => G.put(f._1, f._3))

    def PostProc() {
      var final_depth = 0
      for (root_elem <- G) {
        var depth = 0
        //val new_depth = 
      }

    }

  }

  /*
   * 5.
   * Property usage. 
   * This criterion is used to count the usage of properties within triples.
   */
  def PropertyUsage() {
    val pu = triples
      .map(obj => (obj._2, 1))
      .reduceByKey(_ + _)
    pu.collect.foreach(elem => M.put(elem._1, elem._2))

  }

  /*
   * 6.
   * Property usage distinct per subject.
   */
  def PropertyUsageDistinctPerSubject() {
    val pud = triples
      .groupBy(_._1)
      .map(f => (f._2.filter(p => p._2.contains(p)), 1))
      .reduceByKey(_ + _)
    pud.collect.foreach(elem => M.put(elem._1.slice(2, 2).toString, elem._2))

  }

  /*
   * 7.
   * Property usage distinct per object.
   * 
   */
  def PropertyUsageDistinctPerObject() {
    val pud = triples
      .groupBy(_._3)
      .map(f => (f._2.filter(p => p._2.contains(p)), 1))
      .reduceByKey(_ + _)
    pud.collect.foreach(elem => M.put(elem._1.slice(2, 2).toString, elem._2))
  }

  /*
   * 12.
   * Property hierarchy depth.
   */
  def PropertyHierarchyDepth() = {
    val uc = triples
      .filter(f =>
        (isIRI(f._1)) && (isIRI(f._3)) && f._2.equals("http://www.w3.org/2000/01/rdf-schema#subPropertyOf"))
    uc.collect.foreach(f => G.put(f._1, f._3))
  }

  /*
   * 13.
   * Subclass usage.
   */
  def SubclassUsage(): Long = {
    val c = triples.map(f => f._2)
      .filter(f => f.equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")).count
    count += c
    c
  }

  /*
   * 14.
   * Triples
   * This criterion is used to measure the amount of triples of a dataset. 
   */
  def Triples(): Long = {
    val c = triples.count
    c
  }

  /*
   * 15.
   * Entities mentioned
   * To get a count of entities (resources / IRIs) that are mentioned within a dataset, this criterion is used. 
   */
  def EntitiesMentioned(): Long = {
    val c = triples
      .filter(f =>
        (isIRI(f._1)) && (isIRI(f._2)) && isIRI(f._3))
      .count
    c
  }

  /*
    16.
   *  Distinct entities.
   *  To get a set/list of distinct entities of a dataset all IRIs are extracted from the respective triple and added to the set of entities.
   */
  def Entities(): HashSet[String] = {
    val c = triples
      .filter(f =>
        (isIRI(f._1))) // && (isIRI(f._2)) && isIRI(f._3))
      .map(f => f._1).union(triples.filter(f =>
        (isIRI(f._2))).map(f => f._2)).union(triples.filter(f =>
        (isIRI(f._3))).map(f => f._3))
      .distinct
    val H = new HashSet[String]
    c.collect.foreach(f => H.add(f))
    H
  }

  /*
   * 17.
   * Literals
   * To get the amount of triples that are referencing literals to subjects 
   * the illustrated filter rule is used to analyze the respective triple. 
   */
  def Literals(): Long = {
    val c = triples
      .filter(f =>
        isLiteral(f._3))
      .count
    c
  }

  def LiteralsEnt(): HashSet[String] = {
    val c = triples.map(f => f._3)
      .filter(f =>
        isLiteral(f))
    val H = new HashSet[String]
    c.collect.foreach(f => H.add(f))
    H
  }

  /*
   * 18.
   * Distinct number of entities.
   * Entity - triple, where ?s is IRI (not blank)
   */
  def BlanksAsSubject(): Long = {
    //graph.edges.filter(f => f.attr.startsWith("313f9908:")).count
    val c = triples.map(f => f._1)
      .filter(f => f.startsWith("_:")).count
    count += c
    c
  }

  /*
   * 19.
   * Distinct number of entities.
   * Entity - triple, where ?s is IRI (not blank)
   */
  def BlanksAsObject(): Long = {
    val c = triples.map(f => f._3)
      .filter(_.startsWith("_:")).count
    count += c
    c
    //graph.unpersist(true)
  }

  def printBlanksAsObject() = {
    println(triples.map(f => f._1).collect.foreach(println))
  }

  /*
   * 20.
   * Usually in RDF/S and OWL literals used as objects of triples can be specified narrower. 
   * Histogram of types used for literals.
   */
  def Datatypes(): HashMap[String, Int] = {
    val c = triples.map(f => f._3)
      .filter(f =>
        isLiteral(f) && !dataType(f).isEmpty())
      .map(obj => (dataType(obj), 1))
      .reduceByKey(_ + _)
    val H = new HashMap[String, Int]
    c.collect.foreach(elem => H.put(elem._1, elem._2))
    H
  }

  /*
   * 21.
   * Usually in RDF/S and OWL literals used as objects of triples can be specified narrower. 
   * Histogram of types used for literals.
   */
  def Languages(): HashMap[String, Int] = {
    val c = triples.map(f => f._3)
      .filter(f =>
        isLiteral(f) && !languageTag(f).isEmpty())
      .map(obj => (languageTag(obj), 1))
      .reduceByKey(_ + _)
    val H = new HashMap[String, Int]
    c.collect.foreach(elem => H.put(elem._1, elem._2))
    H
  }

  /*
   * 22.
   * Average typed string length.
   */
  def avg_typed: Double = {
    val c = triples.map(f => f._3)
      .filter(f =>
        isLiteral(f) && dataType(f).equals("http://www.w3.org/2001/XMLSchema#string"))
      .map(obj => obj.sum / obj.size.toDouble)
    val avg = c.aggregate(0.0)(math.max(_, _), _ + _)
    avg
  }

  /*
   * 23.
   * Average untyped string length.
   */
  def avg_untyped: Double = {
    val c = triples.map(f => f._3)
      .filter(f =>
        isLiteral(f) && dataType(f).isEmpty())
      .map(obj => obj.sum / obj.size.toDouble)
    val avg = c.aggregate(0.0)(math.max(_, _), _ + _)
    avg
  }

  /*
   * 24.
   * Typed subjects
   */

  def TypedSubject(): Long = {
    val c = triples
      .filter(f => f._2.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
      .map(f => f._1).count
    c
  }

  def printEntities() = {
    println(triples.map(f => f._1).union(triples.map(f => f._3)).distinct.collect.foreach(println))
  }

  /*
   * 25.
   * Number of labeled subjects.
   */
  def LabeledSubjects(): Long = {
    val c = triples
      .filter(f => f._2.equals("http://www.w3.org/2000/01/rdf-schema#label"))
      .map(f => f._1).count
    c
  }

  /*
   * 26.
   * Number of triples with owl#sameAs as predicate
   */
  def SameAs(): Long = {
    val c = triples.map(f => f._2)
      .filter(f => f.equals("http://www.w3.org/2002/07/owl#sameAs")).count
    count += c
    c
  }

  /*
   * 27.
   * Links
   */
  def Links() = {

  }

  /*
   * 
   */
  def VoID(void_model: String, dataset: String) {

  }

  /*
   * Avarage length of untyped/string literals.
   */
  def StringLength() {

  }

  def runStats() {
    println("-------stats----------------------")
    val bAso = BlanksAsObject
    val bAsS = BlanksAsSubject
    val uc = UsedClasses
    val sAs = SameAs
    val s = ClassesDefined
    val d_e = Entities
    println("Stats [bAsocount:" + bAso + ", bAsScount:" + bAsS + ", uccount:" + uc + ", SameAs:" + SameAs + ", ClassesDefined:" + ClassesDefined +
      ", Entities:" + d_e + "]")
    //printBlanksAsObject
    println("-------printEntities")
    val startLoading = System.currentTimeMillis();
    //printEntities
    val endLoading = System.currentTimeMillis() - startLoading;
    println("Finished in " + endLoading + " ms");

    println("ClassUsedCount")
    ClassUsedCount
    M.take(100).foreach(println(_))
    println("Sizr :" + M.size)

    println("ClassUsedCount end ")

    S.foreach(f => "cla" + f.toString())

    println("size" + S.size)

    println("PropertyUsage")
    PropertyUsage
    M.take(100).foreach(println(_))

    println("PropertyUsageDistinctPerSubject")
    PropertyUsageDistinctPerSubject
    M.foreach(println(_))

    println("as")
    Entities.foreach(println(_))

    println("LiteralsEnt")
    LiteralsEnt.foreach(println(_))
    println("Datatypes")
    Datatypes.foreach(println(_))

    println("----------stats------------------------")
  }

  def isIRI(node: String): Boolean = {
    var res = false;
    if (node.startsWith("http://")) //isURI())
      res = true
    res
  }

  def isBlank(node: String): Boolean = {
    var res = false
    if (node.startsWith("_:"))
      res = true
    res
  }

  def isLiteral(node: String): Boolean = {
    var res = false;
    if (!isBlank(node) && !isIRI(node)) //isURI())
      res = true
    res
  }

  def dataType(literal: String): String = {
    val index = literal.indexOf("^^")
    var res = "";
    if (index > -1)
      res = literal.substring(index + 2)
    res
  }

  def languageTag(literal: String): String = {
    val index = literal.indexOf("@")
    var res = "";
    if (index > -1)
      res = literal.substring(index + 1)
    res
  }

}

object RDFStats {
  def apply(graph: Graph[String, String], triples: RDD[(String, String, String)]) = new RDFStats(graph, triples)

}

trait RDFStatInterface {

  def count(self: RDFStatInterface, s: String, p: String, o: String, s_blank: String)
  def voidify(self: RDFStatInterface)

}

