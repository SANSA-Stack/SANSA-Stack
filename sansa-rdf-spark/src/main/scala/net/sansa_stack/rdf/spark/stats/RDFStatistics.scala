package net.sansa_stack.rdf.spark.stats

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession
import java.io.StringWriter
import java.io.File
import org.apache.jena.vocabulary.RDFS
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.OWL

/**
 * A Distributed implementation of RDF Statisctics.
 *
 * @author Gezim Sejdiu
 */
object RDFStatistics extends Serializable {

  @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Compute distributed RDF dataset statistics.
   * @param triples RDF graph
   * @return VoID description of the given dataset
   */
  def run(triples: RDD[Triple]): RDD[String] = {
    Used_Classes(triples, spark).Voidify()
      .union(DistinctEntities(triples, spark).Voidify)
      .union(DistinctSubjects(triples, spark).Voidify)
      .union(DistinctObjects(triples, spark).Voidify)
      .union(PropertyUsage(triples, spark).Voidify)
      .union(SPO_Vocabularies(triples, spark).Voidify)
  }

  /**
   * Voidify RDF dataset based on the Vocabulary of Interlinked Datasets (VoID) [[https://www.w3.org/TR/void/]]
   *
   * @param stats given RDF dataset statistics
   * @param source name of the Dataset:source--usualy the file's name
   * @param output the directory to save RDF dataset summary
   */
  def voidify(stats: RDD[String], source: String, output: String): Unit = {
    val pw = new StringWriter

    val prefix = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                    @prefix void: <http://rdfs.org/ns/void#> .
                    @prefix void-ext: <http://stats.lod2.eu/rdf/void-ext/> .
                    @prefix qb: <http://purl.org/linked-data/cube#> .
                    @prefix dcterms: <http://purl.org/dc/terms/> .
                    @prefix ls-void: <http://stats.lod2.eu/rdf/void/> .
                    @prefix ls-qb: <http://stats.lod2.eu/rdf/qb/> .
                    @prefix ls-cr: <http://stats.lod2.eu/rdf/qb/criteria/> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                    @prefix xstats: <http://example.org/XStats#> .
                    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
                    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ."""

    val src = "\n<http://stats.lod2.eu/rdf/void/?source=" + source + ">\n"
    val end = "\na void:Dataset ."

    val voidify = prefix.concat(src).concat(stats.coalesce(1, true).collect().mkString).concat(end)
    println("\n" + voidify)
    pw.write(voidify)
    val vidifyStats = spark.sparkContext.parallelize(Seq(pw.toString()))
    vidifyStats.coalesce(1, shuffle = true).saveAsTextFile(output)
    //pw.close
  }

  /**
   * Prints the Voidiy version of the given RDF dataset
   *
   * @param stats given RDF dataset statistics
   * @param source name of the Dataset:source--usualy the file's name
   */
  def print(stats: RDD[String], source: String): Unit = {
    val prefix = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                    @prefix void: <http://rdfs.org/ns/void#> .
                    @prefix void-ext: <http://stats.lod2.eu/rdf/void-ext/> .
                    @prefix qb: <http://purl.org/linked-data/cube#> .
                    @prefix dcterms: <http://purl.org/dc/terms/> .
                    @prefix ls-void: <http://stats.lod2.eu/rdf/void/> .
                    @prefix ls-qb: <http://stats.lod2.eu/rdf/qb/> .
                    @prefix ls-cr: <http://stats.lod2.eu/rdf/qb/criteria/> .
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                    @prefix xstats: <http://example.org/XStats#> .
                    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
                    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ."""

    val src = "\n<http://stats.lod2.eu/rdf/void/?source=" + source + ">\n"
    val end = "\na void:Dataset ."

    val voidify = prefix.concat(src).concat(stats.coalesce(1, true).collect().mkString).concat(end)
    println("\n" + voidify)
  }

}

class Used_Classes(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  //?p=rdf:type && isIRI(?o)
  def Filter() = triples.filter(f =>
    f.getPredicate.toString().equals(RDF.`type`) && f.getObject.isURI())
    .map(_.getObject)

  //M[?o]++
  def Action() = Filter()
    .map(f => (f, 1))
    .reduceByKey(_ + _)
  //.cache()

  //top(M,100)
  def PostProc() = Action().sortBy(_._2, false)
    .take(100)

  def Voidify() = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:classPartition "

    val classes = spark.sparkContext.parallelize(PostProc())
    val vc = classes.map(t => "[ \nvoid:class " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var cl_a = new Array[String](1)
    cl_a(0) = "\nvoid:classes " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(triplesString)
    val c = spark.sparkContext.parallelize(cl_a)
    if (classes.count() > 0)
      c.union(c_p).union(vc)
    else c.union(vc)
  }
}
object Used_Classes {

  def apply(triples: RDD[Triple], spark: SparkSession) = new Used_Classes(triples, spark)

}

class Classes_Defined(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  //?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  def Filter() = triples.filter(f =>
    (f.getPredicate.toString().equals(RDF.`type`) && f.getObject.toString().equals(RDFS.Class))
      || (f.getPredicate.toString().equals(RDF.`type`) && f.getObject.toString().equals(OWL.Class))
      && !f.getSubject.isURI())

  //M[?o]++
  def Action() = Filter().map(_.getSubject).distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}
object Classes_Defined {

  def apply(triples: RDD[Triple], spark: SparkSession) = new Classes_Defined(triples, spark)
}

class PropertiesDefined(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter() = triples.filter(f =>
    (f.getPredicate.toString().equals(RDF.`type`) && f.getObject.toString().equals(OWL.ObjectProperty))
      || (f.getPredicate.toString().equals(RDF.`type`) && f.getObject.toString().equals(RDF.Property))
      && !f.getSubject.isURI())
  def Action() = Filter().map(_.getPredicate).distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:properties  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}
object PropertiesDefined {

  def apply(triples: RDD[Triple], spark: SparkSession) = new PropertiesDefined(triples, spark)
}

class PropertyUsage(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter() = triples

  //M[?p]++
  def Action() = Filter().map(_.getPredicate)
    .map(f => (f, 1))
    .reduceByKey(_ + _)
  //.cache()

  //top(M,100)
  def PostProc() = Action().sortBy(_._2, false)
    .take(100)

  def Voidify() = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:propertyPartition "

    val properties = spark.sparkContext.parallelize(PostProc())
    val vp = properties.map(t => "[ \nvoid:property " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var pl_a = new Array[String](1)
    pl_a(0) = "\nvoid:properties " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(triplesString)
    val p = spark.sparkContext.parallelize(pl_a)
    p.union(c_p).union(vp)

    /* var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:propertyPartition "
    val header = sc.parallelize(triplesString)
    val properties = sc.parallelize(PostProc())
      .map(t => "[ \nvoid:property " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")
    header.union(properties)*/
  }
}
object PropertyUsage {

  def apply(triples: RDD[Triple], spark: SparkSession) = new PropertyUsage(triples, spark)

}

class DistinctEntities(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter() = triples.filter(f =>
    (f.getSubject.isURI() && f.getPredicate.isURI() && f.getObject.isURI()))

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:entities  " + PostProc() + ";"
    spark.sparkContext.parallelize(ents)
  }
}
object DistinctEntities {

  def apply(triples: RDD[Triple], spark: SparkSession) = new DistinctEntities(triples, spark)
}

class DistinctSubjects(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter() = triples.filter(f => f.getSubject.isURI()).map(_.getSubject)

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctSubjects  " + PostProc() + ";"
    spark.sparkContext.parallelize(ents)
  }
}
object DistinctSubjects {

  def apply(triples: RDD[Triple], spark: SparkSession) = new DistinctSubjects(triples, spark)
}

class DistinctObjects(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter() = triples.filter(f => f.getObject.isURI()).map(_.getObject)

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctObjects  " + PostProc() + ";"
    spark.sparkContext.parallelize(ents)
  }
}
object DistinctObjects {

  def apply(triples: RDD[Triple], spark: SparkSession) = new DistinctObjects(triples, spark)
}

class SPO_Vocabularies(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter() = triples

  def Action(node: org.apache.jena.graph.Node) = Filter().map(f => node.getNameSpace()).cache()

  def SubjectVocabulariesAction() = Filter().filter(_.getSubject.isURI()).map(f => (f.getSubject.getNameSpace()))
  def SubjectVocabulariesPostProc() = SubjectVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def PredicateVocabulariesAction() = Filter().filter(_.getPredicate.isURI()).map(f => (f.getPredicate.getNameSpace()))
  def PredicateVocabulariesPostProc() = PredicateVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def ObjectVocabulariesAction() = Filter().filter(_.getObject.isURI()).map(f => (f.getObject.getNameSpace()))
  def ObjectVocabulariesPostProc() = ObjectVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def PostProc(node: org.apache.jena.graph.Node) = Filter().map(f => node.getNameSpace())
    .map(f => (f, 1)).reduceByKey(_ + _)

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:vocabulary  <" + SubjectVocabulariesAction().union(PredicateVocabulariesAction()).union(ObjectVocabulariesAction()).distinct().take(15).mkString(">, <") + ">;"
    spark.sparkContext.parallelize(ents)
  }
}
object SPO_Vocabularies {

  def apply(triples: RDD[Triple], spark: SparkSession) = new SPO_Vocabularies(triples, spark)
}

object OtherStats {

  def PropertyUsageDistinctPerSubject(triples: RDD[Triple]) = {
    triples
      .groupBy(_.getSubject)
      .map(f => (f._2.filter(p => p.getPredicate.getLiteralLexicalForm.contains(p)), 1))
      .reduceByKey(_ + _)
  }

  def PropertyUsageDistinctPerObject(triples: RDD[Triple]) = {
    triples
      .groupBy(_.getObject)
      .map(f => (f._2.filter(p => p.getPredicate.getLiteralLexicalForm.contains(p)), 1))
      .reduceByKey(_ + _)
  }
}
