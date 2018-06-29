package net.sansa_stack.rdf.spark.stats

import java.io.{ File, StringWriter }

import net.sansa_stack.rdf.spark.model.graph._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.vocabulary.{ OWL, RDF, RDFS, XSD }
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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

  /**
   * 6. Property usage distinct per subject criterion
   *
   * @param triples RDD of triples
   * @return the usage of properties grouped by subject
   */
  def PropertyUsageDistinctPerSubject(triples: RDD[Triple]): RDD[(Iterable[Triple], Int)] = {
    triples
      .groupBy(_.getSubject)
      .map(f => (f._2.filter(p => p.getPredicate.getLiteralLexicalForm.contains(p)), 1))
      .reduceByKey(_ + _)
  }

  /**
   * 7. Property usage distinct per object criterion
   *
   * @param triples RDD of triples
   * @return the usage of properties grouped by object
   */
  def PropertyUsageDistinctPerObject(triples: RDD[Triple]): RDD[(Iterable[Triple], Int)] = {
    triples
      .groupBy(_.getObject)
      .map(f => (f._2.filter(p => p.getPredicate.getLiteralLexicalForm.contains(p)), 1))
      .reduceByKey(_ + _)
  }

  /**
   *  4. Class hierarchy depth criterion
   *
   *  @param triples RDD of triples
   *  @return the depth of the graph
   */
  def ClassHierarchyDepth(triples: RDD[Triple]): VertexRDD[Int] = {
    val uc_triples = triples
      .filter(triple => (triple.getPredicate.matches(RDFS.subClassOf.asNode()) &&
        triple.getSubject.isURI() && triple.getObject.isURI()))

    val graph = uc_triples.asGraph()

    graph.inDegrees
  }

  /**
   *  12. Property hierarchy depth criterion
   *
   *  @param triples RDD of triples
   *  @return the depth of the graph
   */
  def PropertyHierarchyDepth(triples: RDD[Triple]): VertexRDD[Int] = {

    val uc_triples = triples
      .filter(triple => (triple.getPredicate.matches(RDFS.subPropertyOf.asNode()) &&
        triple.getSubject.isURI() && triple.getObject.isURI()))

    val graph = uc_triples.asGraph()

    graph.inDegrees

  }

  /**
   * 13. Subclass usage criterion
   *
   * @param triples RDD of triples
   * @return the usage of subclasses on the given graph
   */
  def SubclassUsage(triples: RDD[Triple]): Long = {
    triples.filter(triple => triple.getPredicate.matches(RDFS.subClassOf.asNode()))
      .count
  }

  /**
   * 14. Triples criterion
   *
   * @param RDD of triples
   * @return the amount of triples of a given graph.
   */
  def Triples(triples: RDD[Triple]): Long =
    triples.count

  /**
   * 15.Entities mentioned criterion
   *
   * @param tripls RDD of triples
   * @return  number of entities (resources / IRIs) that are mentioned within a RDF graph.
   */
  def EntitiesMentioned(triples: RDD[Triple]): Long = {
    triples.filter(triple => (triple.getSubject.isURI() && triple.getPredicate.isURI() && triple.getObject.isURI()))
      .count
  }

  /**
   * * 17. Literals criterion
   *
   * @param triples RDD of triples
   * @return number of triples that are referencing literals to subjects.
   */
  def Literals(triples: RDD[Triple]): RDD[Triple] =
    triples.filter(_.getObject.isLiteral())

  /**
   * 18. Blanks as subject criterion
   *
   * @param triples RDD of triples
   * @return number of triples where blanknodes are used as subjects.
   */
  def BlanksAsSubject(triples: RDD[Triple]): RDD[Triple] =
    triples.filter(_.getSubject.isBlank())

  /**
   * 19. Blanks as object criterion
   *
   * @param triples RDD of triples
   * @return number of triples where blanknodes are used as objects.
   */
  def BlanksAsObject(triples: RDD[Triple]): RDD[Triple] =
    triples.filter(_.getObject.isBlank())

  /**
   * 20. Datatypes criterion
   *
   * @param triples RDD of triples
   * @return histogram of types used for literals.
   */
  def Datatypes(triples: RDD[Triple]): RDD[(String, Int)] = {
    triples.filter(triple => (triple.getObject.isLiteral() && !triple.getObject.getLiteralDatatype.getURI.isEmpty()))
      .map(triple => (triple.getObject.getLiteralDatatype.getURI, 1))
      .reduceByKey(_ + _)
  }

  /**
   * 21. Languages criterion
   *
   * @param triples RDD of triples
   * @return histogram of languages used for literals.
   */
  def Languages(triples: RDD[Triple]): RDD[(String, Int)] = {
    triples.filter(triple => (triple.getObject.isLiteral() && !triple.getObject.getLiteralLanguage.isEmpty()))
      .map(triple => (triple.getObject.getLiteralLanguage, 1))
      .reduceByKey(_ + _)
  }

  /**
   * 22. Average typed string length criterion.
   *
   * @param triples RDD of triples
   * @return the average typed string length used throughout the RDF graph.
   */
  def AvgTypedStringLength(triples: RDD[Triple]): Double = {
    val typed_strngs = triples.filter(triple => (triple.getObject.isLiteral() && triple.getObject.getLiteralDatatypeURI.equals(XSD.xstring.getURI)))
    val lenth_o = typed_strngs.map(_.getObject.toString().length()).sum()
    val cnt = typed_strngs.count()
    if (cnt > 0) lenth_o / cnt else 0
  }

  /**
   * 23. Average untyped string length criterion.
   *
   * @param triples RDD of triples
   * @return the average untyped string length used throughout the RDF graph.
   */
  def AvgUntypedStringLength(triples: RDD[Triple]): Double = {
    val typed_strngs = triples.filter(triple => (triple.getObject.isLiteral() && triple.getObject.getLiteralDatatypeURI.isEmpty()))
    val lenth_o = typed_strngs.map(_.getObject.toString().length()).sum()
    val cnt = typed_strngs.count()
    if (cnt > 0) lenth_o / cnt else 0
  }

  /**
   * 24. Typed subjects criterion.
   *
   * @param triples RDD of triples
   * @return list of typed subjects.
   */
  def TypedSubjects(triples: RDD[Triple]): RDD[Node] =
    triples.filter(triple => triple.getPredicate.matches(RDF.`type`.asNode())).map(_.getSubject)

  /**
   * 24. Labeled subjects criterion.
   *
   * @param triples RDD of triples
   * @return list of labeled subjects.
   */
  def LabeledSubjects(triples: RDD[Triple]): RDD[Node] =
    triples.filter(triple => triple.getPredicate.matches(RDFS.label.asNode())).map(_.getSubject)

  /**
   * 25. SameAs criterion.
   *
   * @param triples RDD of triples
   * @return list of triples with owl#sameAs as predicate
   */
  def SameAs(triples: RDD[Triple]): RDD[Triple] =
    triples.filter(_.getPredicate.matches(OWL.sameAs.asNode()))

  /**
   * 26. Links criterion.
   *
   * @param triples RDD of triples
   * @return list of namespaces and their frequentcies.
   */
  def Links(triples: RDD[Triple]): RDD[(String, Int)] = {
    triples.filter(triple => ((triple.getSubject.isURI() && triple.getObject.isURI()) &&
      triple.getSubject.getNameSpace != triple.getObject.getNameSpace))
      .map(triple => (triple.getSubject.getNameSpace() + triple.getObject.getNameSpace()))
      .map(f => (f, 1)).reduceByKey(_ + _)
  }

  /**
   * 28.Maximum per property {int,float,time} criterion
   *
   * @param triples RDD of triples
   * @return entities with their maximum values on the graph
   */
  def MaxPerProperty(triples: RDD[Triple]): (Triple, Int) = {
    val max_per_property_def = triples.filter(triple => (triple.getObject.matches(XSD.xint.asNode())
      | triple.getObject.matches(XSD.xfloat.asNode()) | triple.getObject.matches(XSD.dateTime.asNode())))
    val properties_fr = max_per_property_def.map(f => (f, 1)).reduceByKey(_ + _)

    val ordered = properties_fr.takeOrdered(1)(Ordering[Int].reverse.on(_._2))
    ordered.maxBy(_._2)
  }

  /**
   * 29. Average per property {int,float,time} criterion
   *
   * @param triples RDD of triples
   * @return entities with their average values on the graph
   */
  def AvgPerProperty(triples: RDD[Triple]): RDD[(Triple, Double)] = {
    val avg_per_property_def = triples.filter(triple => (triple.getObject.matches(XSD.xint.asNode())
      | triple.getObject.matches(XSD.xfloat.asNode()) | triple.getObject.matches(XSD.dateTime.asNode())))

    val sumCountPair = avg_per_property_def.map((_, 1)).combineByKey(
      (x: Int) => (x.toDouble, 1),
      (pair1: (Double, Int), x: Int) => (pair1._1 + x, pair1._2 + 1),
      (pair1: (Double, Int), pair2: (Double, Int)) => (pair1._1 + pair2._1, pair1._2 + pair2._2))
    val average = sumCountPair.map(x => (x._1, (x._2._1 / x._2._2)))
    average
  }

}

class Used_Classes(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  // ?p=rdf:type && isIRI(?o)
  def Filter(): RDD[Node] = triples.filter(f =>
    f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.isURI())
    .map(_.getObject)

  // M[?o]++
  def Action(): RDD[(Node, Int)] = Filter()
    .map(f => (f, 1))
    .reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(Node, Int)] = Action().sortBy(_._2, false)
    .take(100)

  def Voidify(): RDD[String] = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:classPartition "

    val classes = spark.sparkContext.parallelize(PostProc())
    val vc = classes.map(t => "[ \nvoid:class " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var cl_a = new Array[String](1)
    cl_a(0) = "\nvoid:classes " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(triplesString)
    val c = spark.sparkContext.parallelize(cl_a)
    if (classes.count() > 0) {
      c.union(c_p).union(vc)
    } else c.union(vc)
  }
}
object Used_Classes {

  def apply(triples: RDD[Triple], spark: SparkSession): Used_Classes = new Used_Classes(triples, spark)

}

class Classes_Defined(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  // ?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  def Filter(): RDD[Triple] = triples.filter(f =>
    (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(RDFS.Class.asNode()))
      || (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(OWL.Class.asNode()))
      && !f.getSubject.isURI())

  // M[?o]++
  def Action(): RDD[Node] = Filter().map(_.getSubject).distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}
object Classes_Defined {

  def apply(triples: RDD[Triple], spark: SparkSession): Classes_Defined = new Classes_Defined(triples, spark)
}

class PropertiesDefined(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter(): RDD[Triple] = triples.filter(f =>
    (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(OWL.ObjectProperty.asNode()))
      || (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(RDF.Property.asNode()))
      && !f.getSubject.isURI())

  def Action(): RDD[Node] = Filter().map(_.getPredicate).distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:properties  " + PostProc() + ";"
    spark.sparkContext.parallelize(cd)
  }
}
object PropertiesDefined {

  def apply(triples: RDD[Triple], spark: SparkSession): PropertiesDefined = new PropertiesDefined(triples, spark)
}

class PropertyUsage(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter(): RDD[Triple] = triples

  // M[?p]++
  def Action(): RDD[(Node, Int)] = Filter().map(_.getPredicate)
    .map(f => (f, 1))
    .reduceByKey(_ + _)

  // top(M,100)
  def PostProc(): Array[(Node, Int)] = Action().sortBy(_._2, false)
    .take(100)

  def Voidify(): RDD[String] = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:propertyPartition "

    val properties = spark.sparkContext.parallelize(PostProc())
    val vp = properties.map(t => "[ \nvoid:property " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var pl_a = new Array[String](1)
    pl_a(0) = "\nvoid:properties " + Action().map(f => f._1).distinct().count + ";"
    val c_p = spark.sparkContext.parallelize(triplesString)
    val p = spark.sparkContext.parallelize(pl_a)
    p.union(c_p).union(vp)
  }
}
object PropertyUsage {

  def apply(triples: RDD[Triple], spark: SparkSession): PropertyUsage = new PropertyUsage(triples, spark)

}

class DistinctEntities(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter(): RDD[Triple] = triples.filter(f =>
    (f.getSubject.isURI() && f.getPredicate.isURI() && f.getObject.isURI()))

  def Action(): RDD[Triple] = Filter().distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:entities  " + PostProc() + ";"
    spark.sparkContext.parallelize(ents)
  }
}
object DistinctEntities {

  def apply(triples: RDD[Triple], spark: SparkSession): DistinctEntities = new DistinctEntities(triples, spark)
}

class DistinctSubjects(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter(): RDD[Node] = triples.filter(f => f.getSubject.isURI()).map(_.getSubject)

  def Action(): RDD[Node] = Filter().distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctSubjects  " + PostProc() + ";"
    spark.sparkContext.parallelize(ents)
  }
}
object DistinctSubjects {

  def apply(triples: RDD[Triple], spark: SparkSession): DistinctSubjects = new DistinctSubjects(triples, spark)
}

class DistinctObjects(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter(): RDD[Node] = triples.filter(f => f.getObject.isURI()).map(_.getObject)

  def Action(): RDD[Node] = Filter().distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): RDD[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctObjects  " + PostProc() + ";"
    spark.sparkContext.parallelize(ents)
  }
}
object DistinctObjects {

  def apply(triples: RDD[Triple], spark: SparkSession): DistinctObjects = new DistinctObjects(triples, spark)
}

class SPO_Vocabularies(triples: RDD[Triple], spark: SparkSession) extends Serializable {

  def Filter(): RDD[Triple] = triples

  def Action(node: Node): RDD[String] = Filter().map(f => node.getNameSpace()).cache()

  def SubjectVocabulariesAction(): RDD[String] = Filter().filter(_.getSubject.isURI()).map(f => (f.getSubject.getNameSpace()))

  def SubjectVocabulariesPostProc(): RDD[(String, Int)] = SubjectVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def PredicateVocabulariesAction(): RDD[String] = Filter().filter(_.getPredicate.isURI()).map(f => (f.getPredicate.getNameSpace()))

  def PredicateVocabulariesPostProc(): RDD[(String, Int)] = PredicateVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def ObjectVocabulariesAction(): RDD[String] = Filter().filter(_.getObject.isURI()).map(f => (f.getObject.getNameSpace()))

  def ObjectVocabulariesPostProc(): RDD[(String, Int)] = ObjectVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def PostProc(node: Node): RDD[(String, Int)] = Filter().map(f => node.getNameSpace())
    .map(f => (f, 1)).reduceByKey(_ + _)

  def Voidify(): RDD[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:vocabulary  <" + SubjectVocabulariesAction().union(PredicateVocabulariesAction()).union(ObjectVocabulariesAction()).distinct().take(15).mkString(">, <") + ">;"
    spark.sparkContext.parallelize(ents)
  }
}
object SPO_Vocabularies {

  def apply(triples: RDD[Triple], spark: SparkSession): SPO_Vocabularies = new SPO_Vocabularies(triples, spark)
}
