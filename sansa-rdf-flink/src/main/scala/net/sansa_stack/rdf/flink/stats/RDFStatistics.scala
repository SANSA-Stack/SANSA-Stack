package net.sansa_stack.rdf.flink.stats

import java.io.File
import java.io.StringWriter

import scala.reflect.ClassTag

import net.sansa_stack.rdf.flink.utils.Logging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.RDFS
import org.apache.jena.vocabulary.OWL

/**
 * A Distributed implementation of RDF Statisctics using Apache Flink.
 *
 * @author Gezim Sejdiu
 */
object RDFStatistics extends Serializable with Logging {
  val env = ExecutionEnvironment.getExecutionEnvironment

  /**
   * Compute distributed RDF dataset statistics.
   * @param triples DataSet graph
   * @return VoID description of the given dataset
   */
  def run(triples: DataSet[Triple]): DataSet[String] = {
    Used_Classes(triples, ExecutionEnvironment.getExecutionEnvironment).Voidify
      .union(DistinctEntities(triples, env).Voidify)
      .union(DistinctSubjects(triples, env).Voidify)
      .union(DistinctObjects(triples, env).Voidify)
      .union(PropertyUsage(triples, env).Voidify)
      .union(SPO_Vocabularies(triples, env).Voidify)
  }

  /**
   * Voidify RDF dataset based on the Vocabulary of Interlinked Datasets (VoID) [[https://www.w3.org/TR/void/]]
   *
   * @param stats given RDF dataset statistics
   * @param source name of the Dataset:source--usualy the file's name
   * @param output the directory to save RDF dataset summary
   */
  def voidify(stats: DataSet[String], source: String, output: String): Unit = {
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

    val voidify = prefix.concat(src).concat(stats.setParallelism(1).collect().mkString).concat(end)
    println("\n" + voidify)
    pw.write(voidify)
    val vidifyStats = env.fromCollection(Seq(pw.toString()))
    vidifyStats.writeAsText(output, writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(1)
  }

  /**
   * Prints the Voidiy version of the given RDF dataset
   *
   * @param stats given RDF dataset statistics
   * @param source name of the Dataset:source--usualy the file's name
   */
  def print(stats: DataSet[String], source: String): Unit = {
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

    val voidify = prefix.concat(src).concat(stats.setParallelism(1).collect().mkString).concat(end)
    println("\n" + voidify)
  }

}

class Used_Classes(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  // ?p=rdf:type && isIRI(?o)
  def Filter(): DataSet[Triple] = triples.filter(f =>
    f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.isURI())

  // M[?o]++
  def Action(): AggregateDataSet[(Node, Int)] = Filter().map(f => f.getObject)
    .map(f => (f, 1))
    .groupBy(0)
    .sum(1)

  // top(M,100)
  def PostProc(): Seq[(Node, Int)] = Action().collect().sortBy(_._2).take(100)

  def Voidify(): DataSet[String] = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:classPartition "

    val classes = env.fromCollection(PostProc())
    val vc = classes.map(t => "[ \nvoid:class " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var cl_a = new Array[String](1)
    cl_a(0) = "\nvoid:classes " + Action().map(f => f._1).distinct().count
    val c_p = env.fromCollection(triplesString)
    val c = env.fromCollection(cl_a)
    c.union(c_p).union(vc)
  }
}
object Used_Classes {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): Used_Classes = new Used_Classes(triples, env)

}

class Classes_Defined(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  // ?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  def Filter(): DataSet[Triple] = triples.filter(f =>
    (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(RDFS.Class.asNode()))
      || (f.getPredicate.matches(RDF.`type`.asNode())  && f.getObject.matches(OWL.Class.asNode()))
      && !f.getSubject.isURI())

  // M[?o]++
  def Action(): DataSet[Node] = Filter().map(_.getSubject).distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): DataSet[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    env.fromCollection(cd)
  }
}
object Classes_Defined {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): Classes_Defined = new Classes_Defined(triples, env)
}

class PropertiesDefined(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter(): DataSet[Triple] = triples.filter(f =>
    (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(OWL.ObjectProperty.asNode()))
      || (f.getPredicate.matches(RDF.`type`.asNode()) && f.getObject.matches(RDF.Property.asNode()))
      && !f.getSubject.isURI())
  def Action(): DataSet[Node] = Filter().map(_.getPredicate).distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): DataSet[String] = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:properties  " + PostProc() + ";"
    env.fromCollection(cd)
  }
}
object PropertiesDefined {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): PropertiesDefined = new PropertiesDefined(triples, env)
}

class PropertyUsage(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter(): DataSet[Triple] = triples

  // M[?p]++
  def Action(): AggregateDataSet[(Node, Int)] = Filter().map(_.getPredicate)
    .map(f => (f, 1))
    .groupBy(0)
    .sum(1)

  // top(M,100)
  def PostProc(): Seq[(Node, Int)] = Action().collect().sortBy(_._2).take(100)

  def Voidify(): DataSet[String] = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:propertyPartition "

    val properties = env.fromCollection(PostProc())
    val vp = properties.map(t => "[ \nvoid:property " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var pl_a = new Array[String](1)
    pl_a(0) = "\nvoid:properties " + Action().map(f => f._1).distinct().count
    val c_p = env.fromCollection(triplesString)
    val p = env.fromCollection(pl_a)
    p.union(c_p).union(vp)
  }
}
object PropertyUsage {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): PropertyUsage = new PropertyUsage(triples, env)
}

class DistinctEntities(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter(): DataSet[Triple] = triples.filter(f =>
    (f.getSubject.isURI() && f.getPredicate.isURI() && f.getObject.isURI()))

  def Action(): DataSet[Triple] = Filter().distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): DataSet[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:entities  " + PostProc() + ";"
    env.fromCollection(ents)
  }
}
object DistinctEntities {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): DistinctEntities = new DistinctEntities(triples, env)
}

class DistinctSubjects(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter(): DataSet[Triple] = triples.filter(f => f.getSubject.isURI())

  def Action(): DataSet[Triple] = Filter().distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): DataSet[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctSubjects  " + PostProc() + ";"
    env.fromCollection(ents)
  }
}
object DistinctSubjects {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): DistinctSubjects = new DistinctSubjects(triples, env)
}

class DistinctObjects(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter(): DataSet[Triple] = triples.filter(f => f.getObject.isURI())

  def Action(): DataSet[Triple] = Filter().distinct()

  def PostProc(): Long = Action().count()

  def Voidify(): DataSet[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctObjects  " + PostProc() + ";"
    env.fromCollection(ents)
  }
}
object DistinctObjects {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): DistinctObjects = new DistinctObjects(triples, env)
}

class SPO_Vocabularies(triples: DataSet[Triple], env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter(): DataSet[Triple] = triples

  def Action(node: Node): DataSet[String] = Filter().map(f => node.getNameSpace())

  def SubjectVocabulariesAction(): DataSet[String] = Filter().filter(f => f.getSubject.isURI()).map(f => (f.getSubject.getNameSpace()))
  def SubjectVocabulariesPostProc(): AggregateDataSet[(String, Int)] = SubjectVocabulariesAction()
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def PredicateVocabulariesAction(): DataSet[String] = Filter().filter(f => f.getPredicate.isURI()).map(f => (f.getPredicate.getNameSpace()))
  def PredicateVocabulariesPostProc(): AggregateDataSet[(String, Int)] = PredicateVocabulariesAction()
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def ObjectVocabulariesAction(): DataSet[String] = Filter().filter(f => f.getObject.isURI()).map(f => (f.getObject.getNameSpace()))
  def ObjectVocabulariesPostProc(): AggregateDataSet[(String, Int)] = ObjectVocabulariesAction()
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def PostProc(node: Node): AggregateDataSet[(String, Int)] = Filter().map(f => node.getNameSpace())
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def Voidify(): DataSet[String] = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:vocabulary  <" + SubjectVocabulariesAction().union(PredicateVocabulariesAction()).union(ObjectVocabulariesAction()).distinct().collect.take(15).mkString(">, <") + ">;"
    env.fromCollection(ents)
  }
}
object SPO_Vocabularies {

  def apply(triples: DataSet[Triple], env: ExecutionEnvironment): SPO_Vocabularies = new SPO_Vocabularies(triples, env)
}
