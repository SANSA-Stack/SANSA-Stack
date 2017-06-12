package net.sansa_stack.rdf.flink.stats

import net.sansa_stack.rdf.flink.data.RDFGraph
import net.sansa_stack.rdf.flink.utils.{ Logging, StatsPrefixes }
import net.sansa_stack.rdf.flink.data.RDFGraph
import scala.reflect.ClassTag
import net.sansa_stack.rdf.flink.model.RDFTriple
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

class RDFStatistics {

}

class Used_Classes(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  //?p=rdf:type && isIRI(?o)
  def Filter() = rdfgraph.triples.filter(f =>
    f.predicate.toString().equals(StatsPrefixes.RDF_TYPE) && f.`object`.isURI())

  //M[?o]++ 
  def Action() = Filter().map(f => f.`object`)
    .map(f => (f, 1))
    .groupBy(0)
    .sum(1)

  //top(M,100)
  def PostProc() = Action().collect().sortBy(_._2).take(100)

  def Voidify() = {

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

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new Used_Classes(rdfgraph, env).Voidify()

}

class Classes_Defined(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  //?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  def Filter() = rdfgraph.triples.filter(f =>
    (f.getPredicate.toString().equals(StatsPrefixes.RDF_TYPE) && f.getObject.toString().equals(StatsPrefixes.RDFS_CLASS))
      || (f.getPredicate.toString().equals(StatsPrefixes.RDF_TYPE) && f.getObject.toString().equals(StatsPrefixes.OWL_CLASS))
      && !f.getSubject.isURI())

  //M[?o]++ 
  def Action() = Filter().map(_.getSubject).distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    env.fromCollection(cd)
  }
}
object Classes_Defined {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new Classes_Defined(rdfgraph, env).Voidify()
}

class PropertiesDefined(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter() = rdfgraph.triples.filter(f =>
    (f.getPredicate.toString().equals(StatsPrefixes.RDF_TYPE) && f.getObject.toString().equals(StatsPrefixes.OWL_OBJECT_PROPERTY))
      || (f.getPredicate.toString().equals(StatsPrefixes.RDF_TYPE) && f.getObject.toString().equals(StatsPrefixes.RDF_PROPERTY))
      && !f.getSubject.isURI())
  def Action() = Filter().map(_.getPredicate).distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:properties  " + PostProc() + ";"
    env.fromCollection(cd)
  }
}
object PropertiesDefined {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new PropertiesDefined(rdfgraph, env).Voidify()
}

class PropertyUsage(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter() = rdfgraph.triples

  //M[?p]++
  def Action() = Filter().map(_.getPredicate)
    .map(f => (f, 1))
    .groupBy(0)
    .sum(1)

  //top(M,100)
  def PostProc() = Action().collect().sortBy(_._2).take(100)

  def Voidify() = {

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

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new PropertyUsage(rdfgraph, env).Voidify()
}

class DistinctEntities(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter() = rdfgraph.triples.filter(f =>
    (f.getSubject.isURI() && f.getPredicate.isURI() && f.getObject.isURI()))

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:entities  " + PostProc() + ";"
    env.fromCollection(ents)
  }
}
object DistinctEntities {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new DistinctEntities(rdfgraph, env).Voidify()
}

class DistinctSubjects(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter() = rdfgraph.triples.filter(f => f.getSubject.isURI())

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctSubjects  " + PostProc() + ";"
    env.fromCollection(ents)
  }
}
object DistinctSubjects {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new DistinctSubjects(rdfgraph, env).Voidify()
}

class DistinctObjects(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter() = rdfgraph.triples.filter(f => f.getObject.isURI())

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctObjects  " + PostProc() + ";"
    env.fromCollection(ents)
  }
}
object DistinctObjects {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment) = new DistinctObjects(rdfgraph, env).Voidify()
}

class SPO_Vocabularies(rdfgraph: RDFGraph, env: ExecutionEnvironment) extends Serializable with Logging {

  def Filter() = rdfgraph.triples

  def Action(node: org.apache.jena.graph.Node) = Filter().map(f => node.getNameSpace())

  def SubjectVocabulariesAction() = Filter().filter(f => f.getSubject.isURI()).map(f => (f.getSubject.getNameSpace()))
  def SubjectVocabulariesPostProc() = SubjectVocabulariesAction()
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def PredicateVocabulariesAction() = Filter().filter(f => f.getPredicate.isURI()).map(f => (f.getPredicate.getNameSpace()))
  def PredicateVocabulariesPostProc() = PredicateVocabulariesAction()
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def ObjectVocabulariesAction() = Filter().filter(f => f.getObject.isURI()).map(f => (f.getObject.getNameSpace()))
  def ObjectVocabulariesPostProc() = ObjectVocabulariesAction()
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def PostProc(node: org.apache.jena.graph.Node) = Filter().map(f => node.getNameSpace())
    .map(f => (f, 1)).groupBy(0)
    .sum(1)

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:vocabulary  <" + SubjectVocabulariesAction().union(PredicateVocabulariesAction()).union(ObjectVocabulariesAction()).distinct().collect.take(15).mkString(">, <") + ">;"
    env.fromCollection(ents)
  }
}
object SPO_Vocabularies {

  def apply(rdfgraph: RDFGraph, env: ExecutionEnvironment)  = new SPO_Vocabularies(rdfgraph, env).Voidify()
}




