package org.dissect.rdf.spark.LODStats
import org.dissect.rdf.spark.model.TripleModel._
import org.apache.spark.rdd.RDD
import java.security.MessageDigest

class Rule(triples: RDD[Triple]) {

  def apply() = {

    triples.filter(f => f._1.equals(Rule.RDF_CLASS))
  }

}

object Rule {

  val hashLength = 8
  def getHashCode(triple: String) = {
    val mess = MessageDigest.getInstance("MD5")
    mess.reset()
    mess.update(triple.getBytes())
    val result = mess.digest()
    result.slice(result.length - hashLength, result.length).toSeq
  }

  val RDF_CLASS = getHashCode("http://www.w3.org/2000/01/rdf-schema#Class")
  val OWL_CLASS = getHashCode("http://www.w3.org/2002/07/owl#Class")
  val RDFS_CLASS = getHashCode("http://www.w3.org/2000/01/rdf-schema#Class")
  val RDFS_SUBCLASS_OF = getHashCode("http://www.w3.org/2000/01/rdf-schema#subClassOf")
  val RDFS_SUBPROPERTY_OF = getHashCode("http://www.w3.org/2000/01/rdf-schema#subPropertyOf")
  val RDF_TYPE = getHashCode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  val XSD_STRING = getHashCode("http://www.w3.org/2001/XMLSchema#string")
  val XSD_INT = getHashCode("http://www.w3.org/2001/XMLSchema#int")
  val XSD_float = getHashCode("http://www.w3.org/2001/XMLSchema#float")
  val XSD_datetime = getHashCode("http://www.w3.org/2001/XMLSchema#datetime")
  val OWL_SAME_AS = getHashCode("http://www.w3.org/2002/07/owl#sameAs")

}