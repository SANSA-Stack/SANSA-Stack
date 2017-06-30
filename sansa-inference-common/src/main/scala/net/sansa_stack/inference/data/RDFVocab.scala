package net.sansa_stack.inference.data

/**
  * @author Lorenz Buehmann
  */
class RDFVocab[Rdf <: RDF](implicit ops: RDFOps[Rdf]) {

  import ops._

  val `type`: Rdf#URI = makeUri("type")
}

object RDFVocab {
  def apply[Rdf <: RDF](implicit ops: RDFOps[Rdf]): RDFVocab[Rdf] = new RDFVocab[Rdf]()
}
