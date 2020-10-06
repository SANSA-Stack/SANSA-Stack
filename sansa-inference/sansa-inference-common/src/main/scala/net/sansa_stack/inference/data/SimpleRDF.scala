package net.sansa_stack.inference.data

/**
  * @author Lorenz Buehmann
  */
class SimpleRDF extends RDF {
    type Triple = RDFTriple
    type Node = String
    type URI = String
    type BNode = String
    type Literal = String
    type Lang = String
}
