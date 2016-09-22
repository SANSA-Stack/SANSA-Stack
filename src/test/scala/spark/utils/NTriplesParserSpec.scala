package spark.utils

import net.sansa.rdf.spark.utils.NTriplesParser
import org.apache.jena.graph.Node_URI
import org.apache.jena.graph.Node_Literal
import org.apache.jena.graph.Node_Blank
import org.apache.jena.graph.NodeFactory

import org.scalatest._
import spark.UnitSpec

class NTriplesParserSpec extends UnitSpec {

  val spoString: String = "<http://one.example/subject1> <http://one.example/predicate1> <http://one.example/object1> . # comments here"
  val splString: String = "<http://one.example/subject1> <http://one.example/predicate1> \"object1\" . "
  val bpoString: String = "_:subject1 <http://one.example/predicate1> <http://one.example/object1> . # comments here"
  val bplString: String = "_:subject1 <http://one.example/predicate1> \"object1\" . "

  it should "parse subject-predicate-object string with comment as (String, String, String) tuple" in {
    val triple = NTriplesParser.parseTriple(spoString)
    assert(triple._1.isInstanceOf[String])
    assert(triple._2.isInstanceOf[String])
    assert(triple._3.isInstanceOf[String])
    assert(triple._1 === "http://one.example/subject1")
    assert(triple._2 === "http://one.example/predicate1")
    assert(triple._3 === "http://one.example/object1")
  }

  it should "parse subject-predicate-literal string as (String, String, String) tuple" in {
    val triple = NTriplesParser.parseTriple(splString)
    assert(triple._1.isInstanceOf[String])
    assert(triple._2.isInstanceOf[String])
    assert(triple._3.isInstanceOf[String])
    assert(triple._1 === "http://one.example/subject1")
    assert(triple._2 === "http://one.example/predicate1")
    assert(triple._3 === "object1")
  }

  it should "parse blank-predicate-object string as (String, String, String) tuple" in {
    val triple = NTriplesParser.parseTriple(bpoString)
    //TODO: we need to depricate parseTriple method as it can not work with blanks
    assert(triple._1.isInstanceOf[String])
    assert(triple._2.isInstanceOf[String])
    assert(triple._3.isInstanceOf[String])
    assert(triple._1 === "_:subject1", "blanks can not be parsed consistently, hash is generated differently everytime")
    assert(triple._2 === "http://one.example/predicate1")
    assert(triple._3 === "http://one.example/object1")
  }

  it should "parse blank-predicate-literal string as (String, String, String) tuple" in {
    val triple = NTriplesParser.parseTriple(bplString)
    assert(triple._1.isInstanceOf[String])
    assert(triple._2.isInstanceOf[String])
    assert(triple._3.isInstanceOf[String])
    assert(triple._1 === "_:subject1", "blanks can not be parsed consistently, hash is generated differently everytime")
    assert(triple._2 === "http://one.example/predicate1")
    assert(triple._3 === "object1")
  }

  it should "parse subject-predicate-object string with comment as (Node, Node, Node) tuple" in {
    val triple = NTriplesParser.parseTripleAsNode(spoString)

    assert(triple._1.isInstanceOf[Node_URI])
    val subject = NodeFactory.createURI("http://one.example/subject1")
    assert(triple._1.equals(subject))

    assert(triple._2.isInstanceOf[Node_URI])
    val predicate = NodeFactory.createURI("http://one.example/predicate1")
    assert(triple._2.equals(predicate))

    assert(triple._3.isInstanceOf[Node_URI])
    val _object = NodeFactory.createURI("http://one.example/object1")
    assert(triple._3.equals(_object))
  }

  it should "parse subject-predicate-literal string as (Node, Node, Node) tuple" in {
    val triple = NTriplesParser.parseTripleAsNode(splString)

    assert(triple._1.isInstanceOf[Node_URI])
    val subject = NodeFactory.createURI("http://one.example/subject1")
    assert(triple._1.equals(subject))

    assert(triple._2.isInstanceOf[Node_URI])
    val predicate = NodeFactory.createURI("http://one.example/predicate1")
    assert(triple._2.equals(predicate))

    assert(triple._3.isInstanceOf[Node_Literal])
    val _object = NodeFactory.createLiteral("object1")
    assert(triple._3.equals(_object))
  }

  it should "parse blank-predicate-object string as (Node, Node, Node) tuple" in {
    val triple = NTriplesParser.parseTripleAsNode(bpoString)

    assert(triple._1.isInstanceOf[Node_Blank])
    val subject = NodeFactory.createBlankNode("_:subject1")
    assert(triple._1.equals(subject), "blanks can not be parsed consistently, hash is generated differently everytime")

    assert(triple._2.isInstanceOf[Node_URI])
    val predicate = NodeFactory.createURI("http://one.example/predicate1")
    assert(triple._2.equals(predicate))

    assert(triple._3.isInstanceOf[Node_URI])
    val _object = NodeFactory.createURI("http://one.example/object1")
    assert(triple._3.equals(_object))
  }

  it should "parse blank-predicate-literal string as (Node, Node, Node) tuple" in {
    val triple = NTriplesParser.parseTripleAsNode(bplString)

    assert(triple._1.isInstanceOf[Node_Blank])
    val subject = NodeFactory.createBlankNode("_:subject1")
    assert(triple._1.equals(subject), "blanks can not be parsed consistently, hash is generated differently everytime")

    assert(triple._2.isInstanceOf[Node_URI])
    val predicate = NodeFactory.createURI("http://one.example/predicate1")
    assert(triple._2.equals(predicate))

    assert(triple._3.isInstanceOf[Node_URI])
    val _object = NodeFactory.createLiteral("object1")
    assert(triple._3.equals(_object))
  }

}
