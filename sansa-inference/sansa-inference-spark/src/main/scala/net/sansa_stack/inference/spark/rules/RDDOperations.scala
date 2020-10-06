package net.sansa_stack.inference.spark.rules

import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.data.RDFTriple

/**
  * Common operations on RDD of triples.
  *
  * @author Lorenz Buehmann
  */
object RDDOperations {


  /**
    * For a given triple (s,p,o) it returns (s,o)
    * @param triples the triples (s,p,o)
    * @return tuples (s,o)
    */
  def subjObj(triples: RDD[RDFTriple]): RDD[(String, String)] = {
    triples.map(t => (t.s, t.o))
  }

  /**
    * For a given triple (s,p,o) it returns (o,s)
    * @param triples the triples (s,p,o)
    * @return tuples (o,s)
    */
  def objSubj(triples: RDD[RDFTriple]): RDD[(String, String)] = {
    triples.map(t => (t.o, t.s))
  }

  /**
    * For a given triple (s,p,o) it returns (s,(p,o))
    * @param triples the triples (s,p,o)
    * @return tuples (s,(p,o))
    */
  def subjKeyPredObj(triples: RDD[RDFTriple]): RDD[(String, (String, String))] = {
    triples.map(t => t.s -> (t.p, t.o))
  }

  /**
    * For a given triple (s,p,o) it returns (s,(o,p))
    * @param triples the triples (s,p,o)
    * @return tuples (s,(o,p))
    */
  def subjKeyObjPred(triples: RDD[RDFTriple]): RDD[(String, (String, String))] = {
    triples.map(t => t.s -> (t.o, t.p))
  }

  /**
    * For a given triple (s,p,o) it returns (o,(s,p))
    * @param triples the triples (s,p,o)
    * @return tuples (o,(s,p))
    */
  def objKeySubjPred(triples: RDD[RDFTriple]): RDD[(String, (String, String))] = {
    triples.map(t => t.o -> (t.s, t.p))
  }

  /**
    * For a given triple (s,p,o) it returns (o,(p,s))
    * @param triples the triples (s,p,o)
    * @return tuples (o,(p,s))
    */
  def objKeyPredSubj(triples: RDD[RDFTriple]): RDD[(String, (String, String))] = {
    triples.map(t => t.o -> (t.p, t.s))
  }

  /**
    * For a given triple (s,p,o) it returns (p,(s,o))
    * @param triples the triples (s,p,o)
    * @return tuples (p,(s,o))
    */
  def predKeySubjObj(triples: RDD[RDFTriple]): RDD[(String, (String, String))] = {
    triples.map(t => t.p -> (t.s, t.o))
  }

  /**
    * For a given triple (s,p,o) it returns (p,(o,s))
    * @param triples the triples (s,p,o)
    * @return tuples (p,(o,s))
    */
  def predKeyObjSubj(triples: RDD[RDFTriple]): RDD[(String, (String, String))] = {
    triples.map(t => t.p -> (t.o, t.s))
  }

  /**
    * For a given triple (s,p,o) it returns ((s,p),o)
    * @param triples the triples (s,p,o)
    * @return tuples ((s,p),o)
    */
  def subjPredKeyObj(triples: RDD[RDFTriple]): RDD[((String, String), String)] = {
    triples.map(t => (t.s, t.p) -> t.o)
  }

  /**
    * For a given triple (s,p,o) it returns ((s,o),p)
    * @param triples the triples (s,p,o)
    * @return tuples ((s,o),p)
    */
  def subjObjKeyPred(triples: RDD[RDFTriple]): RDD[((String, String), String)] = {
    triples.map(t => (t.s, t.o) -> t.p)
  }

  /**
    * For a given triple (s,p,o) it returns ((o,p),s)
    * @param triples the triples (s,p,o)
    * @return tuples ((o,p),s)
    */
  def objPredKeySubj(triples: RDD[RDFTriple]): RDD[((String, String), String)] = {
    triples.map(t => (t.o, t.p) -> t.s)
  }

  /**
    * For a given tuple (x,y) it returns (y,x)
    *
    * @param tuples the tuples (x,y)
    * @return tuples (y,x)
    */
  def swap[A, B](tuples: RDD[(A, B)]): RDD[(B, A)] = {
    tuples.map(t => (t._2, t._1))
  }

}
