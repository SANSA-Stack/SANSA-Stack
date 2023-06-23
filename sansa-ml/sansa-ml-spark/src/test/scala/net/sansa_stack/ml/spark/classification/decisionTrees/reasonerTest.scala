package net.sansa_stack.ml.spark.classification.decisionTrees

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner.StructuralReasoner
import net.sansa_stack.owl.spark.owl
import net.sansa_stack.owl.spark.owl.{OWLAxiomReader, Syntax}
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLDataFactory

class reasonerTest extends AnyFunSuite with SharedSparkContext{
  
  lazy val spark: SparkSession = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config("spark.kryo.registrator", "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()
  
  private val df: OWLDataFactory = OWLManager.getOWLDataFactory
  
  private val defaultPrefix = "http://example.com/foo#"
  
  val syntax: owl.Syntax.Value = Syntax.FUNCTIONAL
  
  val filePath: String = this.getClass.getClassLoader.getResource("trains.owl").getPath
  
  var _rdd: OWLAxiomsRDD = null
  
  def rdd: OWLAxiomsRDD = {
    if (_rdd == null) {
      _rdd = spark.owl(syntax)(filePath)
      _rdd.cache()
    }
    _rdd
  }
  
  
  
  
  test("Instance retrieval") {
  
    val kb: KB = new KB(filePath, rdd, spark)
    val reasoner: StructuralReasoner = new StructuralReasoner.StructuralReasoner(kb)
    val instances = reasoner.getInstances(df.getOWLClass(defaultPrefix + "car"), b = false).size()
    assert(instances == 30)
  }
  
  test("Entailment check") {
    
    val kb: KB = new KB(filePath, rdd, spark)
    val reasoner: StructuralReasoner = new StructuralReasoner.StructuralReasoner(kb)
    val concept = df.getOWLClass(defaultPrefix + "car")
    val individual = df.getOWLNamedIndividual(defaultPrefix + "car_101")
    val entailed = reasoner.isEntailed(df.getOWLClassAssertionAxiom(concept, individual))
    assert(entailed)
  }
  
  
  test("Satisfiability check") {
    
    val kb: KB = new KB(filePath, rdd, spark)
    val reasoner: StructuralReasoner = new StructuralReasoner.StructuralReasoner(kb)
    val concept = df.getOWLClass(defaultPrefix + "car")
    
    val Satisfiable = reasoner.isSatisfiable(df.getOWLObjectComplementOf(concept))
    assert(!Satisfiable)
  }
  
  
}
