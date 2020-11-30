package net.sansa_stack.query.spark.ontop

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLOntology
import uk.ac.manchester.cs.owl.owlapi.concurrent.ConcurrentOWLOntologyImpl

/**
 * @author Lorenz Buehmann
 */
class SerializationTests
  extends FunSuite
//  with BeforeAndAfterAll
with SharedSparkContext {
//    with DataFrameSuiteBase {

  var output: Output = _
  var input: Input = _

  var ont: OWLOntology = _


  override def beforeAll(): Unit = {
    super.beforeAll()

    output = new Output(new FileOutputStream("/tmp/file.dat"))
    input = new Input(new FileInputStream("/tmp/file.dat"))

    val man = OWLManager.createOWLOntologyManager()

    ont = man.createOntology()
  }

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.crossJoin.enabled", "true")
//      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    conf
  }

  test("OWLOntology Spark broadcast") {
    val ontBC = sc.broadcast(ont)

    sc
      .parallelize(Seq(1, 2))
      .map(n => n + ontBC.value.getAxiomCount(false))
      .collect()
      .foreach(println)
  }

  test("OWLOntology Kryo serialization") {
    intercept[KryoException] {
      val kryo = new Kryo()

      kryo.writeClassAndObject(output, ont)
      output.close()

      val inputOnt = kryo.readClassAndObject(input)

      assert(ont == inputOnt, "not the same object")
    }
  }

  test("OWLOntology Kryo serialization with Java Serializer") {
    val kryo = new Kryo()
    kryo.register(classOf[ConcurrentOWLOntologyImpl], new JavaSerializer())

    kryo.writeClassAndObject(output, ont)
    output.close()

    val inputOnt = kryo.readClassAndObject(input)

    assert(ont == inputOnt, "not the same object")

  }

  test("OWLOntology Java serialization") {

    val oos = new ObjectOutputStream(new FileOutputStream("/tmp/file.dat"))
    oos.writeObject(ont)
    oos.close()

    val ois = new ObjectInputStream(new FileInputStream("/tmp/file.dat"))
    val inputOnt = ois.readObject()
    ois.close()

    assert(ont == inputOnt, "not the same object")
  }

}
