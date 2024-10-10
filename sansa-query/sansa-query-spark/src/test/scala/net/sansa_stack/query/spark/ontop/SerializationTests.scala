package net.sansa_stack.query.spark.ontop

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.parameters.Imports
import uk.ac.manchester.cs.owl.owlapi.OWLOntologyImpl
import uk.ac.manchester.cs.owl.owlapi.concurrent.ConcurrentOWLOntologyImpl

import java.io._

/**
 * Some tests regarding Ontop related objects w/o Spark and w/o Kryo.
 *
 * @author Lorenz Buehmann
 */
class SerializationTests
  extends AnyFunSuite
    with SharedSparkContext
    with BeforeAndAfterEach {

  var output: Output = _
  var input: Input = _

  var ont: OWLOntology = _

  val file: File = new File("/tmp/file.dat")
  file.deleteOnExit()

  override def beforeAll(): Unit = {
    super.beforeAll()

    output = new Output(new FileOutputStream(file))
    input = new Input(new FileInputStream(file))

    val man = OWLManager.createOWLOntologyManager()

    ont = man.createOntology()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    output = new Output(new FileOutputStream(file))
    input = new Input(new FileInputStream(file))
  }

  override def afterAll(): Unit = {
    super.afterAll()

    output.close()
    input.close()
  }

  override def conf(): SparkConf = {
    val conf = super.conf
    conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.crossJoin.enabled", "true")
//      .set("spark.kryo.registrationRequired", "true")

    conf.registerKryoClasses(Array(classOf[ConcurrentOWLOntologyImpl]))
    conf
  }

  test("OWLOntology Spark broadcast") {
    val ontBC = sc.broadcast(ont)

    val cnt = sc
      .parallelize(Seq(1, 2))
      .map(n => n + ontBC.value.getAxiomCount(Imports.EXCLUDED))
      .count()

    assert(cnt == 2)
  }

  test("OWLOntology Kryo serialization") {
    val kryo = new Kryo()

    val thrown = intercept[KryoException] {
      kryo.writeClassAndObject(output, ont)
      output.close()

      val inputOnt = kryo.readClassAndObject(input)
    }
  }

  test("OWLOntology Kryo serialization with Java Serializer") {
    val kryo = new Kryo()
    kryo.register(classOf[ConcurrentOWLOntologyImpl], new JavaSerializer())
    kryo.register(classOf[OWLOntologyImpl], new JavaSerializer())

    kryo.writeClassAndObject(output, ont)
    output.close()

    val inputOnt = kryo.readClassAndObject(input)

    assert(ont == inputOnt, "not the same object")
  }

  test("OWLOntology Java serialization") {

    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(ont)
    oos.close()

    val ois = new ObjectInputStream(new FileInputStream(file))
    val inputOnt = ois.readObject()
    ois.close()

    assert(ont == inputOnt, "not the same object")
  }

}
