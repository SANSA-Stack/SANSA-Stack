package net.sansa_stack.rdf.spark.mappings

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.mappings._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class R2RMLMappingsTests extends FunSuite with DataFrameSuiteBase {

  test("creating the CREATE TABLE instructions from data: 25") {
    val path = getClass.getResource("/rdf.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(path)
    val creation = R2RMLMappings.loadSQLTables( triples, spark)

    creation.foreach(println)
    val size = creation.size

    assert(size == 25)
  }

  test("creating the INSERT INTO instructions from data: 100") {
    val path = getClass.getResource("/rdf.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(path)
    val insertion = R2RMLMappings.insertSQLTables( triples, spark)

    println("Printing the first 10 instructions:")
    insertion.take(10).foreach(println)
    val size = insertion.count

    assert(size == 100)
  }

  test("creating the R2RML mappings from data: 25") {
    val path = getClass.getResource("/rdf.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(path)
    val mappings = R2RMLMappings.generateR2RMLMappings( triples, spark)

    mappings.foreach(println)
    val size = mappings.size

    assert(size == 25)
  }

}
