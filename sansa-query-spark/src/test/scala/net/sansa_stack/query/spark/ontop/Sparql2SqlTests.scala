package net.sansa_stack.query.spark.sparql2sql

import java.io.InputStream

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.query._
import net.sansa_stack.query.spark.sparql2sql._
import org.scalatest.FunSuite

class Sparql2SqlTests extends FunSuite with DataFrameSuiteBase {

  test("Test Ontop") {

    val mappingsFile = getClass.getResource("/mappings/mappings.ttl").getPath
    val sparqlFile = getClass.getResource("/mappings/q1.rq").getPath
    val owlFile = getClass.getResource("/mappings/onto.owl").getPath
    val propertyFile = getClass.getResource("/mappings/properties.conf").getPath
    // val result = spark.sparqlDL(query, mappingsFile, configFile)

    val sqlObtained = Sparql2Sql.obtainSQL( sparqlFile, mappingsFile, owlFile, propertyFile)
    println(sqlObtained)

    // val stream : InputStream = getClass.getResourceAsStream(sparqlFile)
    // val lines = scala.io.Source.fromInputStream(stream).getLines

    val res = 1

    assert(res == 1)
  }
}
