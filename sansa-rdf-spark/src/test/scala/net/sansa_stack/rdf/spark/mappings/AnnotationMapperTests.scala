package net.sansa_stack.rdf.spark.mappings

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.mapper.Person
import org.aksw.jena_sparql_api.mapper.proxy.JenaPluginUtils
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class AnnotationMapperTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._
  import net.sansa_stack.rdf.spark.ops._

  test("extracting first names of people from natural trig resources should work") {
    JenaPluginUtils.scan(classOf[Person])

    val path = getClass.getResource("/people.trig").getPath

    val names = spark
      .datasets(Lang.TRIG)(path)
      .mapToNaturalResources()
      .mapAs(classOf[Person])
      .map(_.getFirstName)
      .collect()

    assert(names(0) == "Ana")
    assert(names(1) == "Bob")
  }

}