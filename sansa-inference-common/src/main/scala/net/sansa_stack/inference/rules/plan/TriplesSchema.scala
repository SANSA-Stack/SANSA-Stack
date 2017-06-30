package net.sansa_stack.inference.rules.plan

import java.nio.charset.Charset

import com.google.common.io.Resources
import org.apache.calcite.model.ModelHandler
import org.apache.calcite.schema._

/**
  * @author Lorenz Buehmann
  */
object TriplesSchema {


  def get(): SchemaPlus = {

    val connection = new SimpleCalciteConnection()

    val triplesSchema = Resources.toString(TriplesSchema.getClass.getClassLoader.getResource("schema.json"), Charset.defaultCharset())

    // ModelHandler reads the triples schema and load the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + triplesSchema)


    connection.getRootSchema.getSubSchema(connection.getSchema)

    connection.getRootSchema.getSubSchema("TRIPLES")
  }
}
