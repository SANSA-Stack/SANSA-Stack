package net.sansa_stack.query.spark.ops.rdd

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A ResultSetSpark comprises an RDD[Binding] and a Seq[Var].
 * Hence, operations on ResultSetSparks typically perform operations on the RDD[Binding]
 */
object ResultSetSparkOps {


  def getSparkSchema(classes: Class[_]*): DataType = {
    // val types = classes.map(getType(_))
//    val result = new StructType
//    result.add()
//
//    val types =
//
//    val sparkSchema = ScalaReflection.schemaFor(types)
//      .dataType // .asInstanceOf[StructType]
    null
  }

}
