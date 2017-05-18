package net.sansa_stack.inference.spark.data.loader.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import net.sansa_stack.inference.spark.data.model.TripleUtils._

import net.sansa_stack.inference.utils.NTriplesStringToRDFTriple

class NTriplesRelation(location: String, userSchema: StructType)
                      (@transient val sqlContext: SQLContext)
    extends BaseRelation
      with TableScan
      with Serializable {
    override def schema: StructType = {
      if (this.userSchema != null) {
        this.userSchema
      }
      else {
        StructType(
          Seq(
            StructField("s", StringType, true),
            StructField("p", StringType, true),
            StructField("o", StringType, true)
        ))
      }
    }
    override def buildScan(): RDD[Row] = {
      val rdd = sqlContext
        .sparkContext
        .textFile(location)

      val converter = new NTriplesStringToRDFTriple()

      val rows = rdd.flatMap(x => converter.apply(x)).map(t => Row.fromSeq(Seq(t.s, t.p, t.o)))

      rows
    }
  }