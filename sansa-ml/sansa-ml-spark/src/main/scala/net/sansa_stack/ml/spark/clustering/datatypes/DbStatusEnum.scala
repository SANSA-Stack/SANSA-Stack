package net.sansa_stack.ml.spark.clustering.datatypes

object DbStatusEnum extends Enumeration {

    type DBSTATUS = Value
    val UNDEFINED, NOISE, PARTOFCLUSTER = Value
}
