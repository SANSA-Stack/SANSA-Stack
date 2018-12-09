package net.sansa_stack.ml.spark.clustering.datatypes

import net.sansa_stack.ml.spark.clustering.datatypes.dbstatusEnum._

case class DBPOI(val poiId: String,
                 val lon: Double,
                 val lat: Double) {

    var dbstatus = UNDEFINED
    var isDense = false
    var isBoundary = false
    var clusterName = ""
}

