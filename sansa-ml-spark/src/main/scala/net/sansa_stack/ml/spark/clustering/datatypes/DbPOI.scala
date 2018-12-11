package net.sansa_stack.ml.spark.clustering.datatypes

import net.sansa_stack.ml.spark.clustering.datatypes.DbStatusEnum._

case class DbPOI(val poiId: String,
                 val lon: Double,
                 val lat: Double) {

    var dbstatus = UNDEFINED
    var isDense = false
    var isBoundary = false
    var clusterName = ""
}

