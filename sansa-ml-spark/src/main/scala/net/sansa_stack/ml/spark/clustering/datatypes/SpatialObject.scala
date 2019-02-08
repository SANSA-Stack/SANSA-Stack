package net.sansa_stack.ml.spark.clustering.datatypes

import com.vividsolutions.jts.geom.Geometry
import scala.collection.mutable.HashMap

class SpatialObject(
                     var id: String,
                     var name: String,
                     var keywords: List[String],
                     var score: Double,
                     var geometry: Geometry
                   ) extends Ordered[SpatialObject]{

    var attributes = HashMap[Object, Object]()

    // @Override
    override def compare(o: SpatialObject ): Int = {
        if (this.score > o.score)      -1
        else if (this.score == o.score) 0
        else 1
    }
}

