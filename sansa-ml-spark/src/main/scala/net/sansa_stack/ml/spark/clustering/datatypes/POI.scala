package net.sansa_stack.ml.spark.clustering.datatypes

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}

class POI(
          id: String,
          name: String,
          val x : Double,
          val y : Double,
          keywords: List[String],
          score: Double,
          geometryFactory: GeometryFactory
         ) extends SpatialObject(id, name, keywords, score, geometryFactory.createPoint(new Coordinate(x, y)))

