package net.sansa_stack.query.spark.geospatial

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/**
 * @author Lorenz Buehmann
 */
object GeoSparkRunner {

  def main(args: Array[String]): Unit = {
    System.setProperty("geospark.global.charset", "utf8")

    val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)

    val counties = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(args(0))
    counties.createOrReplaceTempView("county")

    val countieGeom = spark.sql(
      """
        |SELECT county_code, st_geomFromWKT(geom) as geometry from county
    """.stripMargin)
    countieGeom.createOrReplaceTempView("counties")
    countieGeom.show()
    countieGeom.printSchema()

    val shapefileInputLocation = args(1)
    val spatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, shapefileInputLocation)
    val spatialDf2 = Adapter.toDf(spatialRDD, spark)
    spatialDf2.createOrReplaceTempView("pois")
    spatialDf2.show()
    spatialDf2.printSchema()

    val spatialJoinResult = spark.sql(
      """
        SELECT c.county_code, p.fclass
        FROM pois AS p, counties AS c
        WHERE ST_Intersects(p.geometry, c.geometry)
    """
    )

    spatialJoinResult.show()

    spark.stop()
  }

}
