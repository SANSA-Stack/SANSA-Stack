package net.sansa_stack.query.spark.ontop

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.shared.impl.PrefixMappingImpl
import org.apache.jena.sparql.serializer.SerializationContext
import org.apache.jena.sparql.util.FmtUtils
import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{HasDataPropertiesInSignature, HasObjectPropertiesInSignature, IRI}

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.mutable



/**
 * Property table partitioner.
 *
 * (s,p,o)
 *
 * {{{
 * +-----+-------------------+------+-----+-------+
 * |  s  |        p1         |  p2  | ... |  pn   |
 * +-----+-------------------+------+-----+-------+
 * | s1  | [o11,o12,...,o1n] | null | ... | [on1] |
 * | s2  |                   |      |     |       |
 * | ... |                   |      |     |       |
 * | sn  |                   |      |     |       |
 * +-----+-------------------+------+-----+-------+
 *}}}
 *
 * @author Lorenz Buehmann
 */
object PropertyTablePartitioner {

  case class Config(
                     inputPath: URI = null,
                     outputPath: URI = null,
                     schemaPath: URI = null,
                     tableName: String = "triples",
                     computeStatistics: Boolean = true,
                     overwrite: Boolean = false
                   )

  val parser = new scopt.OptionParser[Config]("PropertyTablePartitioner") {
    head("property table partitioner", "0.1")
    opt[URI]('i', "input")
      .required()
      .action((x, c) => c.copy(inputPath = x))
      .text("path to input data")
    opt[URI]('o', "output")
      .required()
      .action((x, c) => c.copy(outputPath = x))
      .text("path to output directory")
    opt[URI]('s', "schema")
      .optional()
      .action((x, c) => c.copy(schemaPath = x))
      .text("an optional file containing the OWL schema to process only object and data properties")
    opt[String]('t', "tableName")
      .optional()
      .action((x, c) => c.copy(tableName = x))
      .text("the table name")
    opt[Boolean]('s', "stats")
      .action((x, c) => c.copy(computeStatistics = x))
      .text("compute statistics")
    opt[Unit]("overwrite")
      .action((_, c) => c.copy(overwrite = true))
      .text("overwrite table if exists")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
    }
  }

  private def run(config: Config): Unit = {
    val spark = SparkSession.builder
            .master("local")
      .appName("property table partitioner")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.kryo.registrationRequired", "true")
      // .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      //      .config("spark.default.parallelism", "4")
      //      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", config.outputPath.getPath)
      .config("spark.sql.cbo.enabled", true)
      .config("spark.sql.statistics.histogram.enabled", true)
      .enableHiveSupport()
      .getOrCreate()


    if (!spark.catalog.tableExists("triples") || config.overwrite) {

      // drop existing table if overwrite was forced
      if (config.overwrite) {
        println("overwriting ...")
        spark.sql(s"DROP TABLE IF EXISTS `${config.tableName}`")
      }

      // read triples as RDD[Triple]
      var triplesRDD: RDD[Triple] = spark.ntriples()(config.inputPath.getPath)

      // filter properties if schema ontology was given
      if (config.schemaPath != null) {
        // load ontology
        val ont = OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(IRI.create(config.schemaPath))
        // get all object properties
        val objectProperties = ont.asInstanceOf[HasObjectPropertiesInSignature].getObjectPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet
        // get all data properties
        val dataProperties = ont.asInstanceOf[HasDataPropertiesInSignature].getDataPropertiesInSignature.iterator().asScala.map(_.toStringID).toSet
        // combine
        val schemaProperties = objectProperties ++ dataProperties ++ mutable.Set(RDF.`type`.getURI)
        // filter triples RDD
        triplesRDD = triplesRDD.filter(t => schemaProperties.contains(t.getPredicate.getURI))
      }
      triplesRDD.cache()

      // get all properties in the RDD
      val properties: List[Node] = triplesRDD.map(t => t.getPredicate.getURI).distinct().collect()
                                            .toList.sorted.map(NodeFactory.createURI)
      //    println(properties)


      def seqOp (accumulator: MultiMap, element: (Node, Node)): MultiMap = {
        accumulator.addBinding(element._1, element._2)
        accumulator
      }

      def combOp (accumulator1: MultiMap, accumulator2: MultiMap): MultiMap = {
        for ( (k, vs) <- accumulator1; v <- vs ) accumulator2.addBinding(k, v)
        accumulator2
      }

      val zero = new MultiMap()

      // create the rows for the Dataframe
      val rows = triplesRDD
        .map(t => (t.getSubject, (t.getPredicate, t.getObject))) // (s, p, o) -> (s, (p, o))
        .aggregateByKey(zero)(seqOp, combOp) // (s, (p, o)) -> (s, Map(p -> {o1, ..., on}))
        .map(e => {
          val s = e._1
          val prop2Values = e._2
          // for each property in the dataset we create either i) an Array with the values or ii) null if no value exists
          val cols = List(s.getURI) ++ properties.map(p => {
            prop2Values.get(p) match {
              case Some(vals) => vals.map(v => v.toString()).toArray
              case None => null // Array(null.asInstanceOf[String])
            }
          })
          //        println(cols)
          Row(cols: _*)
        })

      // TODO create column names
      val prop2ColumnName = properties.map(p => (p.getLocalName, p)).groupBy(_._1).flatMap(e =>
        if (e._2.size > 1) {
          e._2.map(el => (el._2, FmtUtils.stringForNode(el._2)))
        } else {
          e._2
        }
      )

      val pm = new PrefixMappingImpl()
      pm.setNsPrefix("dbo", "http://dbpedia.org/ontology/")
      val ctx = new SerializationContext()
      ctx.setPrefixMapping(pm)

      // create the Dataframe schema
      val schema = (List(StructField("s", StringType, nullable = true)) ++
                    properties.map(p => StructField(FmtUtils.stringForNode(p, pm), ArrayType(StringType, containsNull = false), nullable = true))
                  ).foldLeft(new StructType())(_ add _)
      //    println(schema)

      // create the Dataframe
      val df = spark.createDataFrame(rows, schema)
      df.createOrReplaceTempView("triples")
      df.show(false)
      df.printSchema()

      // write table to disk
      df.repartition(10)
        .write
        .format("parquet")
        .bucketBy(10, "s")
        .sortBy("s")
        .saveAsTable(config.tableName)
    }

    spark.sql("select * FROM triples WHERE s='http://dbpedia.org/resource/Aaron_Dennis'").show(false)
    spark.sql("select * FROM triples WHERE s='http://dbpedia.org/resource/Aaron_King'").show(false)


  }

  type MultiMap = mutable.HashMap[Node, mutable.Set[Node]]

  implicit class ListMultiMap[A, B](map: mutable.Map[A, mutable.Set[B]]) {
    protected def makeSet: mutable.Set[B] = new mutable.HashSet[B]
    def addBinding(key: A, value: B): ListMultiMap[A, B] = {
      map.get(key) match {
        case None =>
          val set = makeSet
          set += value
          map(key) = set
        case Some(set) =>
          set += value
      }
      this
    }
  }

  class SetMultiMap[A, B] extends mutable.HashMap[A, mutable.Set[B]] with mutable.MultiMap[A, B] {}
}
