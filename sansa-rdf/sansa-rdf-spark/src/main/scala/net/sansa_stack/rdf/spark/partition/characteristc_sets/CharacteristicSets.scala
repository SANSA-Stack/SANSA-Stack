package net.sansa_stack.rdf.spark.partition.characteristc_sets

import java.io.File
import java.util

import scala.util.matching.Regex

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.mxgraph.util.mxCellRenderer
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.serializer.SerializationContext
import org.apache.jena.sparql.util.FmtUtils
import org.apache.jena.util.SplitIRI
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, collect_set, count, lit, max, udf, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jgrapht.alg.{TransitiveClosure, TransitiveReduction}
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.io.{Attribute, DefaultAttribute, GraphMLExporter}
import org.jgrapht.io.AttributeType
import org.jgrapht.io.GraphMLExporter.AttributeCategory
import scala.collection.JavaConverters._

import org.jgrapht.alg.connectivity.KosarajuStrongConnectivityInspector

abstract class CharacteristicSetBase(val properties: Set[Node]) {
  /**
   * Given two CSs, ci and cj , and their property sets Pi and Pj , then ci subsumes cj when the property set of ci
   * is a proper subset of the property set of cj , or Pi ⊂ Pj .
   *
   * @param other the other CS
   * @return if this characteristic set subsumes the other
   */
  def subsumes(other: CharacteristicSetBase): Boolean = properties.subsetOf(other.properties)
}

case class CharacteristicSet(override val properties: Set[Node]) extends CharacteristicSetBase(properties) {}

case class ExtendedCharacteristicSet(override val properties: Set[Node], size: Long, isDense: Option[Boolean] = None) extends CharacteristicSetBase(properties) {}

class ExtendedCharacteristicSetGraph extends DirectedAcyclicGraph[ExtendedCharacteristicSet, DefaultEdge](classOf[DefaultEdge]) {

  lazy val denseNodes: Set[ExtendedCharacteristicSet] = vertexSet().asScala.filter(_.isDense.getOrElse(false)).toSet

  private[this] var value: Option[Boolean] = None
  def isInferred: Boolean = value.getOrElse(false)

  /**
   * cutting off descendants from dense nodes
   */
  def cutDescendantsOfDenseNodes(): Unit = {
    // remove all outgoing edges of the dense nodes
    denseNodes.flatMap(v => outgoingEdgesOf(v).asScala).foreach(removeEdge)
  }

  def computeClosure(): Unit = {
    if (!isInferred) {
      TransitiveClosure.INSTANCE.closeDirectedAcyclicGraph(this)
      value = Some(true)
    }
  }

  def ancestralSubgraphsOf(node: ExtendedCharacteristicSet): Unit = {

  }

}

object CharacteristicSetGraphBuilder {

  def create(css: Set[ExtendedCharacteristicSet]) : ExtendedCharacteristicSetGraph = {
    val g = new ExtendedCharacteristicSetGraph()

    for {cs1 <- css
         cs2 <- css
         if cs1 != cs2} {
      if (cs1.subsumes(cs2)) {
        g.addVertex(cs1)
        g.addVertex(cs2)
        g.addEdge(cs1, cs2)
      }
    }

    TransitiveReduction.INSTANCE.reduce(g)

    saveAsImage(g)
    exportGraphML(g)

    g
  }

  def create(css: Set[CharacteristicSet]) : DirectedAcyclicGraph[CharacteristicSet, DefaultEdge] = {
    val g = new DirectedAcyclicGraph[CharacteristicSet, DefaultEdge](classOf[DefaultEdge])

    for {cs1 <- css
         cs2 <- css
         if cs1 != cs2} {
      if (cs1.subsumes(cs2)) {
        g.addVertex(cs1)
        g.addVertex(cs2)
        g.addEdge(cs1, cs2)
      }
    }

    TransitiveReduction.INSTANCE.reduce(g)

    saveAsImage(g)
    exportGraphML(g)

    g
  }

  def saveAsImage[T <: CharacteristicSetBase](g: DirectedAcyclicGraph[T, DefaultEdge]): Unit = {
    import java.awt.Color

    import com.mxgraph.layout.mxFastOrganicLayout
    import javax.imageio.ImageIO
    val graphAdapter = new org.jgrapht.ext.JGraphXAdapter(g)
    val layout = new mxFastOrganicLayout(graphAdapter)
    layout.execute(graphAdapter.getDefaultParent)

    val image = mxCellRenderer.createBufferedImage(graphAdapter, null, 2, Color.WHITE, true, null)
    val imgFile = new File("/tmp/graph.png")
    ImageIO.write(image, "PNG", imgFile)
  }

  def exportGraphML[T <: CharacteristicSetBase](g: DirectedAcyclicGraph[T, DefaultEdge]): Unit = {
    val exporter = new GraphMLExporter[T, DefaultEdge]()

    exporter.setVertexLabelProvider(v => v.properties.map(n => SplitIRI.localname(n.getURI)).mkString(","))

    exporter.registerAttribute("size", AttributeCategory.NODE, AttributeType.LONG)
    exporter.registerAttribute("name", AttributeCategory.ALL, AttributeType.STRING)

    exporter.setVertexAttributeProvider(v => {
      val m = new util.HashMap[String, Attribute]()
      if (v.isInstanceOf[ExtendedCharacteristicSet]) {
        m.put("size", new DefaultAttribute(v.asInstanceOf[ExtendedCharacteristicSet].size, AttributeType.LONG))
      }
      m.put("name", DefaultAttribute.createAttribute(v.properties.map(n => SplitIRI.localname(n.getURI)).mkString(",")))
      m
    })

    exporter.exportGraph(g, new File("/tmp/graph.graphml"))
  }
}

/**
 * @author Lorenz Buehmann
 */
object CharacteristicSets {

  def computeCharacteristicSets(triples: RDD[Triple]): RDD[CharacteristicSet] = {
    triples
      //      .map(t => (t.getSubject, t.getPredicate))
      //      .groupByKey()
      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
      .map(_._2)
      .distinct()
  }

  def computeCharacteristicSetsWithEntities(triples: RDD[Triple]): RDD[(CharacteristicSet, Set[Node])] = {
    triples
      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
      .map(e => (e._2, Set(e._1)))
      .reduceByKey((nodes1, nodes2) => nodes1 ++ nodes2)
  }

  def computeCharacteristicSetsWithEntitiesAndSize(triples: RDD[Triple]): RDD[(CharacteristicSet, Set[Node], Int)] = {
    triples
      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
      .map(e => (e._2, Set(e._1)))
      .reduceByKey((nodes1, nodes2) => nodes1 ++ nodes2)
      .map{ case (cs, entities) => (cs, entities, entities.size)}
  }

  import scala.reflect.runtime.{universe => ru}

  private def getType[T](clazz: Class[T])(implicit runtimeMirror: ru.Mirror) = runtimeMirror.classSymbol(clazz).toType

  implicit val mirror = ru.runtimeMirror(getClass.getClassLoader)

  private def asRow(subj: Node, predObjPairs: Iterable[(Node, Node)]): (Row, StructType) = {
    // the subject first
    var values: IndexedSeq[AnyRef] = IndexedSeq[AnyRef](getObject(subj))

    var schema = new StructType()
    schema = schema.add("s", ScalaReflection.schemaFor(getType(getObject(subj).getClass)).dataType, false)

    // predicate-object pairs group by the predicate to generate a single column only
    // in case of multiple values
    predObjPairs.groupBy(_._1).mapValues(_.map(_._2)).foreach {
      case (prop, objects) =>
        if (objects.size == 1) {
          values = values :+ getObject(objects.head)
        } else {
          values = values :+ objects.map(getObject).toArray
        }
        schema = schema.add("P" + FmtUtils.stringForNode(prop), ScalaReflection.schemaFor(getType(getObject(objects.head).getClass)).dataType, false)
    }

    println(schema)

    Row.fromSeq(values) -> schema
  }

  private def getObject(node: Node): AnyRef = {
    if (node.isLiteral) {
      // literals with no datatype or custom datatype do not have a Java class mapping
      // in this case we use the string representation
      if (node.getLiteralDatatype.getJavaClass == null) {
        node.toString
      } else {
        node.getLiteralValue
      }
    } else {
      FmtUtils.stringForNode(node, null.asInstanceOf[SerializationContext])
    }
  }

  def computeCharacteristicSetsWithEntities(triples: DataFrame): DataFrame = {
    triples
      .select("s", "p")
      .groupBy("s").agg(collect_set("p") as "cs")
      .groupBy("cs").agg(collect_set("s") as "entities")
  }

  def computeCharacteristicSetsWithSize(triples: DataFrame): DataFrame = {
    triples
      .select("s", "p")
      .groupBy("s").agg(collect_set("p") as "cs")
      .groupBy("cs").agg(count("s") as "size")
  }

  def computeCharacteristicSetsWithSizeAndEntities(triples: DataFrame): DataFrame = {
    triples
      .select("s", "p")
      .groupBy("s").agg(collect_set("p") as "cs")
      .groupBy("cs").agg(count("s") as "size", collect_set("s") as "entities")
  }

  /**
   * d(ci) = |ri| >= m × |rmax|
   * with
   * m ∈ [0, 1] being the density factor
   * rmax being the cardinality of the largest CS in D
   *
   */
  def density(css: Seq[ExtendedCharacteristicSet], densityFactor: Double): Double = {
    val r_max = css.maxBy(_.size).size
    densityFactor * r_max
  }

  /**
   * d(ci) = |ri| >= m × |rmax|
   * with
   * m ∈ [0, 1] being the density factor
   * rmax being the cardinality of the largest CS in D
   *
   */
  def density(df: DataFrame, densityFactor: Double): Double = {
    val r_max = df.agg(max("size")).first().getAs[Long](0)
    densityFactor * r_max
  }


  /**
   * r_null represents the ratio of null values to the cardinality of the merged table
   *
   * r_null(ci,cj) = (|Pj \ Pi| x |ri|) / (|rj + |ri|)
   *
   * @param cs_i
   * @param cs_j
   * @return
   */
  def r_null(cs_i: ExtendedCharacteristicSet, cs_j: ExtendedCharacteristicSet): Double = {
    (cs_j.properties.diff(cs_i.properties).size * cs_i.size) / (cs_i.size + cs_j.size)
  }

  /**
   * r_null_g represents the ratio of null values to the cardinality of the merged table
   *
   * r_null_(g)|c_d = sum_i_|g| (|Pd \ Pi| x |ri|) / ( |rd| + sum_i_|g|(|ri|) )
   *
   * @param g graph
   * @param root the dense root of sub-graph g
   * @return
   */
  def r_null_g(g: ExtendedCharacteristicSetGraph, root: ExtendedCharacteristicSet): Double = {
    val nodes = g.vertexSet().asScala
    val nullValues = nodes.map(cs => root.properties.diff(cs.properties).size * cs.size).sum
    val cardinality = nodes.map(_.size).sum + root.size
    nullValues.doubleValue() / cardinality.doubleValue()
  }

  def cost(g: ExtendedCharacteristicSetGraph): Double = {
    g.denseNodes.map(v => r_null_g(g, v)).sum
  }

  private def isDense(density: Double) = udf((cnt: Long) => cnt > density)

  def compute(triples: DataFrame, densityFactor: Double): Unit = {
    // compute the CSs
    var df = computeCharacteristicSetsWithSizeAndEntities(triples)

    val densityVal = density(df, densityFactor)

    df = df.withColumn("dense", isDense(densityVal)(df("size")))

    val r: Regex = "[<>]".r
    val ecss = df.select("cs", "size", "dense").collect().map(row => {
      val cs = row.getAs[Seq[String]]("cs").map(p => NodeFactory.createURI(r.replaceAllIn(p, "")))
      val size = row.getAs[Long]("size")
      val isDense = row.getAs[Boolean]("dense")

      ExtendedCharacteristicSet(cs.toSet, size, Some(isDense))
    }).toSet

    // build the CS hierarchy graph
    val g = CharacteristicSetGraphBuilder.create(ecss)

    // remove descendant edges from the dense nodes
    g.cutDescendantsOfDenseNodes()

    // create the inferred graph
    g.computeClosure()

    // compute the connected components
    val ccInspector = new KosarajuStrongConnectivityInspector[ExtendedCharacteristicSet, DefaultEdge](g)
    val ccs = ccInspector.getStronglyConnectedComponents.asScala

    // for each CC we generate all possible subgraphs
    ccs.foreach(cc => {

    })


  }


  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      throw new RuntimeException("Missing file path as argument")
    }

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = args.lift(1).getOrElse("/tmp/spark-warehouse")

    val spark = SparkSession.builder
      .appName("Characteristic Sets computation")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("parquet.enable.dictionary", true)
      .enableHiveSupport()
      .getOrCreate()

    import net.sansa_stack.rdf.spark.io._
    val path = args(0)

    val databaseName = args.lift(2).getOrElse("sansa")

    spark.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")

    val tablesLoader = new TablesLoader(spark, database = databaseName)
    tablesLoader.loadTriplesTable(path)

    compute(spark.table("triples"), 0.5)


    spark.stop()
    System.exit(0)
    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val css = computeCharacteristicSets(triples).collect().toSet
    println(s"found ${css.size} CSs")
    css.foreach(println)

    CharacteristicSetGraphBuilder.create(css)


    // process each CS and get subjects having all properties in a CS, then get the p-o pairs
    css.foreach { cs =>

      val rowWithSchema = triples
        .map(t => (t.getSubject, (t.getPredicate, t.getObject))) // t => (s, (p,o))
        .groupByKey() // (s, [(p1,o1),...,(pn,on)])
        .filter(e => cs.properties subsetOf e._2.map(_._1).toSet) // [(p1,o1),...,(pn,on)] contains cs ?
        .map { case (s, po) => asRow(s, po) } // create rows
        .take(1) // trigger action

      val hf = Hashing.sha256().newHasher()
      val tableName = hf.putString(cs.properties.toSeq.sortBy(_.getURI).mkString(","), Charsets.UTF_8).hash().toString
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], rowWithSchema.head._2).createOrReplaceTempView(tableName)

      println(rowWithSchema.head._2.toDDL)

////      val subjToPredObjPairs = triples
////        .filter(t => cs.properties.contains(t.getPredicate))
////        .map(t => (t.getSubject, (t.getPredicate, t.getObject)))
////        .groupByKey()
////
////      val rows = subjToPredObjPairs.map { case (s, po) => asRow(s, po) }
////      rows.count()
//
//      val hf = Hashing.sha256().newHasher()
//      val tableName = hf.putString(cs.properties.toSeq.sortBy(_.getURI).mkString(","), Charsets.UTF_8).hash().toString

      //      val df = spark.createDataFrame(subjToPredObjPairs).createOrReplaceTempView(tableName)

    }

//    val db = spark.catalog.currentDatabase
//    val tables = spark.catalog.listTables()
//    tables.foreach {t =>
//      val ddl = spark.sql(s"SHOW CREATE TABLE ${t.name}")
//      println(ddl)
//    }


    spark.stop()
  }
}
