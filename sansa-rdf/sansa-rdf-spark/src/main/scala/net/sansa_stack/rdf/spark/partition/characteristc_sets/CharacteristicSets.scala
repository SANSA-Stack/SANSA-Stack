package net.sansa_stack.rdf.spark.partition.characteristc_sets

import java.io.File

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.mxgraph.util.mxCellRenderer
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.serializer.SerializationContext
import org.apache.jena.sparql.util.FmtUtils
import org.apache.jena.util.SplitIRI
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, collect_set, lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jgrapht.alg.TransitiveReduction
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}
import org.jgrapht.io.GraphMLExporter

case class CharacteristicSet(properties: Set[Node]) {
  def isSubsetOf(cs: CharacteristicSet): Boolean = properties.subsetOf(cs.properties)
  def subsumes(cs: CharacteristicSet): Boolean = cs.properties.subsetOf(properties)
}

object CharacteristicSetGraphBuilder {

  def create(css: Set[CharacteristicSet]) : DefaultDirectedGraph[CharacteristicSet, DefaultEdge] = {
    val g = new DefaultDirectedGraph[CharacteristicSet, DefaultEdge](classOf[DefaultEdge])

    for {cs1 <- css
         cs2 <- css
         if cs1 != cs2} {
      if (cs1.isSubsetOf(cs2)) {
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

  def saveAsImage(g: DefaultDirectedGraph[CharacteristicSet, DefaultEdge]): Unit = {
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

  def exportGraphML(g: DefaultDirectedGraph[CharacteristicSet, DefaultEdge]): Unit = {
   val exporter = new GraphMLExporter[CharacteristicSet, DefaultEdge]()

    exporter.setVertexLabelProvider(v => v.properties.map(n => SplitIRI.localname(n.getURI)).mkString(","))

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
    //    triples
    //      .select("s")
    //      .map(t => (t.getSubject, CharacteristicSet(Set(t.getPredicate))))
    //      .reduceByKey((c1, c2) => CharacteristicSet(c1.properties ++ c2.properties))
    //      .map(e => (e._2, Set(e._1)))
    //      .reduceByKey((nodes1, nodes2) => nodes1 ++ nodes2)
    triples
      .select("s", "p")
      .groupBy("s").agg(collect_set("p") as "cs")
      .groupBy("cs").agg(collect_set("s") as "entities")
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
    val tl = new TablesLoader(spark, databaseName)
    tl.loadTriplesTable(path)
    tl.loadVPTables()
    tl.loadWPTable()
    tl.loadIWPTable()

    spark.table("wpt").show(50)
    spark.table("iwpt").show(50)

    val nonNullColsExpr = spark.table("wpt").columns.map(c => when(col(c).isNotNull, lit(c)))
    val df = spark.sql("select * from wpt where s = '<http://dbpedia.org/resource/Alan_Shearer>'").cache()
    df.show()

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
