package net.sansa_stack.owl.spark.rdd

import com.typesafe.scalalogging.{Logger => ScalaLogger}
import net.sansa_stack.owl.common.parsing.OWLXMLSyntaxParsing
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model._

import scala.jdk.CollectionConverters._

/**
  * An implementation for building RDD[OWLAxioms] out from OWL/XML syntax format
  *
  * @author Heba Mohamed
  */

object OWLXMLSyntaxOWLAxiomsRDDBuilder extends Serializable {

  private val logger = ScalaLogger(this.getClass)
  val parallelism = 240

  /**
    * definition to build OWLAxioms out of OWL file
    * @param spark: spark session
    * @param filePath: absolute path of the OWL file
    * @return OwlAxioms: RDD[Set[OwlAxioms]s]
    * */

  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {

    build(spark, filePath, OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, filePath))
  }

  /**
    * definition to build OwlAxioms out of expressions
    * @param spark: spark session
    * @param owlRecordsRDD: a tuple consisting of RDD records for xmlVersion string, owlxml prefix string and owl expressions
    * */

  def build(spark: SparkSession,
            filePath: String,
            owlRecordsRDD: (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD)): OWLAxiomsRDD = {

    val sc = spark.sparkContext

    // get RDD consisting of xmlVersion
    val xmlVersionRDD = owlRecordsRDD._1.first()

    // get RDD consisting of owlXml prefix
    val owlPrefixRDD = owlRecordsRDD._2.first()

    // get RDD consisting of owl expressions in owlXml syntax
    val owlExpressionsRDD: OWLExpressionsRDD = owlRecordsRDD._3

    logger.info("Start schema records parsing ........ ")

    // for each owl expressions try to extract axioms in it,
    // if not print the corresponding expression for which axioms could not extracted using owl api

    var expr2Axiom = owlExpressionsRDD.map(expressionRDD => {
      try OWLXMLSyntaxParsing.makeAxiom(xmlVersionRDD, owlPrefixRDD, expressionRDD)
      catch {
        case exception: OWLParserException =>
          logger.warn("Parser error for line " + expressionRDD + ": " + exception.getMessage)
          null
      }
    }).filter(_ != null)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val schemaAxiomsRdd = expr2Axiom.flatMap(x => x.toList.asJavaCollection.asScala)

    val bc = broadcastProperties(spark.sparkContext, schemaAxiomsRdd)
    val dataBC = bc._1
    val objBC = bc._2
    val annBC = bc._3

    var owlAxiomsRDD = schemaAxiomsRdd

    owlAxiomsRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val instanceRecords: Map[String, Map[String, String]] = assertionSyntaxParsing(owlAxiomsRDD)

    var rdd = if (instanceRecords.isEmpty) {
                  val refinedRDD = owlAxiomsRDD.map(axiom => RefineOWLAxioms.refineOWLAxiom(axiom, dataBC, objBC, annBC))
                                               .filter(_ != null)

      refinedRDD

    } else {

      logger.info("Start instance records parsing ........ ")

      val expression = new OWLXMLSyntaxExpressionBuilder(spark, filePath)

      var i = 1
      while (i < instanceRecords.size) {

        // get pattern for begin and end tags for owl expressions (instances) to be specified for hadoop stream
        val owlRecordPatterns: OWLExpressionsRDD = expression.getRecord(instanceRecords("instancePattern" + i))

        val inst2Axiom = owlRecordPatterns.map(instance => {
          try OWLXMLSyntaxParsing.makeAxiom(xmlVersionRDD, owlPrefixRDD, instance)
          catch {
            case exception: OWLParserException =>
              logger.warn("Parser error for line " + instance + ": " + exception.getMessage)
              null
          }
        }).filter(_ != null)

        expr2Axiom = sc.union(inst2Axiom, expr2Axiom)

        i = i + 1
      }

      owlAxiomsRDD = expr2Axiom.flatMap(x => x.toList.asJavaCollection.asScala)

      expr2Axiom.unpersist(false)

      val refinedRDD = owlAxiomsRDD.map(axiom => RefineOWLAxioms.refineOWLAxiom(axiom, dataBC, objBC, annBC))
                                   .filter(_ != null)

      refinedRDD
    }

    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd = rdd.repartition(4*parallelism).distinct(4*parallelism)

    rdd.unpersist(false)

    rdd
 }

  def broadcastProperties (sc: SparkContext,
                           schemaAxioms: OWLAxiomsRDD):
                          (Broadcast[Array[IRI]], Broadcast[Array[IRI]], Broadcast[Array[IRI]]) = {

    val allDeclarations = schemaAxioms.filter(axiom => axiom.getAxiomType.equals(AxiomType.DECLARATION))
                                      .asInstanceOf[RDD[OWLDeclarationAxiom]]

    val dataProperties = allDeclarations.filter(a => a.getEntity.isOWLDataProperty)
                                        .map(a => a.getEntity.getIRI)
                                        .persist(StorageLevel.MEMORY_AND_DISK)

    val objectProperties = allDeclarations.filter(a => a.getEntity.isOWLObjectProperty)
                                          .map(a => a.getEntity.getIRI)
                                          .persist(StorageLevel.MEMORY_AND_DISK)

    val annProperties = allDeclarations.filter(a => a.getEntity.isOWLAnnotationProperty)
                                       .map(a => a.getEntity.getIRI)
                                      . persist(StorageLevel.MEMORY_AND_DISK)

    val data = dataProperties.repartition(2*parallelism).collect()
    val obj = objectProperties.repartition(2*parallelism).collect()
    val ann = annProperties.repartition(2*parallelism).collect()

    val dataPropertiesBC = sc.broadcast(data)
    val objPropertiesBC = sc.broadcast(obj)
    val annPropertiesBC = sc.broadcast(ann)

    dataProperties.unpersist(false)
    objectProperties.unpersist(false)

    (dataPropertiesBC, objPropertiesBC, annPropertiesBC)
  }


  def assertionSyntaxParsing(Axioms: OWLAxiomsRDD): Map[String, Map[String, String]] = {

    val classes = Axioms.flatMap {
                            case axiom: HasClassesInSignature => axiom.classesInSignature().iterator().asScala
                            case _ => null
                          }.filter(_ != null)

    val ns = Namespaces.LUBM._1 + ":"

    val tmp = classes.map(x => ("<" + ns + x.getIRI.getShortForm, "</" + ns + x.getIRI.getShortForm + ">"))

    val tmpMap = tmp.collect().toMap

    val it: Iterator[Map[String, String]] = tmpMap.map(x => Map("beginTag" -> x._1, "endTag" -> x._2))
                                                  .asJavaCollection.asScala.iterator
                                                  .filter(_ != null)

    var pattern: Map[String, Map[String, String]] = Map.empty
    var i = 1
    while (it.hasNext) {
      pattern = pattern. +("instancePattern" + i -> it.next())
      i = i + 1
    }

    pattern

  }
 }
