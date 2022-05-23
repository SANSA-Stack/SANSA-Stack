package net.sansa_stack.ml.spark.explainableanomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import net.sansa_stack.ml.spark.anomalydetection.DistADUtil
import net.sansa_stack.ml.spark.featureExtraction.FeatureExtractingSparqlGenerator.createSparql
import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import org.apache.jena.graph
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders}
import org.apache.spark.sql.functions.{col, first, udf}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType}

object ExSmartDataFrame {


  /**
   * Transform given RDD[Triple] to casted type, pivoted, renamed, dataframe
   * @param rdd
   * @return
   */
  def transform(rdd: RDD[Triple],config: ExDistADConfig): DataFrame = {
    val rddWithDataType: RDD[Triple] = addDataTypeToPredicates(rdd)
    val df: DataFrame = createDataFrame(rddWithDataType)
    val featuresDF: DataFrame = extractFeatures(df,rddWithDataType,config)
    var renamedColPivotedDF: DataFrame = renameCols(featuresDF)
    val castedPivotedDF: DataFrame = castTypes(renamedColPivotedDF)
    castedPivotedDF
  }

  def extractFeatures(df:DataFrame,rddWithDataType: RDD[Triple],config: ExDistADConfig) = {
    config.featureExtractor match {
      case config.PIVOT =>
        df.groupBy("s")
          .pivot("p")
          .agg(first("o"))
      case config.LITERAL2FEATURE =>
        implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])
        import net.sansa_stack.rdf.spark.model.TripleOperations
        var onlyLiteralDataDataSet: Dataset[graph.Triple]= rddWithDataType.toDS()
        LOG.info("Starting Literal2Feature. May take time....")
        val seedVarName = "?s"
        val whereClauseForSeed = "?s ?p ?o"
        val maxUp: Int = 0
        val maxDown: Int = config.l2fDepth
        val seedNumber: Int = config.l2fSeedNumber
        val seedNumberAsRatio: Double = 1.0

        val a = createSparql(
          ds = onlyLiteralDataDataSet,
          seedVarName = seedVarName,
          seedWhereClause = whereClauseForSeed,
          maxUp = maxUp,
          maxDown = maxDown,
          numberSeeds = seedNumber,
          ratioNumberSeeds = seedNumberAsRatio
        )
        val sparqlFrame = new SparqlFrame()
          .setSparqlQuery(a._1)
          .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
          .setCollapsByKey(false)
        onlyLiteralDataDataSet.take(10) foreach println
        var b: DataFrame = sparqlFrame.transform(onlyLiteralDataDataSet)
        if(config.verbose){
          LOG.info("Result of Literal2Feature")
          b.show(false)
        }
        b
    }
  }
  /**
   * Gets an RDD and append datatype to the predicate with __
   * @param rdd
   * @return rdd with datatype appended to the predicates
   */
  def addDataTypeToPredicates(
                               rdd: RDD[Triple]
                             ): RDD[Triple] = {
    rdd
      .filter(
        p =>
          !(p.getPredicate.toString.contains("*") ||
            p.getPredicate.toString.contains("'") ||
            p.getPredicate.toString.contains("%") ||
            p.getPredicate.toString.contains("^") ||
            p.getPredicate.toString.contains("$") ||
            p.getPredicate.toString.contains("#") ||
            p.getPredicate.toString.contains("(") ||
            p.getPredicate.toString.contains(")") ||
            p.getPredicate.toString.contains("%") ||
            p.getPredicate.toString.size<=3 ||
            p.getPredicate.toString.contains("1") ||
            p.getPredicate.toString.contains("2") ||
            p.getPredicate.toString.contains("3") ||
            p.getPredicate.toString.contains("4") ||
            p.getPredicate.toString.contains("5") ||
            p.getPredicate.toString.contains("6") ||
            p.getPredicate.toString.contains("7") ||
            p.getPredicate.toString.contains("8") ||
            p.getPredicate.toString.contains("9") ||
            p.getPredicate.toString.contains("0") ||
            p.getPredicate.toString.contains("_") ||
            p.getPredicate.toString.contains("-") ||
            p.getPredicate.toString.contains("&") )
      )
      .map(p => {
        val obj = p.getObject

        if (obj.isLiteral) {
          if (obj.toString.contains("^^")) {

            //TODO: it seems there are many data types in the original dbpedia
            var value = obj.toString.split("\\^\\^")(0)
            var dataType = ""
            if (obj.toString.split("\\^\\^")(1).contains("#")) {
              dataType = obj.toString.split("\\^\\^")(1).split("#").last
            } else if (obj.toString
              .split("\\^\\^")(1)
              .contains("dbpedia.org/datatype/")) {
              dataType = obj.toString
                .split("\\^\\^")(1)
                .split("dbpedia.org/datatype/")
                .last
            } else {
              println("WE SHOULD NEVER BE HERE \t" + obj.toString())
              //TODO: what should we do here. Dbpedia is so dirty
              dataType = ""
              value = "\"ERROR\""
            }
            val predicate = p.getPredicate.toString + "__" + dataType

            value = value.substring(1, value.length - 1)
            if(value.contains("@")){
              value =value.split("@")(0).replace("\"","")
            }

            new graph.Triple(
              p.getSubject,
              NodeFactory.createURI(predicate),
              NodeFactory.createLiteral(value)
            )
          } else {
            var value = obj.toString
            if(value.contains("@")){
              value = value.split("@")(0).replace("\"","")
            }

            new graph.Triple(
              p.getSubject,
              NodeFactory.createURI(p.getPredicate.toString + "__String"),
              NodeFactory.createLiteral(value)
            )
          }
        } else if (obj.isURI) {
          new graph.Triple(
            p.getSubject,
            NodeFactory.createURI(p.getPredicate.toString + "__url"),
            p.getObject
          )
        } else {
          new graph.Triple(
            p.getSubject,
            NodeFactory.createURI(p.getPredicate.toString + "__unknown"),
            p.getObject
          )
        }
      })
  }

  def createDataFrame(rddWithDataType: RDD[Triple]): DataFrame = {
    DistADUtil.createDF(rddWithDataType)
  }

  def pivot(df: DataFrame): DataFrame = {
    df.groupBy("s")
      .pivot("p")
      .agg(first("o"))
  }

  def renameCols(pivotedDF: DataFrame): DataFrame = {
    pivotedDF
      .toDF(
        pivotedDF.columns.map(
          _.split("/").last
        ): _*
      )
  }

  def castTypes(renamedColPivotedDF: DataFrame): DataFrame = {
    var data = renamedColPivotedDF
    data.columns.foreach(c => {
      if (!c.equals("s")) {
        val colRealName = c
        val dataType = c.split("_(?!__)").last.toLowerCase
        dataType match {
          case "integer" | "int" =>
            data = data.withColumn(
              c,
              col(colRealName).cast(IntegerType)
            )
          case "decimal" =>
            data = data.withColumn(
              c,
              col(colRealName).cast(DoubleType)
            )
          case "double" | "centimetre" =>
            data = data.withColumn(
              c,
              col(colRealName).cast(DoubleType)
            )
          case "boolean" =>
            data = data.withColumn(
              c,
              bool2int_udf(col(colRealName).cast(BooleanType))
            )
          case "String" =>
            val indexer = new StringIndexer()
              .setInputCol(c)
              .setOutputCol(colRealName + "_index")
              .setHandleInvalid("keep")
            data = indexer.fit(data).transform(data)

          case "url" =>
            val indexer = new StringIndexer()
              .setInputCol(c)
              .setOutputCol(colRealName + "_index")
              .setHandleInvalid("keep")
            data = indexer.fit(data).transform(data)
          case "unknown" =>
            val indexer = new StringIndexer()
              .setInputCol(c)
              .setOutputCol(colRealName + "_index")
              .setHandleInvalid("keep")
            data = indexer.fit(data).transform(data)
          case "date" =>
            val indexer = new StringIndexer()
              .setInputCol(c)
              .setOutputCol(colRealName + "_index")
              .setHandleInvalid("keep")
            data = indexer.fit(data).transform(data)
          case "gMonthDay" =>
            data = data.withColumn(
              c,
              col(colRealName).cast(IntegerType)
            )
          case "second" =>
            data = data.withColumn(
              c,
              col(colRealName).cast(IntegerType)
            )
          case "perCent" =>
            data = data.withColumn(
              c,
              col(colRealName).cast(DoubleType)
            )
          case _ =>
            val indexer = new StringIndexer()
              .setInputCol(c)
              .setOutputCol(colRealName + "_index")
              .setHandleInvalid("keep")
            data = indexer.fit(data).transform(data)
          //ToDo add other datatypes as well
        }
      }
    })
    data
  }

  def bool2int(b:Boolean) = if (b) 1 else 0
  val bool2int_udf = udf(bool2int _)

}
