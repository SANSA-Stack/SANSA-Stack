package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.util.Scanner

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{ RangePartitioner, SparkContext }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ VectorAssembler, Word2Vec, Word2VecModel }
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ concat, lit }
import org.json.JSONObject

class VandalismDetection extends Serializable {

  // Function 1 : Distributed RDF Parser Approach
  def Start_RDF_Parser_Appraoch(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._ // for UDF
    import org.apache.spark.sql.types._

    println("*********************************************************************")
    println("Distributed RDF Parser Model")
    println("Please Enter 1 for JTriple and  2 for TRIX  process and 3 for RDFXML:")
    println("*********************************************************************")

    val num = scala.io.StdIn.readLine()
    if (num == "1") {
      println("JTriple.........!!!!!!")
      // Streaming records:RDFJtriple file :
      val jobConf = new JobConf()

      val JTriple_Parser_OBJ = new ParseJTriple()
      val DRF_Builder_JTripleOBJ = new FacilitiesClass()
      val RDD_JTriple = JTriple_Parser_OBJ.Start_JTriple_Parser(jobConf, sc)
      RDD_JTriple.foreach(println)
      // ----------------------------DF for RDF TRIX ------------------------------------------
      //  Create SQLContext Object:
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val DFR_JTriple = DRF_Builder_JTripleOBJ.RDD_TO_DFR_JTriple(RDD_JTriple, sqlContext)
      DFR_JTriple.show()

    } else if (num == "2") {

      println("TRIX.........!!!!!!")
      // Streaming records:RDFTRIX file :
      val jobConf = new JobConf()
      val TRIX_Parser_OBJ = new ParseTRIX()
      val DRF_Builder_RDFTRIX_OBJ = new FacilitiesClass()
      val RDD_TRIX = TRIX_Parser_OBJ.Start_TriX_Parser(jobConf, sc)
      RDD_TRIX.foreach(println)
      // ----------------------------DF for RDF TRIX ------------------------------------------
      //  Create SQLContext Object:
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val DFR_TRIX = DRF_Builder_RDFTRIX_OBJ.RDD_TO_DFR_TRIX(RDD_TRIX, sqlContext)
      DFR_TRIX.show()

    } else if (num == "3") {
      println("RDF XML .........!!!!!!")
      // Streaming records:RDFXML file :
      val jobConf_Record = new JobConf()
      val jobConf_Prefixes = new JobConf()

      val RDFXML_Parser_OBJ = new ParseRDFXML()
      val DRF_Builder_RDFXML_OBJ = new FacilitiesClass()

      val RDD_RDFXML = RDFXML_Parser_OBJ.start_RDFXML_Parser(jobConf_Record, jobConf_Prefixes, sc)
      RDD_RDFXML.foreach(println)

      // ----------------------------DF for RDF XML ------------------------------------------
      //  Create SQLContext Object:
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val DFR_RDF_XML = DRF_Builder_RDFXML_OBJ.RDD_TO_DFR_RDFXML(RDD_RDFXML, sqlContext)
      DFR_RDF_XML.show()

    }

    sc.stop()
  }

  // *********************************************************************************
  // Function 2:Training XML and Vandalism Detection
  def Training_Start_StandardXMLParser_VD(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._ // for UDF
    import org.apache.spark.sql.types._

    // Streaming records:
    val jobConf = new JobConf()
    val NormalXML_Parser_OBJ = new ParseNormalXML()
    val RDD_OBJ = new ParseNormalXML()

    val Training_RDD_All_Record1 = RDD_OBJ.Training_DB_NormalXML_Parser_Input1(sc)
    val Training_RDD_All_Record2 = RDD_OBJ.Training_DB_NormalXML_Parser_Input2(sc)
    val Training_RDD_All_Record3 = RDD_OBJ.Training_DB_NormalXML_Parser_Input3(sc)
    // RDD_All_Record1.foreach(println)
    // RDD_All_Record2.foreach(println)
    // RDD_All_Record3.foreach(println)

    val Training_RDD_All_Record = Training_RDD_All_Record1.union(Training_RDD_All_Record2).union(Training_RDD_All_Record3).distinct().cache()

    // println(RDD_All_Record.count())
    println(Training_RDD_All_Record.count())

    // ======= Json part :
    // Json RDD : Each record has its Revision iD:
    val JsonRDD = Training_RDD_All_Record.map(_.split("NNLL")).map(v => replacing_with_Quoto(v(0), v(8))).cache()
    // JsonRDD.foreach(println)
    // println(JsonRDD.count())

    // Data set
    val Ds_Json = sqlContext.jsonRDD(JsonRDD).select("key", "id", "labels", "descriptions", "aliases", "claims", "sitelinks").cache()
    // Ds_Json.show()
    // println(Ds_Json.count())

    // ======= Tags part : // Contributor IP here is in Decimal format not IP format and It is converted in ParseNormalXml stage
    val TagsRDD = Training_RDD_All_Record.map(_.split("NNLL")).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))).cache()
    val DF_Tags = TagsRDD.toDF("Rid", "Itemid", "comment", "pid", "time", "contributorIP",
      "contributorID", "contributorName", "JsonText", "model", "format", "sha").cache()
    //    DF_Tags.show()
    //    println(DF_Tags.count())

    // ======== Join Json part with Tag Part:============================
    // Joining to have full data
    val DF_First_DF_Result_Join_Tags_and_Json = DF_Tags.as("T1").join(Ds_Json.as("T2"), $"T1.Rid" === $"T2.key", "leftouter")
      .select("Rid", "itemid", "comment", "pid", "time", "contributorIP",
        "contributorID", "contributorName", "JsonText", "labels", "descriptions",
        "aliases", "claims", "sitelinks", "model", "format", "sha") // .orderBy("Rid", "Itemid")
    DF_First_DF_Result_Join_Tags_and_Json.registerTempTable("Data1")
    val dfr_DATA_JsonTages1 = sqlContext.sql("select * from Data1 order by itemid ,Rid ").cache()

    val colNames = Seq("Rid2", "itemid2", "comment2", "pid2", "time2", "contributorIP2",
      "contributorID2", "contributorName2", "JsonText2", "labels2", "descriptions2",
      "aliases2", "claims2", "sitelinks2", "model2", "format2", "sha2")
    val DF_Second = DF_First_DF_Result_Join_Tags_and_Json.toDF(colNames: _*) // .distinct()
    DF_Second.registerTempTable("Data2")

    // ===================================================================Parent // Previous Revision==============================================================================================================

    // Joining based on Parent Id to get the previous cases: ParentID
    val DF_Joined = DF_First_DF_Result_Join_Tags_and_Json.as("df1").join(DF_Second.as("df2"), $"df1.pid" === $"df2.Rid2", "leftouter").distinct()

    val RDD_After_JoinDF = DF_Joined.rdd.distinct()
    val x = RDD_After_JoinDF.map(row => (row(0).toString().toInt, row)).cache()
    val part = new RangePartitioner(4, x)
    val partitioned = x.partitionBy(part).persist() // persist is important for this case and obligatory.
    // partitioned.foreach(println)
    //
    //      //=====================================================All Features Based on Categories of Features Data Type :==================================================================================
    //
    val Result_all_Features = partitioned.map { case (x, y) => (x.toString() + "," + All_Features(y).toString()) } // we convert the Pair RDD to String one LineRDD to be able to make DF based on ","
    // Result_all_Features.foreach(println)
    // println("nayef" + Result_all_Features.count())

    // Conver the RDD of All Features to  DataFrame:

    val schema = StructType(

      // 0
      StructField("Rid", IntegerType, false) ::

        // Character Features :
        /* 1 */ StructField("C1uppercaseratio", DoubleType, false) :: /* 2 */ StructField("C2lowercaseratio", DoubleType, false) :: /* 3 */ StructField("C3alphanumericratio", DoubleType, false) ::
        /* 4 */ StructField("C4asciiratio", DoubleType, false) :: /* 5 */ StructField("C5bracketratio", DoubleType, false) :: /* 6 */ StructField("C6digitalratio", DoubleType, false) ::
        /* 7 */ StructField("C7latinratio", DoubleType, false) :: /* 8 */ StructField("C8whitespaceratio", DoubleType, false) :: /* 9 */ StructField("C9puncratio", DoubleType, false) ::
        /* 10 */ StructField("C10longcharacterseq", DoubleType, false) :: /* 11 */ StructField("C11arabicratio", DoubleType, false) :: /* 12 */ StructField("C12bengaliratio", DoubleType, false) ::
        /* 13 */ StructField("C13brahmiratio", DoubleType, false) :: /* 14 */ StructField("C14cyrilinratio", DoubleType, false) :: /* 15 */ StructField("C15hanratio", DoubleType, false) ::
        /* 16 */ StructField("c16malysiaratio", DoubleType, false) :: /* 17 */ StructField("C17tamiratio", DoubleType, false) :: /* 18 */ StructField("C18telugratio", DoubleType, false) ::
        /* 19 */ StructField("C19symbolratio", DoubleType, false) :: /* 20 */ StructField("C20alpharatio", DoubleType, false) :: /* 21 */ StructField("C21visibleratio", DoubleType, false) ::
        /* 22 */ StructField("C22printableratio", DoubleType, false) :: /* 23 */ StructField("C23blankratio", DoubleType, false) :: /* 24 */ StructField("C24controlratio", DoubleType, false) ::
        /* 25 */ StructField("C25hexaratio", DoubleType, false) ::

        // word Features:
        /* 26 */ StructField("W1languagewordratio", DoubleType, false) :: /* 27 Boolean */ StructField("W2Iscontainlanguageword", DoubleType, false) :: /* 28 */ StructField("W3lowercaseratio", DoubleType, false) ::
        /* 29 Integer */ StructField("W4longestword", IntegerType, false) :: /* 30 Boolean */ StructField("W5IscontainURL", DoubleType, false) :: /* 31 */ StructField("W6badwordratio", DoubleType, false) ::
        /* 32 */ StructField("W7uppercaseratio", DoubleType, false) :: /* 33 */ StructField("W8banwordratio", DoubleType, false) :: /* 34 Boolean */ StructField("W9FemalFirstName", DoubleType, false) ::
        /* 35 Boolean */ StructField("W10MaleFirstName", DoubleType, false) :: /* 36 Boolean */ StructField("W11IscontainBadword", DoubleType, false) ::
        /* 37 Boolean */ StructField("W12IsContainBanword", DoubleType, false) ::
        /* 38 integer */ StructField("W13NumberSharewords", DoubleType, false) :: /* 39 Integer */ StructField("W14NumberSharewordswithoutStopwords", DoubleType, false) ::
        /* 40 */ StructField("W15PortionQid", DoubleType, false) :: /* 41 */ StructField("W16PortionLnags", DoubleType, false) :: /* 42 */ StructField("W17PortionLinks", DoubleType, false) ::

        //
        //          // Sentences Features:
        /* 43 */ StructField("S1CommentTailLength", DoubleType, false) :: /* 44 */ StructField("S2SimikaritySitelinkandLabel", DoubleType, false) ::
        /* 45 */ StructField("S3SimilarityLabelandSitelink", DoubleType, false) :: /* 46 */ StructField("S4SimilarityCommentComment", DoubleType, false) ::
        //
        //          // Statements Features :
        /* 47 */ StructField("SS1Property", StringType, false) :: /* 48 */ StructField("SS2DataValue", StringType, false) :: /* 49 */ StructField("SS3ItemValue", StringType, false) ::
        //
        //
        //        // User Features :
        /* 50 Boolean */ StructField("U1IsPrivileged", DoubleType, false) :: /* 51 Boolean */ StructField("U2IsBotUser", DoubleType, false) :: /* 52 Boolean */ StructField("U3IsBotuserWithFlaguser", DoubleType, false) ::
        /* 53 Boolean */ StructField("U4IsProperty", DoubleType, false) :: /* 54 Boolean */ StructField("U5IsTranslator", DoubleType, false) :: /* 55 Boolean */ StructField("U6IsRegister", DoubleType, false) ::
        /* 56 */ StructField("U7IPValue", DoubleType, false) :: /* 57 */ StructField("U8UserID", IntegerType, false) :: /* 58 */ StructField("U9HasBirthDate", DoubleType, false) ::
        /* 59 */ StructField("U10HasDeathDate", DoubleType, false) ::

        // Items Features :

        /* 60 */ StructField("I1NumberLabels", DoubleType, false) :: /* 61 */ StructField("I2NumberDescription", DoubleType, false) :: /* 62 */ StructField("I3NumberAliases", DoubleType, false) ::
        /* 63 */ StructField("I4NumberClaims", DoubleType, false) ::
        /* 64 */ StructField("I5NumberSitelinks", DoubleType, false) :: /* 65 */ StructField("I6NumberStatement", DoubleType, false) :: /* 66 */ StructField("I7NumberReferences", DoubleType, false) ::
        /* 67 */ StructField("I8NumberQualifier", DoubleType, false) ::
        /* 68 */ StructField("I9NumberQualifierOrder", DoubleType, false) :: /* 69 */ StructField("I10NumberBadges", DoubleType, false) :: /* 70 */ StructField("I11ItemTitle", StringType, false) ::

        // Revision Features:
        /* 71 */ StructField("R1languageRevision", StringType, false) :: /* 72 */ StructField("R2RevisionLanguageLocal", StringType, false) :: /* 73 */ StructField("R3IslatainLanguage", DoubleType, false) ::
        /* 74 */ StructField("R4JsonLength", DoubleType, false) :: /* 75 */ StructField("R5RevisionAction", StringType, false) :: /* 76 */ StructField("R6PrevReviAction", StringType, false) ::
        /* 77 */ StructField("R7RevisionAccountChange", DoubleType, false) :: /* 78 */ StructField("R8ParRevision", StringType, false) :: /* 79 */ StructField("R9RevisionTime", StringType, false) ::
        /* 80 */ StructField("R10RevisionSize", DoubleType, false) :: /* 81 */ StructField("R11ContentType", StringType, false) :: /* 82 */ StructField("R12BytesIncrease", DoubleType, false) ::
        /* 83 */ StructField("R13TimeSinceLastRevi", DoubleType, false) :: /* 84 */ StructField("R14CommentLength", DoubleType, false) :: /* 85 */ StructField("R15RevisionSubaction", StringType, false) ::
        /* 86 */ StructField("R16PrevReviSubaction", StringType, false) ::

        Nil)

    val rowRDD = Result_all_Features.map(line => line.split(",")).map(e ⇒ Row(e(0).toInt // character feature column
    , e(1).toDouble, e(2).toDouble, e(3).toDouble, e(4).toDouble, e(5).toDouble, e(6).toDouble, e(7).toDouble, e(8).toDouble, e(9).toDouble, RoundDouble(e(10).toDouble), e(11).toDouble, e(12).toDouble //
    , e(13).toDouble, e(14).toDouble, e(15).toDouble, e(16).toDouble, e(17).toDouble, e(18).toDouble, e(19).toDouble, e(20).toDouble, e(21).toDouble, e(22).toDouble //
    , e(23).toDouble, e(24).toDouble, e(25).toDouble // Word Feature column
    , e(26).toDouble, e(27).toDouble, e(28).toDouble, e(29).toDouble.toInt, e(30).toDouble, e(31).toDouble, e(32).toDouble, e(33).toDouble, e(34).toDouble, e(35).toDouble, e(36).toDouble, e(37).toDouble //
    , RoundDouble(e(38).toDouble), RoundDouble(e(39).toDouble), e(40).toDouble, e(41).toDouble, e(42).toDouble // Sentences Features column:
    , RoundDouble(e(43).toDouble), e(44).toDouble, e(45).toDouble, e(46).toDouble // Statement Features Column:
    , e(47), e(48), e(49) // User Features Column:
    , e(50).toDouble, e(51).toDouble, e(52).toDouble, e(53).toDouble, e(54).toDouble, e(55).toDouble, e(56).toDouble, e(57).toDouble.toInt, e(58).toDouble, e(59).toDouble // Item Features column:
    , e(60).toDouble, e(61).toDouble, e(62).toDouble, e(63).toDouble, e(64).toDouble, e(65).toDouble, e(66).toDouble, e(67).toDouble, e(68).toDouble //
    , e(69).toDouble, "Q" + e(70).toDouble.toInt.toString() // Revision Features Column:
    , e(71), e(72), e(73).toDouble, e(74).toDouble, e(75), e(76), e(77).toDouble, e(78), e(79), e(80).toDouble, e(81), e(82).toDouble, e(83).toDouble, e(84).toDouble, e(85), e(86)))

    // a.User Frequency:
    // number of revisions a user has contributed
    // val resu= DF_Tags.groupBy("contributorID").agg(count("Rid"))
    DF_Tags.registerTempTable("TagesTable")
    val ContributorFreq_for_Each_Revision_DF = sqlContext
      .sql("select contributorID as CIDUSER1, count(Rid) as NumberofRevisionsUserContributed from TagesTable where contributorID !='0' group by contributorID ") // .drop("CIDUSER1")
    // ContributorFreq_for_Each_Revision_DF.show()

    // b.Cumulated : Number of a unique Item a user has contributed.
    val CumulatedNumberof_uniqueItemsForUser_DF = sqlContext
      .sql("select contributorID as CIDUSER2,  COUNT(DISTINCT itemid) as NumberofUniqueItemsUseredit from TagesTable where contributorID !='0' group by contributorID") // .drop("CIDUSER2")
    // CumulatedNumberof_uniqueItemsForUser_DF.show()

    // 1.Item Frequency:
    // number of revisions an Item has
    val ItemFrequ_DF = sqlContext.sql("select itemid, count(Rid) as NumberRevisionItemHas from TagesTable  group by itemid")
    // ItemFrequ_DF.show()

    // 2. Cumulate number of unique users have edited the Item : Did not consider the users IP. Contributor is an IP or Name. we consider name
    val CumulatedNumberof_UniqueUserForItem_DF = sqlContext.sql("select itemid,  COUNT(DISTINCT contributorID) as NumberUniqUserEditItem from TagesTable where contributorID !='0' group by itemid")
    // CumulatedNumberof_UniqueUserForItem_DF.show()

    // 3. freq each Item :
    val Fre_Item_DF = sqlContext.sql("select itemid,  COUNT(itemid) as FreqItem from TagesTable  group by itemid")
    // Fre_Item_DF.show()

    // *****************************************************************************************************************************************
    // This is Main DataFrame:
    val BeforeJoin_All_Features = sqlContext.createDataFrame(rowRDD, schema)
    // BeforeJoin_All_Features.show()

    // ********************************** User feature Join

    // Join1 for add The first User Feature : number of revisions a user has contributed
    val AfterJoinUser1_All_Features = BeforeJoin_All_Features.as("T1").join(ContributorFreq_for_Each_Revision_DF.as("T2"), $"T1.U8UserID" === $"T2.CIDUSER1", "leftouter").drop("CIDUSER1")
    // AfterJoinUser1_All_Features.show()

    // Join2 for add The second  User Feature
    val AfterJoinUser2_All_Features = AfterJoinUser1_All_Features.as("T1").join(CumulatedNumberof_uniqueItemsForUser_DF.as("T2"), $"T1.U8UserID" === $"T2.CIDUSER2", "leftouter").drop("CIDUSER2")
    // AfterJoinUser2_All_Features.show()

    // ********************************** Item Feature Join
    // Join3 for add The First  Item Feature :number of revisions an Item has
    val AfterJoinItem3_All_Features = AfterJoinUser2_All_Features.as("T1").join(ItemFrequ_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")
    // AfterJoinItem3_All_Features.show()

    // Join4 for add The Second  Item Feature
    val AfterJoinItem4_All_Features = AfterJoinItem3_All_Features.as("T1").join(CumulatedNumberof_UniqueUserForItem_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")
    // AfterJoinItem4_All_Features.show()

    // Join5 for add The Third  Item Feature
    val AfterJoinItem5_All_Features = AfterJoinItem4_All_Features.as("T1").join(Fre_Item_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")
    // 2 AfterJoinItem5_All_Features.show()

    // ********************************

    // *Geografical information Feature from Meta File
    // REVISION_ID|REVISION_SESSION_ID|USER_COUNTRY_CODE|USER_CONTINENT_CODE|USER_TIME_ZONE|USER_REGION_CODE|USER_CITY_NAME|USER_COUNTY_NAME|REVISION_TAGS
    val df_GeoInf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("hdfs://localhost:9000/mydata/Meta.csv").select("REVISION_ID", "REVISION_SESSION_ID", "USER_COUNTRY_CODE", "USER_CONTINENT_CODE", "USER_TIME_ZONE",
        "USER_REGION_CODE", "USER_CITY_NAME", "USER_COUNTY_NAME", "REVISION_TAGS")
    // df_GeoInf.show()

    val df_Truth = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("hdfs://localhost:9000/mydata/truth.csv").select("REVISION_ID", "ROLLBACK_REVERTED", "UNDO_RESTORE_REVERTED")
    // df_GeoInf.show()

    val AfterJoinGeoInfo_All_Features = AfterJoinItem5_All_Features.as("T1").join(df_GeoInf.as("T2"), $"T1.Rid" === $"T2.REVISION_ID", "leftouter").drop("REVISION_ID").cache()
    // AfterJoinGeoInfo_All_Features.show()

    val Final_All_Features = AfterJoinGeoInfo_All_Features.as("T1").join(df_Truth.as("T2"), $"T1.Rid" === $"T2.REVISION_ID", "leftouter").drop("REVISION_ID").cache()
    // Final_All_Features.show()

    // Pre- process Data ============================================================================================================================================================

    // For String Column, We fill the Null values by "NA":

    var Fill_Missing_Final_All_Features = Final_All_Features.na.fill("NA", Seq("USER_COUNTRY_CODE", "USER_CONTINENT_CODE", "USER_TIME_ZONE",
      "USER_REGION_CODE", "USER_CITY_NAME", "USER_COUNTY_NAME", "REVISION_TAGS")).cache()

    // For Integer Frequency  Column, We fill the Null values by 0:
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.na.fill(0, Seq("FreqItem", "NumberUniqUserEditItem", "NumberRevisionItemHas", "NumberofUniqueItemsUseredit",
      "NumberofRevisionsUserContributed", "REVISION_SESSION_ID")).cache()
    // Fill_Missing_Final_All_Features.show()

    val BoolToDoubleUDF = udf { (BoolAsString: String) => if (BoolAsString == "T") 1.0 else 0.0 }
    val IntegerToDouble = udf { (IntegerRevisionSessionID: Integer) => IntegerRevisionSessionID.toDouble }
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalROLLBACK_REVERTED", BoolToDoubleUDF(col("ROLLBACK_REVERTED")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUNDO_RESTORE_REVERTED", BoolToDoubleUDF(col("UNDO_RESTORE_REVERTED")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalREVISION_SESSION_ID", IntegerToDouble(col("REVISION_SESSION_ID")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberofRevisionsUserContributed", IntegerToDouble(col("NumberofRevisionsUserContributed")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberofUniqueItemsUseredit", IntegerToDouble(col("NumberofUniqueItemsUseredit")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberRevisionItemHas", IntegerToDouble(col("NumberRevisionItemHas")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberUniqUserEditItem", IntegerToDouble(col("NumberUniqUserEditItem")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalFreqItem", IntegerToDouble(col("FreqItem")))

    // ===========================================================================Caharacter Features : Double , Integer Features ========================================================
    // Double Ratio:  For Ratio Double column, Fill -1 value by Median:Character Features + Ratio of Word Features :
    var Samples = Fill_Missing_Final_All_Features.sample(false, 0.001).cache() // .where($"S2SimikaritySitelinkandLabel">0.0 || $"S3SimilarityLabelandSitelink">0.0 || $"S4SimilarityCommentComment">0.0)
    Samples.registerTempTable("df")

    val Query = "select " +
      "percentile_approx(C1uppercaseratio, 0.5) as meadian1" + "," + "percentile_approx(C2lowercaseratio, 0.5) as median2" + " ," +
      "percentile_approx(C3alphanumericratio, 0.5) as median3" + "," + "percentile_approx(C4asciiratio, 0.5) as median4" + "," +
      "percentile_approx(C5bracketratio, 0.5) as median5" + "," + "percentile_approx(C6digitalratio, 0.5) as median6" + "," +
      "percentile_approx(C7latinratio, 0.5) as median7" + "," + "percentile_approx(C8whitespaceratio, 0.5) as median8" + "," +
      "percentile_approx(C9puncratio, 0.5) as median9" + "," + "percentile_approx(C11arabicratio, 0.5) as median11" + "," +
      "percentile_approx(C12bengaliratio, 0.5) as median12" + "," + "percentile_approx(C13brahmiratio, 0.5) as median13" + "," +
      "percentile_approx(C14cyrilinratio, 0.5) as median14" + "," + "percentile_approx(C15hanratio, 0.5) as median15" + "," +
      "percentile_approx(c16malysiaratio, 0.5) as median16" + "," +
      "percentile_approx(C17tamiratio, 0.5) as median17" + "," + "percentile_approx(C18telugratio, 0.5) as median18" + "," +
      "percentile_approx(C19symbolratio, 0.5) as median19" + "," + "percentile_approx(C20alpharatio, 0.5) as median20" + "," +
      "percentile_approx(C21visibleratio, 0.5) as median21" + "," + "percentile_approx(C22printableratio, 0.5) as median22" + "," +
      "percentile_approx(C23blankratio, 0.5) as median23" + "," + "percentile_approx(C24controlratio, 0.5) as median24" + "," +
      "percentile_approx(C25hexaratio, 0.5) as median25" ++ "," + "percentile_approx(W1languagewordratio, 0.5) as median26" + "," +
      "percentile_approx(W3lowercaseratio, 0.5) as median27" + "," + "percentile_approx(W6badwordratio, 0.5) as median28" + "," +
      "percentile_approx(W7uppercaseratio, 0.5) as median27" + "," + "percentile_approx(W8banwordratio, 0.5) as median27" + " from df"

    val medianValues = sqlContext.sql(Query).rdd
    val Median = medianValues.first()

    // Median :
    // Character Ratio Features: UDF
    val lkpUDF1 = udf { (i: Double) => if (i == 0) Median(0).toString().toDouble else i }
    val lkpUDF2 = udf { (i: Double) => if (i == 0) Median(1).toString().toDouble else i }
    val lkpUDF3 = udf { (i: Double) => if (i == 0) Median(2).toString().toDouble else i }
    val lkpUDF4 = udf { (i: Double) => if (i == 0) Median(3).toString().toDouble else i }
    val lkpUDF5 = udf { (i: Double) => if (i == 0) Median(4).toString().toDouble else i }
    val lkpUDF6 = udf { (i: Double) => if (i == 0) Median(5).toString().toDouble else i }
    val lkpUDF7 = udf { (i: Double) => if (i == 0) Median(6).toString().toDouble else i }
    val lkpUDF8 = udf { (i: Double) => if (i == 0) Median(7).toString().toDouble else i }
    val lkpUDF9 = udf { (i: Double) => if (i == 0) Median(8).toString().toDouble else i }

    val lkpUDF11 = udf { (i: Double) => if (i == 0) Median(9).toString().toDouble else i }
    val lkpUDF12 = udf { (i: Double) => if (i == 0) Median(10).toString().toDouble else i }
    val lkpUDF13 = udf { (i: Double) => if (i == 0) Median(11).toString().toDouble else i }
    val lkpUDF14 = udf { (i: Double) => if (i == 0) Median(12).toString().toDouble else i }
    val lkpUDF15 = udf { (i: Double) => if (i == 0) Median(13).toString().toDouble else i }
    val lkpUDF16 = udf { (i: Double) => if (i == 0) Median(14).toString().toDouble else i }
    val lkpUDF17 = udf { (i: Double) => if (i == 0) Median(15).toString().toDouble else i }
    val lkpUDF18 = udf { (i: Double) => if (i == 0) Median(16).toString().toDouble else i }
    val lkpUDF19 = udf { (i: Double) => if (i == 0) Median(17).toString().toDouble else i }
    val lkpUDF20 = udf { (i: Double) => if (i == 0) Median(18).toString().toDouble else i }
    val lkpUDF21 = udf { (i: Double) => if (i == 0) Median(19).toString().toDouble else i }
    val lkpUDF22 = udf { (i: Double) => if (i == 0) Median(20).toString().toDouble else i }
    val lkpUDF23 = udf { (i: Double) => if (i == 0) Median(21).toString().toDouble else i }
    val lkpUDF24 = udf { (i: Double) => if (i == 0) Median(22).toString().toDouble else i }
    val lkpUDF25 = udf { (i: Double) => if (i == 0) Median(23).toString().toDouble else i }

    val df1 = Fill_Missing_Final_All_Features.withColumn("FinalC1uppercaseratio", lkpUDF1(col("C1uppercaseratio"))) // .drop("C1uppercaseratio").cache()
    val df2 = df1.withColumn("FinalC2lowercaseratio", lkpUDF2(col("C2lowercaseratio"))) // .drop("C2lowercaseratio").cache()
    // df1.unpersist()
    val df3 = df2.withColumn("FinalC3alphanumericratio", lkpUDF3(col("C3alphanumericratio"))) // .drop("C3alphanumericratio").cache()
    // df2.unpersist()
    val df4 = df3.withColumn("FinalC4asciiratio", lkpUDF4(col("C4asciiratio"))) // .drop("C4asciiratio").cache()
    // df3.unpersist()
    val df5 = df4.withColumn("FinalC5bracketratio", lkpUDF5(col("C5bracketratio"))) // .drop("C5bracketratio").cache()
    // df4.unpersist()
    val df6 = df5.withColumn("FinalC6digitalratio", lkpUDF6(col("C6digitalratio"))) // .drop("C6digitalratio").cache()
    // df5.unpersist()
    val df7 = df6.withColumn("FinalC7latinratio", lkpUDF7(col("C7latinratio"))) // .drop("C7latinratio").cache()
    // df6.unpersist()
    val df8 = df7.withColumn("FinalC8whitespaceratio", lkpUDF8(col("C8whitespaceratio"))) // .drop("C8whitespaceratio").cache()
    // df7.unpersist()
    val df9 = df8.withColumn("FinalC9puncratio", lkpUDF9(col("C9puncratio"))) // .drop("C9puncratio").cache()

    // Mean :
    // character integer values :
    val Mean_C10longcharacterseq = Samples.agg(mean("C10longcharacterseq")).head()
    val C10_Mean = Mean_C10longcharacterseq.getDouble(0)
    val lkpUDFC10 = udf { (i: Double) => if (i == 0) C10_Mean else i }
    val df10 = df9.withColumn("FinalC10longcharacterseq", lkpUDFC10(col("C10longcharacterseq")))

    // Median
    val df11 = df10.withColumn("FinalC11arabicratio", lkpUDF11(col("C11arabicratio"))) // .drop("C11arabicratio").cache()
    // df9.unpersist()
    val df12 = df11.withColumn("FinalC12bengaliratio", lkpUDF12(col("C12bengaliratio"))) // .drop("C12bengaliratio").cache()
    // df11.unpersist()
    val df13 = df12.withColumn("FinalC13brahmiratio", lkpUDF13(col("C13brahmiratio"))) // .drop("C13brahmiratio").cache()
    // df12.unpersist()
    val df14 = df13.withColumn("FinalC14cyrilinratio", lkpUDF14(col("C14cyrilinratio"))) // .drop("C14cyrilinratio").cache()
    // df13.unpersist()
    val df15 = df14.withColumn("FinalC15hanratio", lkpUDF15(col("C15hanratio"))) // .drop("C15hanratio").cache()
    // df14.unpersist()
    val df16 = df15.withColumn("Finalc16malysiaratio", lkpUDF16(col("c16malysiaratio"))) // .drop("c16malysiaratio").cache()
    // df15.unpersist()
    val df17 = df16.withColumn("FinalC17tamiratio", lkpUDF17(col("C17tamiratio"))) // .drop("C17tamiratio").cache()
    // df16.unpersist()
    val df18 = df17.withColumn("FinalC18telugratio", lkpUDF18(col("C18telugratio"))) // .drop("C18telugratio").cache()
    // df17.unpersist()
    val df19 = df18.withColumn("FinalC19symbolratio", lkpUDF19(col("C19symbolratio"))) // .drop("C19symbolratio").cache()
    // df18.unpersist()
    val df20 = df19.withColumn("FinalC20alpharatio", lkpUDF20(col("C20alpharatio"))) // .drop("C20alpharatio").cache()
    // df19.unpersist()
    val df21 = df20.withColumn("FinalC21visibleratio", lkpUDF21(col("C21visibleratio"))) // .drop("C21visibleratio").cache()
    // df20.unpersist()
    val df22 = df21.withColumn("FinalC22printableratio", lkpUDF22(col("C22printableratio"))) // .drop("C22printableratio").cache()
    // df21.unpersist()
    val df23 = df22.withColumn("FinalC23blankratio", lkpUDF23(col("C23blankratio"))) // .drop("C23blankratio").cache()
    // df22.unpersist()
    val df24 = df23.withColumn("FinalC24controlratio", lkpUDF24(col("C24controlratio"))) // .drop("C24controlratio").cache()
    // df23.unpersist()
    val df25 = df24.withColumn("FinalC25hexaratio", lkpUDF25(col("C25hexaratio"))) // .drop("C25hexaratio").cache()

    // ************************************************End Character Features ****************************************************************************************

    // ************************************************Start Word  Features ****************************************************************************************

    // Word Ratio Features : UDF
    val lkpUDFW1 = udf { (i: Double) => if (i == 0) Median(24).toString().toDouble else i }
    val lkpUDFW3 = udf { (i: Double) => if (i == 0) Median(25).toString().toDouble else i }
    val lkpUDFW6 = udf { (i: Double) => if (i == 0) Median(26).toString().toDouble else i }
    val lkpUDFW7 = udf { (i: Double) => if (i == 0) Median(27).toString().toDouble else i }
    val lkpUDFW8 = udf { (i: Double) => if (i == 0) Median(28).toString().toDouble else i }

    // 1.
    val df26 = df25.withColumn("FinalW1languagewordratio", lkpUDFW1(col("W1languagewordratio"))) // .drop("W1languagewordratio").cache()

    // 2.Boolean(Double) IsContainLanguageWord

    // 3.
    val df27 = df26.withColumn("FinalW3lowercaseratio", lkpUDFW3(col("W3lowercaseratio"))) // .drop("W3lowercaseratio").cache()
    // df26.unpersist()

    // 4. Integer " Mean:
    val Mean_W4longestword = Samples.agg(mean("W4longestword")).head()
    val W4_Mean = Mean_W4longestword.getDouble(0)
    val lkpUDFW4 = udf { (i: Double) => if (i == 0) W4_Mean else i }
    val df28 = df27.withColumn("FinalW4longestword", lkpUDFW4(col("W4longestword")))

    // 5. Boolean (Double ) W5IscontainURL
    // 6.
    val df29 = df28.withColumn("FinalW6badwordratio", lkpUDFW6(col("W6badwordratio"))) // .drop("W6badwordratio").cache()

    // 7.
    val df30 = df29.withColumn("FinalW7uppercaseratio", lkpUDFW7(col("W7uppercaseratio"))) // .drop("W7uppercaseratio").cache()

    // 8.
    val df31 = df30.withColumn("FinalW8banwordratio", lkpUDFW8(col("W8banwordratio"))) // .drop("W8banwordratio").cache()

    // 9.FemalFirst       Boolean(Double)
    // 10.Male First      Boolean(Double)
    // 11.ContainBadWord  Boolean(Double)
    // 12ContainBanWord   Boolean(Double)

    // 13. Integer(Double):
    val Mean_W13W13NumberSharewords = Samples.agg(mean("W13NumberSharewords")).head()
    val W13_Mean = Mean_W13W13NumberSharewords.getDouble(0)
    val lkpUDFW13 = udf { (i: Double) => if (i == 0) W13_Mean else i }
    val df32 = df31.withColumn("FinalW13NumberSharewords", lkpUDFW13(col("W13NumberSharewords")))

    // 14. Integer (Double):
    val Mean_W14NumberSharewordswithoutStopwords = Samples.agg(mean("W14NumberSharewordswithoutStopwords")).head()
    val W14_Mean = Mean_W14NumberSharewordswithoutStopwords.getDouble(0)
    val lkpUDFW14 = udf { (i: Double) => if (i == 0) W14_Mean else i }
    val df33 = df32.withColumn("FinalW14NumberSharewordswithoutStopwords", lkpUDFW14(col("W14NumberSharewordswithoutStopwords")))

    // 15. Double (Not ratio):
    val Mean_W15PortionQid = Samples.agg(mean("W15PortionQid")).head()
    val W15_Mean = Mean_W15PortionQid.getDouble(0)
    val lkpUDFW15 = udf { (i: Double) => if (i == 0) W15_Mean else i }
    val df34 = df33.withColumn("FinalW15PortionQid", lkpUDFW15(col("W15PortionQid")))

    // 16. Double(Not Ratio):
    val Mean_W16PortionLnags = Samples.agg(mean("W16PortionLnags")).head()
    val W16_Mean = Mean_W16PortionLnags.getDouble(0)
    val lkpUDFW16 = udf { (i: Double) => if (i == 0) W16_Mean else i }
    val df35 = df34.withColumn("FinalW16PortionLnags", lkpUDFW16(col("W16PortionLnags")))

    // 17.Double(Not ratio):
    val Mean_W17PortionLinks = Samples.agg(mean("W17PortionLinks")).head()
    val W17_Mean = Mean_W17PortionLinks.getDouble(0)
    val lkpUDFW17 = udf { (i: Double) => if (i == 0) W17_Mean else i }
    val df36 = df35.withColumn("FinalW17PortionLinks", lkpUDFW17(col("W17PortionLinks")))

    // ************************************************End Word  Features ****************************************************************************************

    // ************************************************Start Sentences  Features ****************************************************************************************
    // 1. Integer(Double)
    val Mean_S1CommentTailLength = Samples.agg(mean("S1CommentTailLength")).head()
    val S1_Mean = RoundDouble(Mean_S1CommentTailLength.getDouble(0))
    val lkpUDFS1 = udf { (i: Double) => if (i == 0) S1_Mean else i }
    val df37 = df36.withColumn("FinalS1CommentTailLength", lkpUDFS1(col("S1CommentTailLength")))

    // 2. Double  but Not ratio values :
    val Mean_S2SimikaritySitelinkandLabel = Samples.agg(mean("S2SimikaritySitelinkandLabel")).head()
    val S2_Mean = RoundDouble(Mean_S2SimikaritySitelinkandLabel.getDouble(0))
    val lkpUDFS2 = udf { (i: Double) => if (i == 0) S2_Mean else i }
    val df39 = df37.withColumn("FinalS2SimikaritySitelinkandLabel", lkpUDFS2(col("S2SimikaritySitelinkandLabel")))

    // 3. Double  but Not ratio values :
    val Mean_S3SimilarityLabelandSitelink = Samples.agg(mean("S3SimilarityLabelandSitelink")).head()
    val S3_Mean = RoundDouble(Mean_S3SimilarityLabelandSitelink.getDouble(0))
    val lkpUDFS3 = udf { (i: Double) => if (i == 0.0) S3_Mean else i }
    val df40 = df39.withColumn("FinalS3SimilarityLabelandSitelink", lkpUDFS3(col("S3SimilarityLabelandSitelink")))

    // 4.  Double  but Not ratio values :
    val Mean_S4SimilarityCommentComment = Samples.agg(mean("S4SimilarityCommentComment")).head()
    val S4_Mean = RoundDouble(Mean_S4SimilarityCommentComment.getDouble(0))
    val lkpUDFS4 = udf { (i: Double) => if (i == 0.0) S4_Mean else i }
    val df41 = df40.withColumn("FinalS4SimilarityCommentComment", lkpUDFS4(col("S4SimilarityCommentComment")))

    // df41.show()
    // ************************************************End Sentences  Features ****************************************************************************************
    // *********************************************** Start Statement  Features ****************************************************************************************
    // 1. String
    // 2. String
    // 3. String
    // ************************************************End Statement  Features ****************************************************************************************
    // *********************************************** Start User Features ****************************************************************************************

    // 1.Boolean(Double)
    // 2.Boolean(Double)
    // 3.Boolean(Double)
    // 4.Boolean(Double)
    // 5.Boolean(Double)
    // 6.Boolean(Double)
    // 7. (Double) IP No need to fill Missing Data
    // 8. (Double) ID No need to fill Missing Data
    // 9.Boolean(Double)
    // 10.Boolean(Double)

    // *********************************************** End User Features ****************************************************************************************
    // *********************************************** Start Item Features ****************************************************************************************
    // 1. Integer (Double) No need to fill missing values
    // 2. Integer (Double) No need to fill missing values
    // 3. Integer (Double) No need to fill missing values
    // 4. Integer (Double) No need to fill missing values
    // 5. Integer (Double) No need to fill missing values
    // 6. Integer (Double) No need to fill missing values
    // 7. Integer (Double) No need to fill missing values
    // 8. Integer (Double) No need to fill missing values
    // 9. Integer (Double) No need to fill missing values
    // 10. Integer (Double) No need to fill missing values
    // 11. String
    // *********************************************** End Item Features ****************************************************************************************
    // *********************************************** Start Revision Features ****************************************************************************************
    // 1.String
    // 2.String
    // 3.Boolean (Double)
    // 4.Integer(Double)
    // 5.String
    // 6.String
    // 7. Boolean(Double)
    // 8. String
    // 9.String
    // 10. Integer (Double)
    // 11.String
    // 12. integer(Double)
    // 13. Long(Double)
    // 14. integer (Double)
    // 15.String
    // 16.String
    // *********************************************** End Revision Features ****************************************************************************************
    // *********************************************** Meta Data , Truth Data and Frequnces  ****************************************************************************************
    // Meta
    //  1.Revision Session :Integer (Converted to Double)
    // 2. User Country Code
    // 3.User Continent Code
    // 4.User Time Size
    // 5.User Region Code
    // 6.User-city Name
    // 7.User Country Name
    // 8.RevisionTags

    // Truth:
    // 1.Undo

    // Freq :

    // 1.5 features

    // Roll Boolean     :Boolean (Double)
    // Undo             :Boolean (Double)

    // *********************************************** End Revision Features ****************************************************************************************

    // ===========================================================================String Features====================================================================================

    val df42 = df41.withColumn(
      // statement String features:
      "StringFeatures", concat($"SS1Property", lit(";"), $"SS2DataValue", lit(";"), $"SS3ItemValue", lit(";"), $"I11ItemTitle",
        // Revision  String Features:
        lit(";"), $"R1languageRevision",
        lit(";"), $"R2RevisionLanguageLocal",
        lit(";"), $"R5RevisionAction",
        lit(";"), $"R6PrevReviAction",
        lit(";"), $"R8ParRevision",
        lit(";"), $"R9RevisionTime",
        lit(";"), $"R11ContentType",
        lit(";"), $"R15RevisionSubaction",
        lit(";"), $"R16PrevReviSubaction",

        lit(";"), $"USER_COUNTRY_CODE",
        lit(";"), $"USER_CONTINENT_CODE",
        lit(";"), $"USER_TIME_ZONE",
        lit(";"), $"USER_REGION_CODE",
        lit(";"), $"USER_CITY_NAME",
        lit(";"), $"USER_COUNTY_NAME",
        lit(";"), $"REVISION_TAGS"))

    val toArray = udf((record: String) => record.split(";").map(_.toString()))
    val test1 = df42.withColumn("StringFeatures", toArray(col("StringFeatures")))
    //  test1.show()
    //  test1.printSchema()

    val word2Vec = new Word2Vec().setInputCol("StringFeatures").setOutputCol("result").setVectorSize(20).setMinCount(0)
    val model = word2Vec.fit(test1)
    val result = model.transform(test1) // .rdd

    // result.show()

    val Todense = udf((b: Vector) => b.toDense)
    val test_new2 = result.withColumn("result", Todense(col("result")))

    val assembler = new VectorAssembler().setInputCols(Array(
      "result",

      // character
      "FinalC1uppercaseratio", "FinalC2lowercaseratio", "FinalC3alphanumericratio", "FinalC4asciiratio", "FinalC5bracketratio", "FinalC6digitalratio",
      "FinalC7latinratio", "FinalC8whitespaceratio", "FinalC9puncratio", "FinalC10longcharacterseq", "FinalC11arabicratio", "FinalC12bengaliratio",
      "FinalC13brahmiratio", "FinalC14cyrilinratio", "FinalC15hanratio", "Finalc16malysiaratio", "FinalC17tamiratio", "FinalC18telugratio",
      "FinalC19symbolratio", "FinalC20alpharatio", "FinalC21visibleratio", "FinalC22printableratio", "FinalC23blankratio", "FinalC24controlratio", "FinalC25hexaratio",

      // Words
      "FinalW1languagewordratio", "W2Iscontainlanguageword", "FinalW3lowercaseratio", "FinalW4longestword", "W5IscontainURL", "FinalW6badwordratio",
      "FinalW7uppercaseratio", "FinalW8banwordratio", "W9FemalFirstName", "W10MaleFirstName", "W11IscontainBadword", "W12IsContainBanword",
      "FinalW13NumberSharewords", "FinalW14NumberSharewordswithoutStopwords", "FinalW15PortionQid", "FinalW16PortionLnags", "FinalW17PortionLinks",

      // Sentences :
      "FinalS1CommentTailLength", "FinalS2SimikaritySitelinkandLabel", "FinalS3SimilarityLabelandSitelink", "FinalS4SimilarityCommentComment",

      // User :
      "U1IsPrivileged", "U2IsBotUser", "U3IsBotuserWithFlaguser", "U4IsProperty", "U5IsTranslator", "U6IsRegister", "U7IPValue", "U8UserID",
      "U9HasBirthDate", "U10HasDeathDate",

      // Item:

      "I1NumberLabels", "I2NumberDescription", "I3NumberAliases", "I4NumberClaims", "I5NumberSitelinks", "I6NumberStatement",
      "I7NumberReferences", "I8NumberQualifier", "I9NumberQualifierOrder", "I10NumberBadges",

      // Revision:
      "R3IslatainLanguage", "R4JsonLength", "R7RevisionAccountChange", "R10RevisionSize", "R12BytesIncrease",
      "R13TimeSinceLastRevi", "R14CommentLength",

      // Meta , truth , Freq
      // meta :
      "FinalREVISION_SESSION_ID",
      // Truth:
      "FinalUNDO_RESTORE_REVERTED",

      // Freq:
      "FinalNumberofRevisionsUserContributed",
      "FinalNumberofUniqueItemsUseredit", "FinalNumberRevisionItemHas", "FinalNumberUniqUserEditItem", "FinalFreqItem")).setOutputCol("features")
    val Training_Data = assembler.transform(test_new2)

    // Prepare the data for classification:
    //  NewData.registerTempTable("DB")
    //  val Training_Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB")
    // val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB") // for logistic regrision

    // Data.show()

    // val TestClassifiers = new Classifiers()
    //
    //   TestClassifiers.RandomForestClassifer(Data, sqlContext)
    //      // TestClassifiers.DecisionTreeClassifier(Data, sqlContext)
    //      // TestClassifiers.LogisticRegrision(Data, sqlContext)
    //      // TestClassifiers.GradientBoostedTree(Data, sqlContext)
    //      // TestClassifiers.MultilayerPerceptronClassifier(Data, sqlContext)

    Training_Data

  }

  // ***********************************************************************************************************************************************
  // Function 3:Testing XML and Vandalism Detection
  def Testing_Start_StandardXMLParser_VD(sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._ // for UDF
    import org.apache.spark.sql.types._

    // Streaming records:
    val jobConf = new JobConf()
    val NormalXML_Parser_OBJ = new ParseNormalXML()
    val RDD_OBJ = new ParseNormalXML()

    val Testing_RDD_All_Record = RDD_OBJ.Testing_DB_NormalXML_Parser(sc).cache()

    // ======= Json part :
    // Json RDD : Each record has its Revision iD:
    val JsonRDD = Testing_RDD_All_Record.map(_.split("NNLL")).map(v => replacing_with_Quoto(v(0), v(8))).cache()
    // JsonRDD.foreach(println)
    // println(JsonRDD.count())

    // Data set
    val Ds_Json = sqlContext.jsonRDD(JsonRDD).select("key", "id", "labels", "descriptions", "aliases", "claims", "sitelinks").cache()
    // Ds_Json.show()
    // println(Ds_Json.count())

    // ======= Tags part : // Contributor IP here is in Decimal format not IP format and It is converted in ParseNormalXml stage
    val TagsRDD = Testing_RDD_All_Record.map(_.split("NNLL")).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))).cache()
    val DF_Tags = TagsRDD.toDF("Rid", "Itemid", "comment", "pid", "time", "contributorIP",
      "contributorID", "contributorName", "JsonText", "model", "format", "sha").cache()
    //    DF_Tags.show()
    //    println(DF_Tags.count())

    // ======== Join Json part with Tag Part:============================
    // Joining to have full data
    val DF_First_DF_Result_Join_Tags_and_Json = DF_Tags.as("T1").join(Ds_Json.as("T2"), $"T1.Rid" === $"T2.key", "leftouter").select("Rid", "itemid", "comment", "pid", "time",
      "contributorIP", "contributorID", "contributorName", "JsonText", "labels", "descriptions",
      "aliases", "claims", "sitelinks", "model", "format", "sha") // .orderBy("Rid", "Itemid")
    DF_First_DF_Result_Join_Tags_and_Json.registerTempTable("Data1")
    val dfr_DATA_JsonTages1 = sqlContext.sql("select * from Data1 order by itemid ,Rid ").cache()

    val colNames = Seq("Rid2", "itemid2", "comment2", "pid2", "time2", "contributorIP2", "contributorID2", "contributorName2", "JsonText2", "labels2", "descriptions2",
      "aliases2", "claims2", "sitelinks2", "model2", "format2", "sha2")
    val DF_Second = DF_First_DF_Result_Join_Tags_and_Json.toDF(colNames: _*) // .distinct()
    DF_Second.registerTempTable("Data2")

    // ===================================================================Parent // Previous Revision==============================================================================================================

    // Joining based on Parent Id to get the previous cases: ParentID
    val DF_Joined = DF_First_DF_Result_Join_Tags_and_Json.as("df1").join(DF_Second.as("df2"), $"df1.pid" === $"df2.Rid2", "leftouter").distinct()

    val RDD_After_JoinDF = DF_Joined.rdd.distinct()
    val x = RDD_After_JoinDF.map(row => (row(0).toString().toInt, row)).cache()
    val part = new RangePartitioner(4, x)
    val partitioned = x.partitionBy(part).persist() // persist is important for this case and obligatory.
    // partitioned.foreach(println)
    //
    //      //=====================================================All Features Based on Categories of Features Data Type :==================================================================================
    //
    val Result_all_Features = partitioned.map { case (x, y) => (x.toString() + "," + All_Features(y).toString()) } // we convert the Pair RDD to String one LineRDD to be able to make DF based on ","
    // Result_all_Features.foreach(println)
    // println("nayef" + Result_all_Features.count())

    // Conver the RDD of All Features to  DataFrame:

    val schema = StructType(

      // 0
      StructField("Rid", IntegerType, false) ::

        // Character Features :
        /* 1 */ StructField("C1uppercaseratio", DoubleType, false) :: /* 2 */ StructField("C2lowercaseratio", DoubleType, false) :: /* 3 */ StructField("C3alphanumericratio", DoubleType, false) ::
        /* 4 */ StructField("C4asciiratio", DoubleType, false) :: /* 5 */ StructField("C5bracketratio", DoubleType, false) :: /* 6 */ StructField("C6digitalratio", DoubleType, false) ::
        /* 7 */ StructField("C7latinratio", DoubleType, false) :: /* 8 */ StructField("C8whitespaceratio", DoubleType, false) :: /* 9 */ StructField("C9puncratio", DoubleType, false) ::
        /* 10 */ StructField("C10longcharacterseq", DoubleType, false) :: /* 11 */ StructField("C11arabicratio", DoubleType, false) :: /* 12 */ StructField("C12bengaliratio", DoubleType, false) ::
        /* 13 */ StructField("C13brahmiratio", DoubleType, false) :: /* 14 */ StructField("C14cyrilinratio", DoubleType, false) :: /* 15 */ StructField("C15hanratio", DoubleType, false) ::
        /* 16 */ StructField("c16malysiaratio", DoubleType, false) :: /* 17 */ StructField("C17tamiratio", DoubleType, false) :: /* 18 */ StructField("C18telugratio", DoubleType, false) ::
        /* 19 */ StructField("C19symbolratio", DoubleType, false) :: /* 20 */ StructField("C20alpharatio", DoubleType, false) :: /* 21 */ StructField("C21visibleratio", DoubleType, false) ::
        /* 22 */ StructField("C22printableratio", DoubleType, false) :: /* 23 */ StructField("C23blankratio", DoubleType, false) :: /* 24 */ StructField("C24controlratio", DoubleType, false) ::
        /* 25 */ StructField("C25hexaratio", DoubleType, false) ::

        // word Features:
        /* 26 */ StructField("W1languagewordratio", DoubleType, false) :: /* 27 Boolean */ StructField("W2Iscontainlanguageword", DoubleType, false) :: /* 28 */ StructField("W3lowercaseratio", DoubleType, false) ::
        /* 29 Integer */ StructField("W4longestword", IntegerType, false) :: /* 30 Boolean */ StructField("W5IscontainURL", DoubleType, false) :: /* 31 */ StructField("W6badwordratio", DoubleType, false) ::
        /* 32 */ StructField("W7uppercaseratio", DoubleType, false) :: /* 33 */ StructField("W8banwordratio", DoubleType, false) :: /* 34 Boolean */ StructField("W9FemalFirstName", DoubleType, false) ::
        /* 35 Boolean */ StructField("W10MaleFirstName", DoubleType, false) :: /* 36 Boolean */ StructField("W11IscontainBadword", DoubleType, false) ::
        /* 37 Boolean */ StructField("W12IsContainBanword", DoubleType, false) ::
        /* 38 integer */ StructField("W13NumberSharewords", DoubleType, false) :: /* 39 Integer */ StructField("W14NumberSharewordswithoutStopwords", DoubleType, false) ::
        /* 40 */ StructField("W15PortionQid", DoubleType, false) :: /* 41 */ StructField("W16PortionLnags", DoubleType, false) :: /* 42 */ StructField("W17PortionLinks", DoubleType, false) ::

        //
        //          // Sentences Features:
        /* 43 */ StructField("S1CommentTailLength", DoubleType, false) :: /* 44 */ StructField("S2SimikaritySitelinkandLabel", DoubleType, false) ::
        /* 45 */ StructField("S3SimilarityLabelandSitelink", DoubleType, false) ::
        /* 46 */ StructField("S4SimilarityCommentComment", DoubleType, false) ::
        //
        //          // Statements Features :
        /* 47 */ StructField("SS1Property", StringType, false) :: /* 48 */ StructField("SS2DataValue", StringType, false) :: /* 49 */ StructField("SS3ItemValue", StringType, false) ::
        //
        //
        //        //User Features :
        /* 50 Boolean */ StructField("U1IsPrivileged", DoubleType, false) :: /* 51 Boolean */ StructField("U2IsBotUser", DoubleType, false) :: /* 52 Boolean */ StructField("U3IsBotuserWithFlaguser", DoubleType, false) ::
        /* 53 Boolean */ StructField("U4IsProperty", DoubleType, false) :: /* 54 Boolean */ StructField("U5IsTranslator", DoubleType, false) :: /* 55 Boolean */ StructField("U6IsRegister", DoubleType, false) ::
        /* 56 */ StructField("U7IPValue", DoubleType, false) :: /* 57 */ StructField("U8UserID", IntegerType, false) :: /* 58 */ StructField("U9HasBirthDate", DoubleType, false) ::
        /* 59 */ StructField("U10HasDeathDate", DoubleType, false) ::

        // Items Features :

        /* 60 */ StructField("I1NumberLabels", DoubleType, false) :: /* 61 */ StructField("I2NumberDescription", DoubleType, false) :: /* 62 */ StructField("I3NumberAliases", DoubleType, false) ::
        /* 63 */ StructField("I4NumberClaims", DoubleType, false) ::
        /* 64 */ StructField("I5NumberSitelinks", DoubleType, false) :: /* 65 */ StructField("I6NumberStatement", DoubleType, false) :: /* 66 */ StructField("I7NumberReferences", DoubleType, false) ::
        /* 67 */ StructField("I8NumberQualifier", DoubleType, false) ::
        /* 68 */ StructField("I9NumberQualifierOrder", DoubleType, false) :: /* 69 */ StructField("I10NumberBadges", DoubleType, false) :: /* 70 */ StructField("I11ItemTitle", StringType, false) ::

        // Revision Features:
        /* 71 */ StructField("R1languageRevision", StringType, false) :: /* 72 */ StructField("R2RevisionLanguageLocal", StringType, false) :: /* 73 */ StructField("R3IslatainLanguage", DoubleType, false) ::
        /* 74 */ StructField("R4JsonLength", DoubleType, false) :: /* 75 */ StructField("R5RevisionAction", StringType, false) :: /* 76 */ StructField("R6PrevReviAction", StringType, false) ::
        /* 77 */ StructField("R7RevisionAccountChange", DoubleType, false) :: /* 78 */ StructField("R8ParRevision", StringType, false) :: /* 79 */ StructField("R9RevisionTime", StringType, false) ::
        /* 80 */ StructField("R10RevisionSize", DoubleType, false) :: /* 81 */ StructField("R11ContentType", StringType, false) :: /* 82 */ StructField("R12BytesIncrease", DoubleType, false) ::
        /* 83 */ StructField("R13TimeSinceLastRevi", DoubleType, false) :: /* 84 */ StructField("R14CommentLength", DoubleType, false) :: /* 85 */ StructField("R15RevisionSubaction", StringType, false) ::
        /* 86 */ StructField("R16PrevReviSubaction", StringType, false) ::

        Nil)

    val rowRDD = Result_all_Features.map(line => line.split(",")).map(e ⇒ Row(e(0).toInt // character feature column
    , e(1).toDouble, e(2).toDouble, e(3).toDouble, e(4).toDouble, e(5).toDouble, e(6).toDouble, e(7).toDouble, e(8).toDouble, e(9).toDouble, RoundDouble(e(10).toDouble), e(11).toDouble, e(12).toDouble, e(13).toDouble //
    , e(14).toDouble, e(15).toDouble, e(16).toDouble, e(17).toDouble, e(18).toDouble, e(19).toDouble, e(20).toDouble, e(21).toDouble, e(22).toDouble, e(23).toDouble, e(24).toDouble, e(25).toDouble // Word Feature column
    , e(26).toDouble, e(27).toDouble, e(28).toDouble, e(29).toDouble.toInt, e(30).toDouble, e(31).toDouble, e(32).toDouble, e(33).toDouble, e(34).toDouble, e(35).toDouble, e(36).toDouble, e(37).toDouble //
    , RoundDouble(e(38).toDouble), RoundDouble(e(39).toDouble), e(40).toDouble, e(41).toDouble, e(42).toDouble // Sentences Features column:
    , RoundDouble(e(43).toDouble), e(44).toDouble, e(45).toDouble, e(46).toDouble // Statement Features Column:
    , e(47), e(48), e(49) // User Features Column:
    , e(50).toDouble, e(51).toDouble, e(52).toDouble, e(53).toDouble, e(54).toDouble, e(55).toDouble, e(56).toDouble, e(57).toDouble.toInt, e(58).toDouble, e(59).toDouble // Item Features column:
    , e(60).toDouble, e(61).toDouble, e(62).toDouble, e(63).toDouble, e(64).toDouble, e(65).toDouble, e(66).toDouble, e(67).toDouble //
    , e(68).toDouble, e(69).toDouble, "Q" + e(70).toDouble.toInt.toString() // Revision Features Column:
    , e(71), e(72), e(73).toDouble, e(74).toDouble, e(75), e(76), e(77).toDouble, e(78), e(79), e(80).toDouble, e(81), e(82).toDouble, e(83).toDouble, e(84).toDouble, e(85), e(86)))

    // a.User Frequency:
    // number of revisions a user has contributed
    // val resu= DF_Tags.groupBy("contributorID").agg(count("Rid"))
    DF_Tags.registerTempTable("TagesTable")
    val ContributorFreq_for_Each_Revision_DF = sqlContext
      .sql("select contributorID as CIDUSER1, count(Rid) as NumberofRevisionsUserContributed from TagesTable where contributorID !='0' group by contributorID ") // .drop("CIDUSER1")
    // ContributorFreq_for_Each_Revision_DF.show()

    // b.Cumulated : Number of a unique Item a user has contributed.
    val CumulatedNumberof_uniqueItemsForUser_DF = sqlContext
      .sql("select contributorID as CIDUSER2,  COUNT(DISTINCT itemid) as NumberofUniqueItemsUseredit from TagesTable where contributorID !='0' group by contributorID") // .drop("CIDUSER2")
    // CumulatedNumberof_uniqueItemsForUser_DF.show()

    // 1.Item Frequency:
    // number of revisions an Item has
    val ItemFrequ_DF = sqlContext
      .sql("select itemid, count(Rid) as NumberRevisionItemHas from TagesTable  group by itemid")
    // ItemFrequ_DF.show()

    // 2. Cumulate number of unique users have edited the Item : Did not consider the users IP. Contributor is an IP or Name. we consider name
    val CumulatedNumberof_UniqueUserForItem_DF = sqlContext.sql("select itemid,  COUNT(DISTINCT contributorID) as NumberUniqUserEditItem from TagesTable where contributorID !='0' group by itemid")
    // CumulatedNumberof_UniqueUserForItem_DF.show()

    // 3. freq each Item :
    val Fre_Item_DF = sqlContext.sql("select itemid,  COUNT(itemid) as FreqItem from TagesTable  group by itemid")
    // Fre_Item_DF.show()

    // *****************************************************************************************************************************************
    // This is Main DataFrame:
    val BeforeJoin_All_Features = sqlContext.createDataFrame(rowRDD, schema)
    // BeforeJoin_All_Features.show()

    // ********************************** User feature Join

    // Join1 for add The first User Feature : number of revisions a user has contributed
    val AfterJoinUser1_All_Features = BeforeJoin_All_Features.as("T1").join(ContributorFreq_for_Each_Revision_DF.as("T2"), $"T1.U8UserID" === $"T2.CIDUSER1", "leftouter").drop("CIDUSER1")
    // AfterJoinUser1_All_Features.show()

    // Join2 for add The second  User Feature
    val AfterJoinUser2_All_Features = AfterJoinUser1_All_Features.as("T1").join(CumulatedNumberof_uniqueItemsForUser_DF.as("T2"), $"T1.U8UserID" === $"T2.CIDUSER2", "leftouter").drop("CIDUSER2")
    // AfterJoinUser2_All_Features.show()

    // ********************************** Item Feature Join
    // Join3 for add The First  Item Feature :number of revisions an Item has
    val AfterJoinItem3_All_Features = AfterJoinUser2_All_Features.as("T1").join(ItemFrequ_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")
    // AfterJoinItem3_All_Features.show()

    // Join4 for add The Second  Item Feature
    val AfterJoinItem4_All_Features = AfterJoinItem3_All_Features.as("T1").join(CumulatedNumberof_UniqueUserForItem_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")
    // AfterJoinItem4_All_Features.show()

    // Join5 for add The Third  Item Feature
    val AfterJoinItem5_All_Features = AfterJoinItem4_All_Features.as("T1").join(Fre_Item_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")
    // 2 AfterJoinItem5_All_Features.show()

    // ********************************

    // *Geografical information Feature from Meta File
    // REVISION_ID|REVISION_SESSION_ID|USER_COUNTRY_CODE|USER_CONTINENT_CODE|USER_TIME_ZONE|USER_REGION_CODE|USER_CITY_NAME|USER_COUNTY_NAME|REVISION_TAGS
    val df_GeoInf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("hdfs://localhost:9000/mydata/Meta.csv").select("REVISION_ID", "REVISION_SESSION_ID", "USER_COUNTRY_CODE", "USER_CONTINENT_CODE", "USER_TIME_ZONE",
        "USER_REGION_CODE", "USER_CITY_NAME", "USER_COUNTY_NAME", "REVISION_TAGS")
    // df_GeoInf.show()

    val df_Truth = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("hdfs://localhost:9000/mydata/truth.csv").select("REVISION_ID", "ROLLBACK_REVERTED", "UNDO_RESTORE_REVERTED")
    // df_GeoInf.show()

    val AfterJoinGeoInfo_All_Features = AfterJoinItem5_All_Features.as("T1").join(df_GeoInf.as("T2"), $"T1.Rid" === $"T2.REVISION_ID", "leftouter").drop("REVISION_ID").cache()
    // AfterJoinGeoInfo_All_Features.show()

    val Final_All_Features = AfterJoinGeoInfo_All_Features.as("T1").join(df_Truth.as("T2"), $"T1.Rid" === $"T2.REVISION_ID", "leftouter").drop("REVISION_ID").cache()
    // Final_All_Features.show()

    // Pre- process Data ============================================================================================================================================================

    // For String Column, We fill the Null values by "NA":

    var Fill_Missing_Final_All_Features = Final_All_Features.na.fill("NA", Seq("USER_COUNTRY_CODE", "USER_CONTINENT_CODE",
      "USER_TIME_ZONE", "USER_REGION_CODE", "USER_CITY_NAME", "USER_COUNTY_NAME", "REVISION_TAGS")).cache()

    // For Integer Frequency  Column, We fill the Null values by 0:
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.na.fill(0, Seq("FreqItem", "NumberUniqUserEditItem", "NumberRevisionItemHas",
      "NumberofUniqueItemsUseredit", "NumberofRevisionsUserContributed", "REVISION_SESSION_ID")).cache()
    // Fill_Missing_Final_All_Features.show()

    val BoolToDoubleUDF = udf { (BoolAsString: String) => if (BoolAsString == "T") 1.0 else 0.0 }
    val IntegerToDouble = udf { (IntegerRevisionSessionID: Integer) => IntegerRevisionSessionID.toDouble }
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalROLLBACK_REVERTED", BoolToDoubleUDF(col("ROLLBACK_REVERTED")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUNDO_RESTORE_REVERTED", BoolToDoubleUDF(col("UNDO_RESTORE_REVERTED")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalREVISION_SESSION_ID", IntegerToDouble(col("REVISION_SESSION_ID")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberofRevisionsUserContributed", IntegerToDouble(col("NumberofRevisionsUserContributed")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberofUniqueItemsUseredit", IntegerToDouble(col("NumberofUniqueItemsUseredit")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberRevisionItemHas", IntegerToDouble(col("NumberRevisionItemHas")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberUniqUserEditItem", IntegerToDouble(col("NumberUniqUserEditItem")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalFreqItem", IntegerToDouble(col("FreqItem")))

    // ===========================================================================Caharacter Features : Double , Integer Features ====================================================================================
    // Double Ratio:  For Ratio Double column, Fill -1 value by Median:Character Features + Ratio of Word Features :
    var Samples = Fill_Missing_Final_All_Features.sample(false, 0.001).cache() // .where($"S2SimikaritySitelinkandLabel">0.0 || $"S3SimilarityLabelandSitelink">0.0 || $"S4SimilarityCommentComment">0.0)
    Samples.registerTempTable("df")

    val Query = "select " +
      "percentile_approx(C1uppercaseratio, 0.5) as meadian1" + "," + "percentile_approx(C2lowercaseratio, 0.5) as median2" + " ," +
      "percentile_approx(C3alphanumericratio, 0.5) as median3" + "," + "percentile_approx(C4asciiratio, 0.5) as median4" + "," +
      "percentile_approx(C5bracketratio, 0.5) as median5" + "," + "percentile_approx(C6digitalratio, 0.5) as median6" + "," +
      "percentile_approx(C7latinratio, 0.5) as median7" + "," + "percentile_approx(C8whitespaceratio, 0.5) as median8" + "," +
      "percentile_approx(C9puncratio, 0.5) as median9" + "," + "percentile_approx(C11arabicratio, 0.5) as median11" + "," +
      "percentile_approx(C12bengaliratio, 0.5) as median12" + "," + "percentile_approx(C13brahmiratio, 0.5) as median13" + "," +
      "percentile_approx(C14cyrilinratio, 0.5) as median14" + "," + "percentile_approx(C15hanratio, 0.5) as median15" + "," +
      "percentile_approx(c16malysiaratio, 0.5) as median16" + "," +
      "percentile_approx(C17tamiratio, 0.5) as median17" + "," + "percentile_approx(C18telugratio, 0.5) as median18" + "," +
      "percentile_approx(C19symbolratio, 0.5) as median19" + "," + "percentile_approx(C20alpharatio, 0.5) as median20" + "," +
      "percentile_approx(C21visibleratio, 0.5) as median21" + "," + "percentile_approx(C22printableratio, 0.5) as median22" + "," +
      "percentile_approx(C23blankratio, 0.5) as median23" + "," + "percentile_approx(C24controlratio, 0.5) as median24" + "," +
      "percentile_approx(C25hexaratio, 0.5) as median25" ++ "," + "percentile_approx(W1languagewordratio, 0.5) as median26" + "," +
      "percentile_approx(W3lowercaseratio, 0.5) as median27" + "," + "percentile_approx(W6badwordratio, 0.5) as median28" + "," +
      "percentile_approx(W7uppercaseratio, 0.5) as median27" + "," + "percentile_approx(W8banwordratio, 0.5) as median27" + " from df"

    val medianValues = sqlContext.sql(Query).rdd
    val Median = medianValues.first()

    // Median :
    // Character Ratio Features: UDF
    val lkpUDF1 = udf { (i: Double) => if (i == 0) Median(0).toString().toDouble else i }
    val lkpUDF2 = udf { (i: Double) => if (i == 0) Median(1).toString().toDouble else i }
    val lkpUDF3 = udf { (i: Double) => if (i == 0) Median(2).toString().toDouble else i }
    val lkpUDF4 = udf { (i: Double) => if (i == 0) Median(3).toString().toDouble else i }
    val lkpUDF5 = udf { (i: Double) => if (i == 0) Median(4).toString().toDouble else i }
    val lkpUDF6 = udf { (i: Double) => if (i == 0) Median(5).toString().toDouble else i }
    val lkpUDF7 = udf { (i: Double) => if (i == 0) Median(6).toString().toDouble else i }
    val lkpUDF8 = udf { (i: Double) => if (i == 0) Median(7).toString().toDouble else i }
    val lkpUDF9 = udf { (i: Double) => if (i == 0) Median(8).toString().toDouble else i }

    val lkpUDF11 = udf { (i: Double) => if (i == 0) Median(9).toString().toDouble else i }
    val lkpUDF12 = udf { (i: Double) => if (i == 0) Median(10).toString().toDouble else i }
    val lkpUDF13 = udf { (i: Double) => if (i == 0) Median(11).toString().toDouble else i }
    val lkpUDF14 = udf { (i: Double) => if (i == 0) Median(12).toString().toDouble else i }
    val lkpUDF15 = udf { (i: Double) => if (i == 0) Median(13).toString().toDouble else i }
    val lkpUDF16 = udf { (i: Double) => if (i == 0) Median(14).toString().toDouble else i }
    val lkpUDF17 = udf { (i: Double) => if (i == 0) Median(15).toString().toDouble else i }
    val lkpUDF18 = udf { (i: Double) => if (i == 0) Median(16).toString().toDouble else i }
    val lkpUDF19 = udf { (i: Double) => if (i == 0) Median(17).toString().toDouble else i }
    val lkpUDF20 = udf { (i: Double) => if (i == 0) Median(18).toString().toDouble else i }
    val lkpUDF21 = udf { (i: Double) => if (i == 0) Median(19).toString().toDouble else i }
    val lkpUDF22 = udf { (i: Double) => if (i == 0) Median(20).toString().toDouble else i }
    val lkpUDF23 = udf { (i: Double) => if (i == 0) Median(21).toString().toDouble else i }
    val lkpUDF24 = udf { (i: Double) => if (i == 0) Median(22).toString().toDouble else i }
    val lkpUDF25 = udf { (i: Double) => if (i == 0) Median(23).toString().toDouble else i }

    val df1 = Fill_Missing_Final_All_Features.withColumn("FinalC1uppercaseratio", lkpUDF1(col("C1uppercaseratio"))) // .drop("C1uppercaseratio").cache()
    val df2 = df1.withColumn("FinalC2lowercaseratio", lkpUDF2(col("C2lowercaseratio"))) // .drop("C2lowercaseratio").cache()
    // df1.unpersist()
    val df3 = df2.withColumn("FinalC3alphanumericratio", lkpUDF3(col("C3alphanumericratio"))) // .drop("C3alphanumericratio").cache()
    // df2.unpersist()
    val df4 = df3.withColumn("FinalC4asciiratio", lkpUDF4(col("C4asciiratio"))) // .drop("C4asciiratio").cache()
    // df3.unpersist()
    val df5 = df4.withColumn("FinalC5bracketratio", lkpUDF5(col("C5bracketratio"))) // .drop("C5bracketratio").cache()
    // df4.unpersist()
    val df6 = df5.withColumn("FinalC6digitalratio", lkpUDF6(col("C6digitalratio"))) // .drop("C6digitalratio").cache()
    // df5.unpersist()
    val df7 = df6.withColumn("FinalC7latinratio", lkpUDF7(col("C7latinratio"))) // .drop("C7latinratio").cache()
    // df6.unpersist()
    val df8 = df7.withColumn("FinalC8whitespaceratio", lkpUDF8(col("C8whitespaceratio"))) // .drop("C8whitespaceratio").cache()
    // df7.unpersist()
    val df9 = df8.withColumn("FinalC9puncratio", lkpUDF9(col("C9puncratio"))) // .drop("C9puncratio").cache()

    // Mean :
    // character integer values :
    val Mean_C10longcharacterseq = Samples.agg(mean("C10longcharacterseq")).head()
    val C10_Mean = Mean_C10longcharacterseq.getDouble(0)
    val lkpUDFC10 = udf { (i: Double) => if (i == 0) C10_Mean else i }
    val df10 = df9.withColumn("FinalC10longcharacterseq", lkpUDFC10(col("C10longcharacterseq")))

    // Median
    val df11 = df10.withColumn("FinalC11arabicratio", lkpUDF11(col("C11arabicratio"))) // .drop("C11arabicratio").cache()
    // df9.unpersist()
    val df12 = df11.withColumn("FinalC12bengaliratio", lkpUDF12(col("C12bengaliratio"))) // .drop("C12bengaliratio").cache()
    // df11.unpersist()
    val df13 = df12.withColumn("FinalC13brahmiratio", lkpUDF13(col("C13brahmiratio"))) // .drop("C13brahmiratio").cache()
    // df12.unpersist()
    val df14 = df13.withColumn("FinalC14cyrilinratio", lkpUDF14(col("C14cyrilinratio"))) // .drop("C14cyrilinratio").cache()
    // df13.unpersist()
    val df15 = df14.withColumn("FinalC15hanratio", lkpUDF15(col("C15hanratio"))) // .drop("C15hanratio").cache()
    // df14.unpersist()
    val df16 = df15.withColumn("Finalc16malysiaratio", lkpUDF16(col("c16malysiaratio"))) // .drop("c16malysiaratio").cache()
    // df15.unpersist()
    val df17 = df16.withColumn("FinalC17tamiratio", lkpUDF17(col("C17tamiratio"))) // .drop("C17tamiratio").cache()
    //  df16.unpersist()
    val df18 = df17.withColumn("FinalC18telugratio", lkpUDF18(col("C18telugratio"))) // .drop("C18telugratio").cache()
    // df17.unpersist()
    val df19 = df18.withColumn("FinalC19symbolratio", lkpUDF19(col("C19symbolratio"))) // .drop("C19symbolratio").cache()
    // df18.unpersist()
    val df20 = df19.withColumn("FinalC20alpharatio", lkpUDF20(col("C20alpharatio"))) // .drop("C20alpharatio").cache()
    // df19.unpersist()
    val df21 = df20.withColumn("FinalC21visibleratio", lkpUDF21(col("C21visibleratio"))) // .drop("C21visibleratio").cache()
    // df20.unpersist()
    val df22 = df21.withColumn("FinalC22printableratio", lkpUDF22(col("C22printableratio"))) // .drop("C22printableratio").cache()
    // df21.unpersist()
    val df23 = df22.withColumn("FinalC23blankratio", lkpUDF23(col("C23blankratio"))) // .drop("C23blankratio").cache()
    // df22.unpersist()
    val df24 = df23.withColumn("FinalC24controlratio", lkpUDF24(col("C24controlratio"))) // .drop("C24controlratio").cache()
    // df23.unpersist()
    val df25 = df24.withColumn("FinalC25hexaratio", lkpUDF25(col("C25hexaratio"))) // .drop("C25hexaratio").cache()

    // ************************************************End Character Features ****************************************************************************************

    // ************************************************Start Word  Features ****************************************************************************************

    // Word Ratio Features : UDF
    val lkpUDFW1 = udf { (i: Double) => if (i == 0) Median(24).toString().toDouble else i }
    val lkpUDFW3 = udf { (i: Double) => if (i == 0) Median(25).toString().toDouble else i }
    val lkpUDFW6 = udf { (i: Double) => if (i == 0) Median(26).toString().toDouble else i }
    val lkpUDFW7 = udf { (i: Double) => if (i == 0) Median(27).toString().toDouble else i }
    val lkpUDFW8 = udf { (i: Double) => if (i == 0) Median(28).toString().toDouble else i }

    // 1.
    val df26 = df25.withColumn("FinalW1languagewordratio", lkpUDFW1(col("W1languagewordratio"))) // .drop("W1languagewordratio").cache()

    // 2.Boolean(Double) IsContainLanguageWord

    // 3.
    val df27 = df26.withColumn("FinalW3lowercaseratio", lkpUDFW3(col("W3lowercaseratio"))) // .drop("W3lowercaseratio").cache()
    // df26.unpersist()

    // 4. Integer " Mean:
    val Mean_W4longestword = Samples.agg(mean("W4longestword")).head()
    val W4_Mean = Mean_W4longestword.getDouble(0)
    val lkpUDFW4 = udf { (i: Double) => if (i == 0) W4_Mean else i }
    val df28 = df27.withColumn("FinalW4longestword", lkpUDFW4(col("W4longestword")))

    // 5. Boolean (Double ) W5IscontainURL
    // 6.
    val df29 = df28.withColumn("FinalW6badwordratio", lkpUDFW6(col("W6badwordratio"))) // .drop("W6badwordratio").cache()

    // 7.
    val df30 = df29.withColumn("FinalW7uppercaseratio", lkpUDFW7(col("W7uppercaseratio"))) // .drop("W7uppercaseratio").cache()

    // 8.
    val df31 = df30.withColumn("FinalW8banwordratio", lkpUDFW8(col("W8banwordratio"))) // .drop("W8banwordratio").cache()

    // 9.FemalFirst       Boolean(Double)
    // 10.Male First      Boolean(Double)
    // 11.ContainBadWord  Boolean(Double)
    // 12ContainBanWord   Boolean(Double)

    // 13. Integer(Double):
    val Mean_W13W13NumberSharewords = Samples.agg(mean("W13NumberSharewords")).head()
    val W13_Mean = Mean_W13W13NumberSharewords.getDouble(0)
    val lkpUDFW13 = udf { (i: Double) => if (i == 0) W13_Mean else i }
    val df32 = df31.withColumn("FinalW13NumberSharewords", lkpUDFW13(col("W13NumberSharewords")))

    // 14. Integer (Double):
    val Mean_W14NumberSharewordswithoutStopwords = Samples.agg(mean("W14NumberSharewordswithoutStopwords")).head()
    val W14_Mean = Mean_W14NumberSharewordswithoutStopwords.getDouble(0)
    val lkpUDFW14 = udf { (i: Double) => if (i == 0) W14_Mean else i }
    val df33 = df32.withColumn("FinalW14NumberSharewordswithoutStopwords", lkpUDFW14(col("W14NumberSharewordswithoutStopwords")))

    // 15. Double (Not ratio):
    val Mean_W15PortionQid = Samples.agg(mean("W15PortionQid")).head()
    val W15_Mean = Mean_W15PortionQid.getDouble(0)
    val lkpUDFW15 = udf { (i: Double) => if (i == 0) W15_Mean else i }
    val df34 = df33.withColumn("FinalW15PortionQid", lkpUDFW15(col("W15PortionQid")))

    // 16. Double(Not Ratio):
    val Mean_W16PortionLnags = Samples.agg(mean("W16PortionLnags")).head()
    val W16_Mean = Mean_W16PortionLnags.getDouble(0)
    val lkpUDFW16 = udf { (i: Double) => if (i == 0) W16_Mean else i }
    val df35 = df34.withColumn("FinalW16PortionLnags", lkpUDFW16(col("W16PortionLnags")))

    // 17.Double(Not ratio):
    val Mean_W17PortionLinks = Samples.agg(mean("W17PortionLinks")).head()
    val W17_Mean = Mean_W17PortionLinks.getDouble(0)
    val lkpUDFW17 = udf { (i: Double) => if (i == 0) W17_Mean else i }
    val df36 = df35.withColumn("FinalW17PortionLinks", lkpUDFW17(col("W17PortionLinks")))

    // ************************************************End Word  Features ****************************************************************************************

    // ************************************************Start Sentences  Features ****************************************************************************************
    // 1. Integer(Double)
    val Mean_S1CommentTailLength = Samples.agg(mean("S1CommentTailLength")).head()
    val S1_Mean = RoundDouble(Mean_S1CommentTailLength.getDouble(0))
    val lkpUDFS1 = udf { (i: Double) => if (i == 0) S1_Mean else i }
    val df37 = df36.withColumn("FinalS1CommentTailLength", lkpUDFS1(col("S1CommentTailLength")))

    // 2. Double  but Not ratio values :
    val Mean_S2SimikaritySitelinkandLabel = Samples.agg(mean("S2SimikaritySitelinkandLabel")).head()
    val S2_Mean = RoundDouble(Mean_S2SimikaritySitelinkandLabel.getDouble(0))
    val lkpUDFS2 = udf { (i: Double) => if (i == 0) S2_Mean else i }
    val df39 = df37.withColumn("FinalS2SimikaritySitelinkandLabel", lkpUDFS2(col("S2SimikaritySitelinkandLabel")))

    // 3. Double  but Not ratio values :
    val Mean_S3SimilarityLabelandSitelink = Samples.agg(mean("S3SimilarityLabelandSitelink")).head()
    val S3_Mean = RoundDouble(Mean_S3SimilarityLabelandSitelink.getDouble(0))
    val lkpUDFS3 = udf { (i: Double) => if (i == 0.0) S3_Mean else i }
    val df40 = df39.withColumn("FinalS3SimilarityLabelandSitelink", lkpUDFS3(col("S3SimilarityLabelandSitelink")))

    // 4.  Double  but Not ratio values :
    val Mean_S4SimilarityCommentComment = Samples.agg(mean("S4SimilarityCommentComment")).head()
    val S4_Mean = RoundDouble(Mean_S4SimilarityCommentComment.getDouble(0))
    val lkpUDFS4 = udf { (i: Double) => if (i == 0.0) S4_Mean else i }
    val df41 = df40.withColumn("FinalS4SimilarityCommentComment", lkpUDFS4(col("S4SimilarityCommentComment")))

    // df41.show()
    // ************************************************End Sentences  Features ****************************************************************************************
    // *********************************************** Start Statement  Features ****************************************************************************************
    // 1. String
    // 2. String
    // 3. String
    // ************************************************End Statement  Features ****************************************************************************************
    // *********************************************** Start User Features ****************************************************************************************

    // 1.Boolean(Double)
    // 2.Boolean(Double)
    // 3.Boolean(Double)
    // 4.Boolean(Double)
    // 5.Boolean(Double)
    // 6.Boolean(Double)
    // 7. (Double) IP No need to fill Missing Data
    // 8. (Double) ID No need to fill Missing Data
    // 9.Boolean(Double)
    // 10.Boolean(Double)

    // *********************************************** End User Features ****************************************************************************************
    // *********************************************** Start Item Features ****************************************************************************************
    // 1. Integer (Double) No need to fill missing values
    // 2. Integer (Double) No need to fill missing values
    // 3. Integer (Double) No need to fill missing values
    // 4. Integer (Double) No need to fill missing values
    // 5. Integer (Double) No need to fill missing values
    // 6. Integer (Double) No need to fill missing values
    // 7. Integer (Double) No need to fill missing values
    // 8. Integer (Double) No need to fill missing values
    // 9. Integer (Double) No need to fill missing values
    // 10. Integer (Double) No need to fill missing values
    // 11. String
    // *********************************************** End Item Features ****************************************************************************************
    // *********************************************** Start Revision Features ****************************************************************************************
    // 1.String
    // 2.String
    // 3.Boolean (Double)
    // 4.Integer(Double)
    // 5.String
    // 6.String
    // 7. Boolean(Double)
    // 8. String
    // 9.String
    // 10. Integer (Double)
    // 11.String
    // 12. integer(Double)
    // 13. Long(Double)
    // 14. integer (Double)
    // 15.String
    // 16.String
    // *********************************************** End Revision Features ****************************************************************************************
    // *********************************************** Meta Data , Truth Data and Frequnces  ****************************************************************************************
    // Meta
    // 1.Revision Session :Integer (Converted to Double)
    // 2. User Country Code
    // 3.User Continent Code
    // 4.User Time Size
    // 5.User Region Code
    // 6.User-city Name
    // 7.User Country Name
    // 8.RevisionTags

    // Truth:
    // 1.Undo

    // Freq :

    // 1.5 features

    // Roll Boolean     :Boolean (Double)
    // Undo             :Boolean (Double)

    // *********************************************** End Revision Features ****************************************************************************************

    // ===========================================================================String Features====================================================================================

    val df42 = df41.withColumn(
      // statement String features:
      "StringFeatures", concat($"SS1Property", lit(";"), $"SS2DataValue", lit(";"), $"SS3ItemValue", lit(";"), $"I11ItemTitle",
        // Revision  String Features:
        lit(";"), $"R1languageRevision",
        lit(";"), $"R2RevisionLanguageLocal",
        lit(";"), $"R5RevisionAction",
        lit(";"), $"R6PrevReviAction",
        lit(";"), $"R8ParRevision",
        lit(";"), $"R9RevisionTime",
        lit(";"), $"R11ContentType",
        lit(";"), $"R15RevisionSubaction",
        lit(";"), $"R16PrevReviSubaction",

        lit(";"), $"USER_COUNTRY_CODE",
        lit(";"), $"USER_CONTINENT_CODE",
        lit(";"), $"USER_TIME_ZONE",
        lit(";"), $"USER_REGION_CODE",
        lit(";"), $"USER_CITY_NAME",
        lit(";"), $"USER_COUNTY_NAME",
        lit(";"), $"REVISION_TAGS"))

    val toArray = udf((record: String) => record.split(";").map(_.toString()))
    val test1 = df42.withColumn("StringFeatures", toArray(col("StringFeatures")))
    //  test1.show()
    //  test1.printSchema()

    val word2Vec = new Word2Vec().setInputCol("StringFeatures").setOutputCol("result").setVectorSize(20).setMinCount(0)
    val model = word2Vec.fit(test1)
    val result = model.transform(test1) // .rdd

    // result.show()

    val Todense = udf((b: Vector) => b.toDense)
    val test_new2 = result.withColumn("result", Todense(col("result")))

    val assembler = new VectorAssembler().setInputCols(Array(
      "result",

      // character
      "FinalC1uppercaseratio", "FinalC2lowercaseratio", "FinalC3alphanumericratio", "FinalC4asciiratio", "FinalC5bracketratio", "FinalC6digitalratio",
      "FinalC7latinratio", "FinalC8whitespaceratio", "FinalC9puncratio", "FinalC10longcharacterseq", "FinalC11arabicratio", "FinalC12bengaliratio",
      "FinalC13brahmiratio", "FinalC14cyrilinratio", "FinalC15hanratio", "Finalc16malysiaratio", "FinalC17tamiratio", "FinalC18telugratio",
      "FinalC19symbolratio", "FinalC20alpharatio", "FinalC21visibleratio", "FinalC22printableratio", "FinalC23blankratio", "FinalC24controlratio", "FinalC25hexaratio",

      // Words
      "FinalW1languagewordratio", "W2Iscontainlanguageword", "FinalW3lowercaseratio", "FinalW4longestword", "W5IscontainURL", "FinalW6badwordratio",
      "FinalW7uppercaseratio", "FinalW8banwordratio", "W9FemalFirstName", "W10MaleFirstName", "W11IscontainBadword", "W12IsContainBanword",
      "FinalW13NumberSharewords", "FinalW14NumberSharewordswithoutStopwords", "FinalW15PortionQid", "FinalW16PortionLnags", "FinalW17PortionLinks",

      // Sentences :
      "FinalS1CommentTailLength", "FinalS2SimikaritySitelinkandLabel", "FinalS3SimilarityLabelandSitelink", "FinalS4SimilarityCommentComment",

      // User :
      "U1IsPrivileged", "U2IsBotUser", "U3IsBotuserWithFlaguser", "U4IsProperty", "U5IsTranslator", "U6IsRegister", "U7IPValue", "U8UserID",
      "U9HasBirthDate", "U10HasDeathDate",

      // Item:

      "I1NumberLabels", "I2NumberDescription", "I3NumberAliases", "I4NumberClaims", "I5NumberSitelinks", "I6NumberStatement",
      "I7NumberReferences", "I8NumberQualifier", "I9NumberQualifierOrder", "I10NumberBadges",

      // Revision:
      "R3IslatainLanguage", "R4JsonLength", "R7RevisionAccountChange", "R10RevisionSize", "R12BytesIncrease",
      "R13TimeSinceLastRevi", "R14CommentLength",

      // Meta , truth , Freq
      // meta :
      "FinalREVISION_SESSION_ID",
      // Truth:
      "FinalUNDO_RESTORE_REVERTED",

      // Freq:
      "FinalNumberofRevisionsUserContributed",
      "FinalNumberofUniqueItemsUseredit", "FinalNumberRevisionItemHas", "FinalNumberUniqUserEditItem", "FinalFreqItem")).setOutputCol("features")
    val Testing_Data = assembler.transform(test_new2)

    // Prepare the data for classification:
    //  NewData.registerTempTable("DB")
    //  val Training_Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB")
    // val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB") // for logistic regrision

    // Data.show()

    //  val TestClassifiers = new Classifiers()
    //
    //  TestClassifiers.RandomForestClassifer(Testing_Data, sqlContext)
    //      // TestClassifiers.DecisionTreeClassifier(Data, sqlContext)
    //      // TestClassifiers.LogisticRegrision(Data, sqlContext)
    //      // TestClassifiers.GradientBoostedTree(Data, sqlContext)
    //      // TestClassifiers.MultilayerPerceptronClassifier(Data, sqlContext)

    Testing_Data

  }

  // ===========================================================================================================================================
  // =================================================Functions Part=============================================================================

  def Ration(va: Double, median: Double): Double = {

    var tem = va
    if (tem == -1.0) {
      tem = median

    } else {

      tem = va
    }
    tem

  }

  // Full All Features String:

  def All_Features(row: Row): String = {

    var temp = ""
    // all characters
    val character_Str_String = Character_Features(row)
    temp = character_Str_String

    // all Words
    val Words_Str_String = Words_Features(row)
    temp = temp + "," + Words_Str_String

    // all sentences
    val Sentences_Str_String = Sentences_Features(row)
    temp = temp + "," + Sentences_Str_String

    // all statements
    val Statement_Str_String = Statement_Features(row)
    temp = temp + "," + Statement_Str_String

    // User Features -  there are 3 Joins in last stage when we have Data Frame
    val User_Str_String = User_Features_Normal(row)
    temp = temp + "," + User_Str_String

    // Item Features -  there are 3 Joins in last stage when we have Data Frame
    val Item_Str_String = Item_Features(row)
    temp = temp + "," + Item_Str_String

    // Revision Features
    val Revision_Str_String = Revision_Features(row)
    temp = temp + "," + Revision_Str_String

    temp.trim()

  }

  // Function for character features
  def Character_Features(row: Row): String = {

    var str_results = ""
    // 1. Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // 2. Revision ID current operation:
    var RevisionID = row(0)
    // 3. row(2) =  represent the Comment:
    var CommentRecord_AsString = row(2).toString()
    // 4. extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
    val CommentObj = new CommentProcessor()
    val Temp_commentTail = CommentObj.Extract_CommentTail(CommentRecord_AsString)

    if (Temp_commentTail != "" && Temp_commentTail != "NA") { // That means the comment is normal comment:
      val CharactersOBJ = new CharactersFeatures()
      var vectorElements = CharactersOBJ.Vector_Characters_Feature(Temp_commentTail)

      val FacilityOBJ = new FacilitiesClass()
      var Str_vector_Values = FacilityOBJ.ArrayToString(vectorElements)
      str_results = Str_vector_Values
      // CharacterFeatures = Vector_AsArrayElements
      // new_Back_Row = Row(vectorElements)

    } else {

      var RatioValues = new Array[Double](25)
      RatioValues(0) = 0
      RatioValues(1) = 0
      RatioValues(2) = 0
      RatioValues(3) = 0
      RatioValues(4) = 0
      RatioValues(5) = 0
      RatioValues(6) = 0
      RatioValues(7) = 0
      RatioValues(8) = 0
      RatioValues(9) = 0
      RatioValues(10) = 0
      RatioValues(11) = 0
      RatioValues(12) = 0
      RatioValues(13) = 0
      RatioValues(14) = 0
      RatioValues(15) = 0
      RatioValues(16) = 0
      RatioValues(17) = 0
      RatioValues(18) = 0
      RatioValues(19) = 0
      RatioValues(20) = 0
      RatioValues(21) = 0
      RatioValues(22) = 0
      RatioValues(23) = 0
      RatioValues(24) = 0

      val FacilityOBJ = new FacilitiesClass()
      var Str_vector_Values = FacilityOBJ.ArrayToString(RatioValues)
      str_results = Str_vector_Values
      // new_Back_Row = Row(vector_Values)

    }
    // CharacterFeatures
    // new_Back_Row
    str_results.trim()
  }

  // Function for Word features
  def Words_Features(row: Row): String = {

    var str_results = ""
    // Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // Revision ID current operation:
    var RevisionID = row(0)
    // row(2) =  represent the Comment:
    var CommentRecord_AsString = row(2).toString()
    // Extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
    val CommentObj = new CommentProcessor()
    val Temp_commentTail = CommentObj.Extract_CommentTail(CommentRecord_AsString)
    var tempQids = 0.0
    var temLinks = 0.0
    var temlangs = 0.0

    if (row(19) != null && row(25) != null) {

      val WordsOBJ_countOf = new WordsFeatures()

      var current_Body_Revision = row(2).toString() + row(8).toString()
      var Prev_Body_Revision = row(19).toString() + row(25).toString()

      // Feature PortionOfQids
      var count_Qids_Prev = WordsOBJ_countOf.GetNumberofQid(Prev_Body_Revision)
      var count_Qids_Current = WordsOBJ_countOf.GetNumberofQid(current_Body_Revision)
      var porortion_Qids = WordsOBJ_countOf.proportion(count_Qids_Prev, count_Qids_Current)
      tempQids = porortion_Qids

      // Feature PortionOfLanguageAdded
      var count_Lang_Prev = WordsOBJ_countOf.GetNumberofLanguageword(Prev_Body_Revision)
      var count_lang_Current = WordsOBJ_countOf.GetNumberofLanguageword(current_Body_Revision)
      var porportion_Lang = WordsOBJ_countOf.proportion(count_Lang_Prev, count_lang_Current)
      temlangs = porportion_Lang

      // Feature PortionOfLinksAddes
      var count_links_Prev = WordsOBJ_countOf.GetNumberofLinks(Prev_Body_Revision)
      var count_links_Current = WordsOBJ_countOf.GetNumberofLinks(current_Body_Revision)
      var porportion_links = WordsOBJ_countOf.proportion(count_links_Prev, count_links_Current)
      temLinks = porportion_links
    } else {

      var porortion_Qids = tempQids // =0.0
      var porportion_Lang = temlangs // =0.0
      var porportion_links = temLinks // =0.0

    }

    if (Temp_commentTail != "" && Temp_commentTail != "NA") { // That means the comment is normal comment:
      val WordsOBJ = new WordsFeatures()

      // 10- Features have Double type
      var ArrayElements = WordsOBJ.Vector_Words_Feature(Temp_commentTail)
      //   new_Back_Row = Row(vectorElements_Doubles)

      var prevComment = row(19)
      if (prevComment != null) {
        var Prev_commentTail = CommentObj.Extract_CommentTail(prevComment.toString())
        if (Prev_commentTail != "") {

          // 11.Feature Current_Previous_CommentTial_NumberSharingWords:

          val NumberSharingWords = WordsOBJ.Current_Previous_CommentTial_NumberSharingWords(Temp_commentTail, Prev_commentTail)
          ArrayElements(12) = NumberSharingWords.toDouble
          // 12.Feature Current_Previous_CommentTial_NumberSharingWords without Stopword:
          val NumberSharingWordsWithoutStopwords = WordsOBJ.Current_Previous_CommentTial_NumberSharingWords_WithoutStopWords(Temp_commentTail, Prev_commentTail)
          ArrayElements(13) = NumberSharingWordsWithoutStopwords.toDouble

        }

      }

      ArrayElements(14) = tempQids
      ArrayElements(15) = temlangs
      ArrayElements(16) = temLinks

      //      val FacilityOBJ = new FacilitiesClass()
      //      var vector_Values = FacilityOBJ.ToVector(ArrayElements)
      //      new_Back_Row = Row(vector_Values)

      val FacilityOBJ = new FacilitiesClass()
      var Str_vector_Values = FacilityOBJ.ArrayToString(ArrayElements)
      str_results = Str_vector_Values

    } else {

      var RatioValues = new Array[Double](17)
      RatioValues(0) = 0
      RatioValues(1) = 0
      RatioValues(2) = 0
      RatioValues(3) = 0
      RatioValues(4) = 0
      RatioValues(5) = 0
      RatioValues(6) = 0
      RatioValues(7) = 0
      RatioValues(8) = 0
      RatioValues(9) = 0
      RatioValues(10) = 0
      RatioValues(11) = 0
      RatioValues(12) = 0
      RatioValues(13) = 0
      RatioValues(14) = tempQids
      RatioValues(15) = temlangs
      RatioValues(16) = temLinks

      //      val FacilityOBJ = new FacilitiesClass()
      //      val vector_Values = FacilityOBJ.ToVector(RatioValues)
      //      new_Back_Row = Row(vector_Values)

      val FacilityOBJ = new FacilitiesClass()
      var Str_vector_Values = FacilityOBJ.ArrayToString(RatioValues)
      str_results = Str_vector_Values

    }
    // new_Back_Row
    // Word_Features
    str_results
  }

  // Function for Sentences features
  def Sentences_Features(row: Row): String = {

    var str_results = ""
    // This will be used to save values in vector
    var DoubleValues = new Array[Double](4)

    // 1. Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // 2. Revision ID current operation:
    var RevisionID = row(0)
    // 3. row(2) =  represent the Full Comment:
    var CommentRecord_AsString = row(2).toString()
    // 4. extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
    val CommentObj = new CommentProcessor()
    val Temp_commentTail = CommentObj.Extract_CommentTail(CommentRecord_AsString)

    if (Temp_commentTail != "" && Temp_commentTail != "NA") {

      // This is CommentTail Feature:-----------------------------------------------------
      val comment_Tail_Length = Temp_commentTail.length()

      // Feature 1 comment tail length
      DoubleValues(0) = comment_Tail_Length

      // Feature 2 similarity  between comment contain Sitelink and label :
      // Check the language in comment that contain sitelinkword: --------------------
      val Sitelink_inCommentObj = new SentencesFeatures()

      if (CommentRecord_AsString.contains("sitelink")) { // start 1 loop
        // 1. First step : get the language from comment
        val languagesitelink_from_Comment = Sitelink_inCommentObj.extract_CommentSiteLink_LanguageType(CommentRecord_AsString).trim()

        // 2. second step: get  the Label tage from json table :
        if (row(9).toString() != "[]") { // start 2 loop
          // if (row(8).toString() != "") {
          val jsonStr = "\"\"\"" + row(9).toString() + "\"\"\"" // row(9) is the label record
          val jsonObj: JSONObject = new JSONObject(row(9).toString()) // we have here the record which represents Label
          var text_lang = languagesitelink_from_Comment.replace("wiki", "").trim()
          var key_lang = "\"" + text_lang + "\""
          if (jsonStr.contains(""""language"""" + ":" + key_lang)) {
            val value_from_Label: String = jsonObj.getJSONObject(text_lang).getString("value")
            val result = StringUtils.getJaroWinklerDistance(Temp_commentTail, value_from_Label)
            DoubleValues(1) = RoundDouble(result)
          } else {
            DoubleValues(1) = 0.0
          }

        } // endd 2 loop
        else {

          DoubleValues(1) = 0.0

        }
      } // end 1 loop
      else {

        DoubleValues(1) = 0.0

      }

      // Feature 3 similarity between comment contain label word and sitelink
      // Check the language in comment that contain Label word:-----------------------
      val Label_inCommentObj = new SentencesFeatures()
      if (CommentRecord_AsString.contains("label")) {
        // 1. First step : get the language from comment
        val languageLabel_from_Comment = Label_inCommentObj.extract_CommentLabel_LanguageType(CommentRecord_AsString).trim()
        // 2. second step: get  the site link  tage from json table :
        if (row(13).toString() != "[]") { // start 2 loop
          val jsonStr = "\"\"\"" + row(13).toString() + "\"\"\"" // row(13) is the sitelink record
          val jsonObj: JSONObject = new JSONObject(row(13).toString())
          var text_lang = languageLabel_from_Comment + "wiki"
          var key_lang = "\"" + text_lang + "\""
          if (jsonStr.contains(""""site"""" + ":" + key_lang)) {
            val value_from_sitelink: String = jsonObj.getJSONObject(text_lang).getString("title")
            val result = StringUtils.getJaroWinklerDistance(Temp_commentTail, value_from_sitelink)
            DoubleValues(2) = RoundDouble(result)

          } else {
            DoubleValues(2) = 0.0

          }

        } else {
          DoubleValues(2) = 0.0

        }

      } else {
        DoubleValues(2) = 0.0

      }

      val prevComment = row(19)
      if (prevComment != null) {
        var Prev_commentTail = CommentObj.Extract_CommentTail(prevComment.toString())
        val Similarityresult = StringUtils.getJaroWinklerDistance(Temp_commentTail, Prev_commentTail)
        DoubleValues(3) = RoundDouble(Similarityresult)
      } else {
        DoubleValues(3) = 0.0

      }

      //      val FacilityOBJ = new FacilitiesClass()
      //      val vector_Values = FacilityOBJ.ToVector(DoubleValues)
      //      new_Back_Row = Row(vector_Values)

      val FacilityOBJ = new FacilitiesClass()
      var Str_vector_Values = FacilityOBJ.ArrayToString(DoubleValues)
      str_results = Str_vector_Values

    } else {

      DoubleValues(0) = 0.0
      DoubleValues(1) = 0.0
      DoubleValues(2) = 0.0
      DoubleValues(3) = 0.0

      //      val FacilityOBJ = new FacilitiesClass()
      //      val vector_Values = FacilityOBJ.ToVector(DoubleValues)
      //      new_Back_Row = Row(vector_Values)

      val FacilityOBJ = new FacilitiesClass()
      var Str_vector_Values = FacilityOBJ.ArrayToString(DoubleValues)
      str_results = Str_vector_Values

    }

    // new_Back_Row
    str_results

  }

  // statement Features :
  def Statement_Features(row: Row): String = {
    var full_Str_Result = ""
    // 1. row(2) =  represent the Comment:
    var fullcomment = row(2).toString()
    val StatementOBJ = new StatementFeatures()

    val property = StatementOBJ.getProperty(fullcomment)
    val DataValue = StatementOBJ.getDataValue(fullcomment)
    val Itemvalue = StatementOBJ.getItemValue(fullcomment)

    // Feature 1 - Property
    if (property != null) {
      full_Str_Result = property.trim()
    } else {
      full_Str_Result = "NA"

    }
    // Feature 2 - DataValue
    if (DataValue != null) {

      full_Str_Result = full_Str_Result.trim() + "," + DataValue.trim()

    } else {
      full_Str_Result = full_Str_Result + "," + "NA"

    }
    // Feature 3 - Itemvalue
    if (Itemvalue != null) {

      full_Str_Result = full_Str_Result.trim() + "," + Itemvalue.trim()

    } else {
      full_Str_Result = full_Str_Result + "," + "NA"
    }

    full_Str_Result.trim()
  }
  // User Normal Features :
  def User_Features_Normal(row: Row): String = {

    var str_results = ""
    var DoubleValues = new Array[Double](10) // you should change the index when add more element feature

    // Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // row(7) =  represent the Contributor name:
    var full_comment = row(2).toString()
    var contributor_Name = row(7).toString()
    var contributor_ID = row(6).toString()
    var contributor_IP = row(5).toString()
    if (contributor_Name != "NA") {

      val useFeatureOBJ = new UserFeatures()

      // 1. Is privileged :  There are 5 cases : if one of these cases is true that mean it is privileged else it is not privileged user
      var flag_case1 = useFeatureOBJ.CheckName_isGlobalSysopUser(contributor_Name)
      var flag_case2 = useFeatureOBJ.CheckName_isGlobalRollBackerUser(contributor_Name)
      var flag_case3 = useFeatureOBJ.CheckName_isGlobalStewarUser(contributor_Name)
      var flag_case4 = useFeatureOBJ.CheckName_isAdmin(contributor_Name)
      var flag_case5 = useFeatureOBJ.CheckName_isRollBackerUser(contributor_Name)

      if (flag_case1 == true || flag_case2 == true || flag_case3 == true || flag_case4 == true || flag_case5 == true) {

        DoubleValues(0) = 1.0

      } else {

        DoubleValues(0) = 0.0
      }

      // 2. is BotUser : There are 3 cases  :
      var flag_case1_1 = useFeatureOBJ.CheckName_isLocalBotUser(contributor_Name)
      var flag_case2_2 = useFeatureOBJ.CheckName_isGlobalbotUser(contributor_Name)
      var flag_case3_3 = useFeatureOBJ.CheckName_isExtensionBotUser(contributor_Name)

      if (flag_case1_1 == true || flag_case2_2 == true || flag_case3_3 == true) {

        DoubleValues(1) = 1.0

      } else {

        DoubleValues(1) = 0.0
      }

      // 3. is Bot User without BotflagUser : There is 1 case  :
      var flag_BUWBF = useFeatureOBJ.CheckName_isBotUserWithoutBotFlagUser(contributor_Name)

      if (flag_BUWBF == true) {
        DoubleValues(2) = 1.0

      } else {
        DoubleValues(2) = 0.0

      }

      // 4. is Property  creator :
      var flagCreator = useFeatureOBJ.CheckName_isPropertyCreator(contributor_Name)

      if (flagCreator == true) {
        DoubleValues(3) = 1.0

      } else {
        DoubleValues(3) = 0.0

      }

      // 5. is translator :
      var flagTranslator = useFeatureOBJ.CheckName_isTranslator(contributor_Name)
      if (flagTranslator == true) {
        DoubleValues(4) = 1.0
      } else {
        DoubleValues(4) = 0.0
      }

      // 6. is register user:
      var flagRegistered = useFeatureOBJ.IsRegisteroUser(contributor_Name)
      if (flagRegistered == true) {
        DoubleValues(5) = 1.0
      } else {
        DoubleValues(5) = 0.0
      }

    } else {

      DoubleValues(0) = 0.0
      DoubleValues(1) = 0.0
      DoubleValues(2) = 0.0
      DoubleValues(3) = 0.0
      DoubleValues(4) = 0.0
      DoubleValues(5) = 0.0

    }

    // 7. IP as a long value
    if (contributor_IP != "0") {
      DoubleValues(6) = contributor_IP.toDouble
    } else {
      DoubleValues(6) = 0.0
    }
    // 8. ID

    if (contributor_ID != "0") {
      DoubleValues(7) = contributor_ID.toDouble
    } else {
      DoubleValues(7) = 0.0
    }

    // 9- 10  BitrthDate  - DeatDate:

    var DateObj = new UserFeatures()
    var BirthDate = DateObj.IsBirthDate(full_comment)
    var DeathDate = DateObj.IsDeathDate(full_comment)

    if (BirthDate == true) {
      DoubleValues(8) = 1.0

    } else {
      DoubleValues(8) = 0.0
    }
    if (DeathDate == true) {
      DoubleValues(9) = 1.0
    } else {
      DoubleValues(9) = 0.0

    }

    //    val FacilityOBJ = new FacilitiesClass()
    //    val vector_Values = FacilityOBJ.ToVector(DoubleValues)
    //    new_Back_Row = Row(vector_Values)
    //    new_Back_Row

    val FacilityOBJ = new FacilitiesClass()
    var Str_vector_Values = FacilityOBJ.ArrayToString(DoubleValues)
    str_results = Str_vector_Values

    str_results.trim()

  }

  def Item_Features(row: Row): String = {

    var str_results = ""
    var DoubleValues = new Array[Double](11)
    // Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    var ItemOBJ = new ItemFeatures()

    // 1. Feature depending on Label:
    var NumberOfLabel = 0.0
    var Label_String = row(9).toString()
    if (Label_String != "[]") {
      NumberOfLabel = ItemOBJ.Get_NumberOfLabels(Label_String)
      DoubleValues(0) = NumberOfLabel
    } else {
      NumberOfLabel = 0.0
      DoubleValues(0) = NumberOfLabel
    }
    // 2. Feature depending on Description:
    var Description_String = row(10).toString()
    var NumberOfDescription = 0.0
    if (Description_String != "[]") {
      NumberOfDescription = ItemOBJ.Get_NumberOfDescription(Description_String)
      DoubleValues(1) = NumberOfDescription

    } else {
      NumberOfDescription = 0.0
      DoubleValues(1) = NumberOfDescription

    }
    // 3. Feature depending on Aliases:
    var Aliases_String = row(11).toString()
    var NumberOfAliases = 0.0
    if (Aliases_String != "[]") {
      NumberOfAliases = ItemOBJ.Get_NumberOfAliases(Aliases_String)
      DoubleValues(2) = NumberOfAliases

    } else {
      NumberOfAliases = 0.0
      DoubleValues(2) = NumberOfAliases

    }
    // 4. Feature depending on Claims :
    var Claims_String = row(12).toString()
    var NumberOfClaims = 0.0
    if (Claims_String != "[]") {
      NumberOfClaims = ItemOBJ.Get_NumberOfClaim(Claims_String)
      DoubleValues(3) = NumberOfClaims

    } else {
      NumberOfClaims = 0.0
      DoubleValues(3) = NumberOfClaims

    }
    // 5. Feature depending on SiteLink
    var SiteLink_String = row(13).toString()
    var NumberOfSitelink = 0.0
    if (SiteLink_String != "[]") {
      NumberOfSitelink = ItemOBJ.Get_NumberOfSiteLinks(SiteLink_String)
      DoubleValues(4) = NumberOfSitelink

    } else {
      NumberOfSitelink = 0.0
      DoubleValues(4) = NumberOfSitelink

    }

    // 6. Feature depending on Claims - statements :
    var statement_String = row(12).toString() // from claim
    var NumberOfstatement = 0.0
    if (statement_String != "[]") {
      NumberOfstatement = ItemOBJ.Get_NumberOfstatements(statement_String)
      DoubleValues(5) = NumberOfstatement

    } else {
      NumberOfstatement = 0.0
      DoubleValues(5) = NumberOfstatement

    }

    // 7. Feature depending on Claims - References  :
    var References_String = row(12).toString() // from claim
    var NumberOfReferences = 0.0
    if (References_String != "[]") {
      NumberOfReferences = ItemOBJ.Get_NumberOfReferences(References_String)
      DoubleValues(6) = NumberOfReferences

    } else {
      NumberOfReferences = 0.0
      DoubleValues(6) = NumberOfReferences

    }
    // 8. Feature depending on claim
    var Qualifier_String = row(12).toString() // from claim
    var NumberOfQualifier = 0.0
    if (Qualifier_String != "[]") {
      NumberOfQualifier = ItemOBJ.Get_NumberOfQualifier(Qualifier_String)
      DoubleValues(7) = NumberOfQualifier

    } else {
      NumberOfQualifier = 0.0
      DoubleValues(7) = NumberOfQualifier

    }

    // 9. Features depending on  claim
    var Qualifier_String_order = row(12).toString() // from claim
    var NumberOfQualifier_order = 0.0
    if (Qualifier_String_order != "[]") {
      NumberOfQualifier_order = ItemOBJ.Get_NumberOfQualifier_Order(Qualifier_String_order)
      DoubleValues(8) = NumberOfQualifier_order

    } else {
      NumberOfQualifier_order = 0.0
      DoubleValues(8) = NumberOfQualifier_order

    }

    // 10. Feature depending on  Site link
    var BadgesString = row(13).toString() // from claim
    var NumberOfBadges = 0.0
    if (BadgesString != "[]") {
      NumberOfBadges = ItemOBJ.Get_NumberOfBadges(BadgesString)
      DoubleValues(9) = NumberOfBadges

    } else {
      NumberOfBadges = 0.0
      DoubleValues(9) = NumberOfBadges

    }

    // 11. Item Title (instead of Item  ID)
    var Item_Id_Title = row(1).toString().replace("Q", "")
    var Item = Item_Id_Title.trim().toDouble
    DoubleValues(10) = Item

    //    val FacilityOBJ = new FacilitiesClass()
    //    val vector_Values = FacilityOBJ.ToVector(DoubleValues)
    //    new_Back_Row = Row(vector_Values)
    //    new_Back_Row
    //

    val FacilityOBJ = new FacilitiesClass()
    var Str_vector_Values = FacilityOBJ.ArrayToString(DoubleValues)
    str_results = Str_vector_Values

    str_results.trim()

  }

  def Revision_Features(row: Row): String = {

    // var DoubleValues = new Array[Double](6)
    var full_Str_Result = ""
    // 1. Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // 2. Revision ID current operation:
    var RevisionID = row(0)
    // 3. row(2) =  represent the Comment:
    var fullcomment = row(2).toString()
    // DoubleValues(0) = length

    // 1. Revision Language :---------------------------------------------------------------------------------

    var comment_for_Language = row(2).toString()
    val CommentLanguageOBJ = new RevisionFeatures()
    val language = CommentLanguageOBJ.Extract_Revision_Language(fullcomment)
    if (language != null && language != "NA") {
      full_Str_Result = language.trim()
    } else {
      full_Str_Result = "NA".trim()

    }
    // 2. Revision Language  local:----------------------------------------------------------------------------
    if (language != "NA") {
      if (language.contains("-")) { // E.g.Revision ID = 10850 sample1
        var LocalLangArray: Array[String] = language.split("-", 2)
        var location = LocalLangArray(1)
        full_Str_Result = full_Str_Result + "," + location.trim()
      } else {

        full_Str_Result = full_Str_Result + "," + "NA"
      }

    } else {
      full_Str_Result = full_Str_Result + "," + "NA"
    }

    // 3. Is it Latin Language or Not:-------------------------------------------------------------------------
    val revisionFeatureOBJ = new RevisionFeatures()
    val flagLatin = revisionFeatureOBJ.Check_ContainLanguageLatin_NonLatin(language)

    if (flagLatin == true) {

      full_Str_Result = full_Str_Result + "," + "1.0"

    } else {

      full_Str_Result = full_Str_Result + "," + "0.0"
    }

    // 4. Json Length : be care full to RDD where the json before parsed--------------------------------------
    // var Jason_Text = row(8).toString()

    // replacing_with_Quoto for cleaning the Json tag from extr tags such as <SHA>...
    var Jason_Text = replacing_with_Quoto(row(0).toString(), row(8).toString())
    var Json_Length = Jason_Text.length()

    full_Str_Result = full_Str_Result + "," + Json_Length.toString()

    // 5. Revision Action -:-----------------------------------------------------------------------
    val CommentProcessOBJ1 = new CommentProcessor()
    val actions1 = CommentProcessOBJ1.Extract_Actions_FromComments(fullcomment)

    var ActionsArray1: Array[String] = actions1.split("_", 2)
    var action1 = ActionsArray1(0).toString()
    // var SubAction = ActionsArray(1)
    full_Str_Result = full_Str_Result + "," + action1.trim()
    // full_Str_Result = full_Str_Result + "," + SubAction.trim()

    // 6.  Revision Prev-Action :-------------------------------------------------------------------------------
    if (row(19) != null) {
      var Prev_fullcomment1 = row(19).toString()
      val Prev_CommentProcessOBJ1 = new CommentProcessor()
      val Prev_actions1 = Prev_CommentProcessOBJ1.Extract_Actions_FromComments(fullcomment)
      var Prev_ActionsArray1: Array[String] = Prev_actions1.split("_", 2)
      var Prev_action1 = ActionsArray1(0).trim()
      //      var Prev_SubAction = ActionsArray(1).trim()
      full_Str_Result = full_Str_Result + "," + Prev_action1.trim()
      // full_Str_Result = full_Str_Result + "," + Prev_SubAction.trim()

      // println(row(16).toString())
    } else {

      full_Str_Result = full_Str_Result + "," + "NA"
      // full_Str_Result = full_Str_Result + "," + "NA"
    }

    // 7. Revision Account user type change :----------------------------------------------------------------------------
    var changeFlag = false
    if (row(23) != null) {
      var Prev_Contributor_ID = row(23).toString()
      var Current_Contributor_ID = row(6).toString()

      if (Prev_Contributor_ID != Current_Contributor_ID) {

        changeFlag = true
        full_Str_Result = full_Str_Result + "," + "1.0"

      } else {
        full_Str_Result = full_Str_Result + "," + "0.0"
      }

    } else {
      full_Str_Result = full_Str_Result + "," + "0.0"

    }

    // 8.Revision Parent :-----------------------------------------------------------------------------------------------------
    var RevisionParent = row(3).toString()
    full_Str_Result = full_Str_Result + "," + RevisionParent.toString().trim()

    // 9. Revision Time Stamp------------------------------------------------------------------------------------------------
    var RevisionTimeZone = row(4).toString()
    full_Str_Result = full_Str_Result + "," + RevisionTimeZone

    // 10. Revision Size:------------------------------------------------------------------------------------------------

    var RevisionBody = row(0).toString() + row(2).toString() + row(3).toString() + row(4).toString() +
      row(8).toString() + row(14).toString() + row(15).toString() + row(16).toString()
    if (row(5).toString() != "0") {

      RevisionBody = RevisionBody + row(5).toString()
      full_Str_Result = full_Str_Result + "," + RevisionBody.length().toString()

    } else {
      RevisionBody = RevisionBody + row(6).toString() + row(7).toString()
      full_Str_Result = full_Str_Result + "," + RevisionBody.length().toString()

    }

    // 11. ContentType: take Action1 as input : --------------------------------------------------------------

    val CommentProcessOBJ_New = new CommentProcessor()
    val actions_New = CommentProcessOBJ_New.Extract_Actions_FromComments(fullcomment)

    var ActionsArrayNew: Array[String] = actions_New.split("_", 2)
    var actionNew = ActionsArrayNew(0)
    var CTOBJ = new RevisionFeatures()
    var contentType = CTOBJ.getContentType(actionNew.trim())
    full_Str_Result = full_Str_Result + "," + contentType.trim()

    // 12. Bytes Increase (  subtract Bytes current revision with previous revision ):--------------------------------------------------------------

    var CurrentRevision = ""
    var PreviRevision = ""

    // For Current Revision
    CurrentRevision = row(0).toString() + row(2).toString() + row(3).toString() + row(4).toString() +
      row(8).toString() + row(14).toString() + row(15).toString() + row(16).toString()
    if (row(5).toString() != "0") {
      CurrentRevision = CurrentRevision.trim() + row(5).toString()
    } else {
      CurrentRevision = CurrentRevision.trim() + row(6).toString() + row(7).toString()
    }

    // For Previous Revision :
    if (row(17) != null && row(19) != null && row(20) != null && row(21) != null && row(25) != null && row(31) != null && row(32) != null && row(33) != null) {
      if (row(22) != null && row(22).toString() != "0") {
        var PreviRevision = row(17).toString() + row(19).toString() + row(20).toString() + row(21).toString() +
          row(25).toString() + row(31).toString() + row(32).toString() + row(33).toString() + row(22).toString()

      } else if (row(23) != null && row(24) != null) {
        var PreviRevision = row(17).toString() + row(19).toString() + row(20).toString() + row(21).toString() +
          row(25).toString() + row(31).toString() + row(32).toString() + row(33).toString() + row(23).toString() + row(24).toString()
      } else {

        PreviRevision = null
      }

    }

    if (PreviRevision != null) {

      var Bytes1 = CurrentRevision.length()
      var Bytes2 = PreviRevision.length()
      var BytesResults = Bytes1 - Bytes2

      full_Str_Result = full_Str_Result + "," + BytesResults.toString()

    } else {

      full_Str_Result = full_Str_Result + "," + "0"

    }

    // 13. Time since last Revision: ----------------------------------------------------------------------

    if (row(21) != null) {

      var CurrentTime = DateToLong(row(4).toString())

      var PreviousTime = DateToLong(row(21).toString())

      var FinalTime = CurrentTime - PreviousTime

      full_Str_Result = full_Str_Result + "," + FinalTime.toString()

    } else {

      full_Str_Result = full_Str_Result + "," + "0"

    }

    // 14. Comment Length:---------------------------------------
    var lengthcomment = fullcomment.length().toString()
    full_Str_Result = full_Str_Result + "," + lengthcomment

    // 15. Revision SubAction:
    val CommentProcessOBJ2 = new CommentProcessor()
    val actions2 = CommentProcessOBJ2.Extract_Actions_FromComments(fullcomment)

    var ActionsArray2: Array[String] = actions2.split("_", 2)
    var SubAction2 = ActionsArray2(1)
    full_Str_Result = full_Str_Result + "," + SubAction2.trim()

    // 16.Prev_revision SubAction:
    if (row(19) != null) {
      var Prev_fullcomment2 = row(19).toString()
      val Prev_CommentProcessOBJ2 = new CommentProcessor()
      val Prev_actions2 = Prev_CommentProcessOBJ2.Extract_Actions_FromComments(fullcomment)
      var Prev_ActionsArray2: Array[String] = Prev_actions2.split("_", 2)
      var Prev_SubAction2 = ActionsArray2(1).trim()
      full_Str_Result = full_Str_Result + "," + Prev_SubAction2.trim()

    } else {

      full_Str_Result = full_Str_Result + "," + "NA"
    }

    //    //15. Item of Revision :----------------------------------------
    //
    //    var ItemOfRevision = row(1).toString()
    //    full_Str_Result = full_Str_Result + "," + ItemOfRevision

    full_Str_Result

  }

  //  ========================

  def RoundDouble(va: Double): Double = {

    val rounded: Double = Math.round(va * 10000).toDouble / 1000
    rounded

  }
  def DateToLong(strDate: String): Long = {

    var str = strDate.replace("T", " ")
    str = str.replace("Z", "").trim()
    val ts1: java.sql.Timestamp = java.sql.Timestamp.valueOf(str)
    val tsTime1: Long = ts1.getTime()

    tsTime1
  }

  def replacing_with_Quoto(keyValue: String, str: String): String = {
    var Full_Key = "\"" + "key" + "\"" + ":" + "\"" + keyValue + "\"" + ","
    var container = str
    // val x= '"'+'"'+'"'+str+'"'+'"'+'"'
    val before = "</format>" + "<text xml:space=" + """"preserve"""" + ">"
    val after1 = "</text><sha1>"
    val after2 = "<sha1>"
    var quot = "\"\"\""
    var tem = ""
    val flag1 = container.contains(before)

    if (flag1 == true) {
      //    var dd= container.replace(before, quot)
      var dd = container.replace(before, "")

      tem = dd

    }
    //
    val flag2 = tem.contains(after1)

    if (flag2 == true) {
      //    var dd= tem.replace(after1, quot)
      var dd = tem.replace(after1, "")

      tem = dd

    }

    val flag3 = tem.contains(after2)
    if (flag3 == true) {
      //    var dd= tem.replace(after2, quot)
      var dd = tem.replace(after2, "")

      tem = dd

    }

    var sb: StringBuffer = new StringBuffer(tem)
    var result = sb.insert(1, Full_Key).toString()

    result

  }

}
