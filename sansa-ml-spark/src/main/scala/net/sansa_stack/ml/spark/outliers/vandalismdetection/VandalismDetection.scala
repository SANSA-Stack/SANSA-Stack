package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{ RangePartitioner, SparkContext }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ VectorAssembler, Word2Vec, Word2VecModel }
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ concat, lit }
import org.json.JSONObject

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._
import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.extraction._
import net.sansa_stack.ml.spark.outliers.vandalismdetection.parser._

class VandalismDetection extends Serializable {

  // Training XML and Vandalism Detection
  def run(triples: RDD[String], metaFile: String, truthFile: String, sampleFraction: Double, spark: SparkSession): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    // Streaming records:
    val jobConf = new JobConf()

    // ======= Tags part : // Contributor IP here is in Decimal format not IP format and It is converted in ParseNormalXml stage
    val TagsRDD_x = triples.filter(line => line.nonEmpty).map(line => line.split("<1VandalismDetector2>"))
    val TagsRDD = TagsRDD_x.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10))).cache()

    val DF_Tags = TagsRDD.toDF("Rid", "comment", "pid", "time", "contributorIP", "contributorID", "contributorName", "JsonText", "model", "format", "sha").dropDuplicates

    // ======= Json part :
    // Json RDD : Each record has its Revision iD:
    val JsonRDD_x = triples.map(_.split("<1VandalismDetector2>")).filter(v => v(7) != "NA").filter(v => !v(7).contains("\"" + "entity" + "\"" + ":")) // .cache()
    val xx = JsonRDD_x.map(v => (v(0).toInt, v(7)))

    val part1 = new RangePartitioner(4, xx)
    val partitioned1 = xx.partitionBy(part1).persist()
    val JsonRDD = partitioned1.map { case (x, y) => (replacingWithQuoto(x.toString(), y)) }

    // Data set
    val Ds_Json = spark.sqlContext.jsonRDD(JsonRDD).select("key", "id", "labels", "descriptions", "aliases", "claims", "sitelinks").dropDuplicates()

    // ======== Join Json part with Tag Part:============================
    // Joining to have full data Tags + Json:

    val DF_First_DF_Result_Join_Tags_and_Json_x = DF_Tags.as("T1").join(Ds_Json.as("T2"), $"T1.Rid" === $"T2.key", "leftouter") //
    DF_First_DF_Result_Join_Tags_and_Json_x.createOrReplaceTempView("TAGSANDJSON")
    val DF_First_DF_Result_Join_Tags_and_Json = spark.sql("select Rid, id as itemid, " +
      "comment, pid, time, contributorIP, contributorID, contributorName, JsonText, " +
      "labels, descriptions, aliases, claims, sitelinks,model, format, sha from TAGSANDJSON")
    // DF_First_DF_Result_Join_Tags_and_Json.show()

    // Duplication the data for joining based on parent ID :
    val colNames = Seq("Rid2", "itemid2", "comment2", "pid2", "time2", "contributorIP2",
      "contributorID2", "contributorName2", "JsonText2", "labels2", "descriptions2",
      "aliases2", "claims2", "sitelinks2", "model2", "format2", "sha2")
    val DF_Second = DF_First_DF_Result_Join_Tags_and_Json.toDF(colNames: _*)

    // Joining based on Parent Id to get the previous cases: ParentID
    val DF_Joined = DF_First_DF_Result_Join_Tags_and_Json.as("df1").join(DF_Second.as("df2"), $"df1.pid" === $"df2.Rid2", "leftouter")

    val RDD_After_JoinDF = DF_Joined.rdd
    val x = RDD_After_JoinDF.map(row => (row(0).toString().toInt, row)) // .sortByKey(true)
    val part = new RangePartitioner(4, x)
    val partitioned = x.partitionBy(part).persist() // persist is important for this case and obligatory

    // ==============================================All Features Based on Categories of Features Data Type :=============
    val Result_all_Features = partitioned.map { case (x, y) => (allFeatures(y).toString()) } // we convert the Pair RDD to String one LineRDD to be able to make DF based on ","
    // Result_all_Features.foreach(println)

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

        // Sentences Features:
        /* 43 */ StructField("S1CommentTailLength", DoubleType, false) :: /* 44 */ StructField("S2SimikaritySitelinkandLabel", DoubleType, false) ::
        /* 45 */ StructField("S3SimilarityLabelandSitelink", DoubleType, false) ::
        /* 46 */ StructField("S4SimilarityCommentComment", DoubleType, false) ::

        // Statements Features :
        /* 47 */ StructField("SS1Property", StringType, false) :: /* 48 */ StructField("SS2DataValue", StringType, false) :: /* 49 */ StructField("SS3ItemValue", StringType, false) ::

        // User Features :

        /* 50 Boolean */ StructField("U1IsPrivileged", StringType, false) :: /* 51 Boolean */ StructField("U2IsBotUser", StringType, false) :: /* 52 Boolean */ StructField("U3IsBotuserWithFlaguser", StringType, false) ::
        /* 53 Boolean */ StructField("U4IsProperty", StringType, false) :: /* 54 Boolean */ StructField("U5IsTranslator", StringType, false) ::
        /* 55 Boolean */ StructField("U6IsRegister", StringType, false) :: /* 56 */ StructField("U7IPValue", StringType, false) ::
        /* 57 */ StructField("U8UserID", StringType, false) :: /* 58 */ StructField("U9HasBirthDate", StringType, false) ::
        /* 59 */ StructField("U10HasDeathDate", StringType, false) ::

        // Items Features :
        /* 60 */ StructField("I1NumberLabels", StringType, false) :: /* 61 */ StructField("I2NumberDescription", StringType, false) :: /* 62 */ StructField("I3NumberAliases", StringType, false) ::
        /* 63 */ StructField("I4NumberClaims", StringType, false) ::
        /* 64 */ StructField("I5NumberSitelinks", StringType, false) ::
        /* 65 */ StructField("I6NumberStatement", StringType, false) :: /* 66 */ StructField("I7NumberReferences", StringType, false) ::
        /* 67 */ StructField("I8NumberQualifier", StringType, false) ::
        /* 68 */ StructField("I9NumberQualifierOrder", StringType, false) ::
        /* 69 */ StructField("I10NumberBadges", StringType, false) :: /* 70 */ StructField("I11ItemTitle", StringType, false) ::

        // Revision Features:
        /* 71StructField("R1languageRevision", StringType, false) :: */ /* 72 */ StructField("R2RevisionLanguageLocal", StringType, false) ::
        /* 73 */ StructField("R3IslatainLanguage", StringType, false) ::
        /* 74 */ StructField("R4JsonLength", StringType, false) :: /* 75 */ StructField("R5RevisionAction", StringType, false) ::
        /* 76 */ StructField("R6PrevReviAction", StringType, false) ::
        /* 77 */ StructField("R7RevisionAccountChange", StringType, false) :: /* 78 */ StructField("R8ParRevision", StringType, false) ::
        /* 79 */ StructField("R9RevisionTime", StringType, false) ::
        /* 80 */ StructField("R10RevisionSize", StringType, false) :: /* 81 */ StructField("R11ContentType", StringType, false) ::
        /* 82 */ StructField("R12BytesIncrease", StringType, false) ::
        /* 83 */ StructField("R13TimeSinceLastRevi", StringType, false) ::

        /* 84 */ StructField("R14CommentLength", StringType, false) ::

        /* 85 */ StructField("HasHashTable", StringType, false) ::
        /* 86 */ StructField("IsspecialUser", StringType, false) ::
        /* 87 */ StructField("IsProperty", StringType, false) ::
        /* 88 */ StructField("IsPropertyQuestion", StringType, false) ::

        Nil)

    val rowRDD = Result_all_Features.map(line => line.split("<1VandalismDetector2>")).map(e ⇒ Row(

      e(0).toInt // character feature column
      , e(1).toDouble, e(2).toDouble, e(3).toDouble, e(4).toDouble, e(5).toDouble, e(6).toDouble, e(7).toDouble, e(8).toDouble, e(9).toDouble, roundDouble(e(10).toDouble),
      e(11).toDouble, e(12).toDouble, e(13).toDouble, e(14).toDouble, e(15).toDouble, e(16).toDouble, e(17).toDouble, e(18).toDouble, e(19).toDouble, e(20).toDouble, e(21).toDouble,
      e(22).toDouble, e(23).toDouble, e(24).toDouble, e(25).toDouble // Word Feature column
      , e(26).toDouble, e(27).toDouble, e(28).toDouble, e(29).toDouble.toInt, e(30).toDouble, e(31).toDouble,
      e(32).toDouble, e(33).toDouble, e(34).toDouble, e(35).toDouble, e(36).toDouble, e(37).toDouble,

      roundDouble(e(38).toDouble), roundDouble(e(39).toDouble), e(40).toDouble, e(41).toDouble, e(42).toDouble // Sentences Features column:
      , roundDouble(e(43).toDouble), e(44).toDouble, e(45).toDouble, e(46).toDouble // Statement Features Column:
      , e(47), e(48), e(49) // User Features Column:
      , e(50), e(51), e(52), e(53), e(54), e(55), e(56), e(57), e(58), e(59) // Item Features column:
      , e(60), e(61), e(62), e(63), e(64), e(65), e(66), e(67), e(68), e(69), "Q" + e(70) // Revision Features Column:
      , e(71), e(72), e(73), e(74), e(75), e(76), e(77), e(78), e(79), e(80), e(81), e(82), e(83), e(84), e(85), e(86), e(87)))

    // This is Main DataFrame:Includes all vlaues from features function:
    val BeforeJoin_All_Features = spark.createDataFrame(rowRDD, schema)

    // *Geografical information Feature from Meta File
    // REVISION_ID|REVISION_SESSION_ID|USER_COUNTRY_CODE|USER_CONTINENT_CODE|USER_TIME_ZONE|USER_REGION_CODE|USER_CITY_NAME|USER_COUNTY_NAME|REVISION_TAGS
    val df_GeoInf = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(metaFile).select("REVISION_ID", "REVISION_SESSION_ID", "USER_COUNTRY_CODE", "USER_CONTINENT_CODE", "USER_TIME_ZONE", "USER_REGION_CODE", "USER_CITY_NAME", "USER_COUNTY_NAME", "REVISION_TAGS")

    val AfterJoinGeoInfo_All_Features = BeforeJoin_All_Features.as("T1").join(df_GeoInf.as("T2"), $"T1.Rid" === $"T2.REVISION_ID", "leftouter").drop("REVISION_ID") // .cache()

    val df_Truth = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(truthFile).select("REVISION_ID", "ROLLBACK_REVERTED", "UNDO_RESTORE_REVERTED")

    val AfterJoinTruthInfo_All_Features = AfterJoinGeoInfo_All_Features.as("T1").join(df_Truth.as("T2"), $"T1.Rid" === $"T2.REVISION_ID", "leftouter").drop("REVISION_ID") // .cache()

    DF_Tags.createOrReplaceTempView("TagesTable")
    DF_Joined.createOrReplaceTempView("JoindedTabel")

    // a.User Frequency:
    // number of revisions a user has contributed
    val ContributorFreq_for_Each_Revision_DF = spark.sql("select contributorID as CIDUSER1, count(Rid) as NumberofRevisionsUserContributed from TagesTable where contributorID !='0' group by contributorID ")

    // Join1 for add The first User Feature : number of revisions a user has contributed
    val AfterJoinUser1_All_Features = AfterJoinTruthInfo_All_Features.as("T1").join(ContributorFreq_for_Each_Revision_DF.as("T2"), $"T1.U8UserID" === $"T2.CIDUSER1", "leftouter").drop("CIDUSER1")

    // b.Cumulated : Number of a unique Item a user has contributed.
    val CumulatedNumberof_uniqueItemsForUser_DF = spark.sql("select contributorID as CIDUSER2,  COUNT(DISTINCT itemid) as NumberofUniqueItemsUseredit from JoindedTabel where contributorID !='0' group by contributorID")

    // Join2 for add The second  User Feature
    val AfterJoinUser2_All_Features = AfterJoinUser1_All_Features.as("T1").join(CumulatedNumberof_uniqueItemsForUser_DF.as("T2"), $"T1.U8UserID" === $"T2.CIDUSER2", "leftouter").drop("CIDUSER2")

    // c.Item Frequency:
    // number of revisions an Item has
    val ItemFrequ_DF = spark.sql("select itemid, count(Rid) as NumberRevisionItemHas from JoindedTabel  group by itemid")

    // Join3 for add The First  Item Feature :number of revisions an Item has
    val AfterJoinItem3_All_Features = AfterJoinUser2_All_Features.as("T1").join(ItemFrequ_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")

    // d. Cumulate number of unique users have edited the Item : Did not consider the users IP. Contributor is an IP or Name. we consider name
    val CumulatedNumberof_UniqueUserForItem_DF = spark.sql("select itemid,  COUNT(DISTINCT contributorID) as NumberUniqUserEditItem from JoindedTabel  where contributorID !='0' group by itemid")

    // Join4 for add The Second  Item Feature
    val AfterJoinItem4_All_Features = AfterJoinItem3_All_Features.as("T1").join(CumulatedNumberof_UniqueUserForItem_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")

    // e. freq each Item :
    val Fre_Item_DF = spark.sql("select itemid,  COUNT(itemid) as FreqItem from JoindedTabel  group by itemid")

    // Join5 for add The Third  Item Feature
    val Final_All_Features = AfterJoinItem4_All_Features.as("T1").join(Fre_Item_DF.as("T2"), $"T1.I11ItemTitle" === $"T2.itemid", "leftouter").drop("itemid")

    // *****************************************************************************************************************************************
    // Pre- process Data Step1: ============================================================================================================================================================

    // For String Column, We fill the Null values by "NA"

    val NullToNA = udf { (ValString: String) => if (ValString == null || ValString == "") "NA" else ValString }

    var Fill_Missing_Final_All_Features = Final_All_Features.withColumn("FinalUSER_COUNTRY_CODE", NullToNA(col("USER_COUNTRY_CODE")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUSER_CONTINENT_CODE", NullToNA(col("USER_CONTINENT_CODE")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUSER_TIME_ZONE", NullToNA(col("USER_TIME_ZONE")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUSER_REGION_CODE", NullToNA(col("USER_REGION_CODE")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUSER_CITY_NAME", NullToNA(col("USER_CITY_NAME")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUSER_COUNTY_NAME", NullToNA(col("USER_COUNTY_NAME")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalREVISION_TAGS", NullToNA(col("REVISION_TAGS")))

    val BoolToDoubleUDF_ForUndoTruth = udf { (BoolAsString: String) => if (BoolAsString == "T") 1.0 else 0.0 }
    // val BoolToDoubleUDF_labelTruth = udf { (BoolAsString: String) => if (BoolAsString == "T") "T" else "F" }
    val BoolToDoubleUDF_labelTruth = udf { (BoolAsString: String) => if (BoolAsString == "T") 1.0 else 0.0 }

    val IntegerToDouble = udf { (IntegerRevisionSessionID: Integer) => if (IntegerRevisionSessionID == null) 0.0 else IntegerRevisionSessionID.toDouble }
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalROLLBACK_REVERTED", BoolToDoubleUDF_labelTruth(col("ROLLBACK_REVERTED")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalUNDO_RESTORE_REVERTED", BoolToDoubleUDF_ForUndoTruth(col("UNDO_RESTORE_REVERTED")))

    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalREVISION_SESSION_ID", IntegerToDouble(col("REVISION_SESSION_ID")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberofRevisionsUserContributed", IntegerToDouble(col("NumberofRevisionsUserContributed")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberofUniqueItemsUseredit", IntegerToDouble(col("NumberofUniqueItemsUseredit")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberRevisionItemHas", IntegerToDouble(col("NumberRevisionItemHas")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalNumberUniqUserEditItem", IntegerToDouble(col("NumberUniqUserEditItem")))
    Fill_Missing_Final_All_Features = Fill_Missing_Final_All_Features.withColumn("FinalFreqItem", IntegerToDouble(col("FreqItem")))

    // =================================================Caharacter Features : Double , Integer Features ============================
    // Double Ratio:  For Ratio Double column, Fill -1 value by Median:Character Features + Ratio of Word Features :
    var Samples = Fill_Missing_Final_All_Features.sample(false, sampleFraction)
    Samples.createOrReplaceTempView("df")

    // Upper  case
    val Mean_C1uppercaseratio = Samples.agg(mean("C1uppercaseratio")).head()
    val C1_Mean = Mean_C1uppercaseratio.getDouble(0)
    val lkpUDF1 = udf { (i: Double) => if (i == 0) C1_Mean else i }

    val df1 = Fill_Missing_Final_All_Features.withColumn("FinalC1uppercaseratio", lkpUDF1(col("C1uppercaseratio")))

    // Lower case
    val Mean_C2lowercaseratio = Samples.agg(mean("C2lowercaseratio")).head()
    val C2_Mean = Mean_C2lowercaseratio.getDouble(0)
    val lkpUDF2 = udf { (i: Double) => if (i == 0) C2_Mean else i }
    val df2 = df1.withColumn("FinalC2lowercaseratio", lkpUDF2(col("C2lowercaseratio")))

    // Alphanumeric:
    val Mean_C3alphanumericratio = Samples.agg(mean("C3alphanumericratio")).head()
    val C3_Mean = Mean_C3alphanumericratio.getDouble(0)
    val lkpUDF3 = udf { (i: Double) => if (i == 0) C3_Mean else i }
    val df3 = df2.withColumn("FinalC3alphanumericratio", lkpUDF3(col("C3alphanumericratio")))

    // Ascii
    val Mean_C4asciiratio = Samples.agg(mean("C4asciiratio")).head()
    val C4_Mean = Mean_C4asciiratio.getDouble(0)
    val lkpUDF4 = udf { (i: Double) => if (i == 0) C4_Mean else i }

    val df4 = df3.withColumn("FinalC4asciiratio", lkpUDF4(col("C4asciiratio")))

    // Bracket
    val Mean_C5bracketratio = Samples.agg(mean("C5bracketratio")).head()
    val C5_Mean = Mean_C5bracketratio.getDouble(0)
    val lkpUDF5 = udf { (i: Double) => if (i == 0) C5_Mean else i }
    val df5 = df4.withColumn("FinalC5bracketratio", lkpUDF5(col("C5bracketratio")))

    // Digital
    val Mean_C6digitalratio = Samples.agg(mean("C6digitalratio")).head()
    val C6_Mean = Mean_C6digitalratio.getDouble(0)
    val lkpUDF6 = udf { (i: Double) => if (i == 0) C6_Mean else i }

    val df6 = df5.withColumn("FinalC6digitalratio", lkpUDF6(col("C6digitalratio")))

    // Latian
    val Mean_C7latinratio = Samples.agg(mean("C7latinratio")).head()
    val C7_Mean = Mean_C7latinratio.getDouble(0)
    val lkpUDF7 = udf { (i: Double) => if (i == 0) C7_Mean else i }

    val df7 = df6.withColumn("FinalC7latinratio", lkpUDF7(col("C7latinratio")))

    // WhiteSpace
    val Mean_C8whitespaceratio = Samples.agg(mean("C8whitespaceratio")).head()
    val C8_Mean = Mean_C8whitespaceratio.getDouble(0)
    val lkpUDF8 = udf { (i: Double) => if (i == 0) C8_Mean else i }
    val df8 = df7.withColumn("FinalC8whitespaceratio", lkpUDF8(col("C8whitespaceratio")))

    // Punc
    val Mean_C9puncratio = Samples.agg(mean("C9puncratio")).head()
    val C9_Mean = Mean_C9puncratio.getDouble(0)
    val lkpUDF9 = udf { (i: Double) => if (i == 0) C9_Mean else i }
    val df9 = df8.withColumn("FinalC9puncratio", lkpUDF9(col("C9puncratio")))

    // Mean :
    // character integer values :
    val Mean_C10longcharacterseq = Samples.agg(mean("C10longcharacterseq")).head()
    val C10_Mean = Mean_C10longcharacterseq.getDouble(0)

    val lkpUDFC10 = udf { (i: Double) => if (i == 0) C10_Mean else i }

    val df10 = df9.withColumn("FinalC10longcharacterseq", lkpUDFC10(col("C10longcharacterseq")))

    // Median
    val Mean_C11arabicratio = Samples.agg(mean("C11arabicratio")).head()
    val C11_Mean = Mean_C11arabicratio.getDouble(0)
    val lkpUDFC11 = udf { (i: Double) => if (i == 0) C11_Mean else i }
    val df11 = df10.withColumn("FinalC11arabicratio", lkpUDFC11(col("C11arabicratio")))

    //
    val Mean_C12bengaliratio = Samples.agg(mean("C12bengaliratio")).head()
    val C12_Mean = Mean_C12bengaliratio.getDouble(0)
    val lkpUDFC12 = udf { (i: Double) => if (i == 0) C12_Mean else i }
    val df12 = df11.withColumn("FinalC12bengaliratio", lkpUDFC12(col("C12bengaliratio")))

    //
    val Mean_C13brahmiratio = Samples.agg(mean("C13brahmiratio")).head()
    val C13_Mean = Mean_C13brahmiratio.getDouble(0)
    val lkpUDFC13 = udf { (i: Double) => if (i == 0) C13_Mean else i }
    val df13 = df12.withColumn("FinalC13brahmiratio", lkpUDFC13(col("C13brahmiratio")))

    //
    val Mean_C14cyrilinratio = Samples.agg(mean("C14cyrilinratio")).head()
    val C14_Mean = Mean_C14cyrilinratio.getDouble(0)
    val lkpUDFC14 = udf { (i: Double) => if (i == 0) C14_Mean else i }
    val df14 = df13.withColumn("FinalC14cyrilinratio", lkpUDFC14(col("C14cyrilinratio")))

    //
    val Mean_C15hanratio = Samples.agg(mean("C15hanratio")).head()
    val C15_Mean = Mean_C15hanratio.getDouble(0)
    val lkpUDFC15 = udf { (i: Double) => if (i == 0) C15_Mean else i }
    val df15 = df14.withColumn("FinalC15hanratio", lkpUDFC15(col("C15hanratio")))

    //
    val Mean_c16malysiaratio = Samples.agg(mean("c16malysiaratio")).head()
    val C16_Mean = Mean_c16malysiaratio.getDouble(0)
    val lkpUDFC16 = udf { (i: Double) => if (i == 0) C16_Mean else i }
    val df16 = df15.withColumn("Finalc16malysiaratio", lkpUDFC16(col("c16malysiaratio")))

    //
    val Mean_C17tamiratio = Samples.agg(mean("C17tamiratio")).head()
    val C17_Mean = Mean_C17tamiratio.getDouble(0)
    val lkpUDFC17 = udf { (i: Double) => if (i == 0) C17_Mean else i }
    val df17 = df16.withColumn("FinalC17tamiratio", lkpUDFC17(col("C17tamiratio")))

    //
    val Mean_C18telugratio = Samples.agg(mean("C18telugratio")).head()
    val C18_Mean = Mean_C18telugratio.getDouble(0)
    val lkpUDFC18 = udf { (i: Double) => if (i == 0) C18_Mean else i }
    val df18 = df17.withColumn("FinalC18telugratio", lkpUDFC18(col("C18telugratio")))

    //

    val Mean_C19symbolratio = Samples.agg(mean("C19symbolratio")).head()
    val C19_Mean = Mean_C19symbolratio.getDouble(0)
    val lkpUDFC19 = udf { (i: Double) => if (i == 0) C19_Mean else i }
    val df19 = df18.withColumn("FinalC19symbolratio", lkpUDFC19(col("C19symbolratio")))

    //
    val Mean_C20alpharatio = Samples.agg(mean("C20alpharatio")).head()
    val C20_Mean = Mean_C20alpharatio.getDouble(0)
    val lkpUDFC20 = udf { (i: Double) => if (i == 0) C20_Mean else i }
    val df20 = df19.withColumn("FinalC20alpharatio", lkpUDFC20(col("C20alpharatio")))

    //
    val Mean_C21visibleratio = Samples.agg(mean("C21visibleratio")).head()
    val C21_Mean = Mean_C21visibleratio.getDouble(0)
    val lkpUDFC21 = udf { (i: Double) => if (i == 0) C21_Mean else i }
    val df21 = df20.withColumn("FinalC21visibleratio", lkpUDFC21(col("C21visibleratio")))

    val Mean_C22printableratio = Samples.agg(mean("C22printableratio")).head()
    val C22_Mean = Mean_C22printableratio.getDouble(0)
    val lkpUDFC22 = udf { (i: Double) => if (i == 0) C22_Mean else i }
    val df22 = df21.withColumn("FinalC22printableratio", lkpUDFC22(col("C22printableratio")))

    val Mean_C23blankratio = Samples.agg(mean("C23blankratio")).head()
    val C23_Mean = Mean_C23blankratio.getDouble(0)
    val lkpUDFC23 = udf { (i: Double) => if (i == 0) C23_Mean else i }
    val df23 = df22.withColumn("FinalC23blankratio", lkpUDFC23(col("C23blankratio")))

    val Mean_C24controlratio = Samples.agg(mean("C24controlratio")).head()
    val C24_Mean = Mean_C24controlratio.getDouble(0)
    val lkpUDFC24 = udf { (i: Double) => if (i == 0) C24_Mean else i }
    val df24 = df23.withColumn("FinalC24controlratio", lkpUDFC24(col("C24controlratio")))

    val Mean_C25hexaratio = Samples.agg(mean("C25hexaratio")).head()
    val C25_Mean = Mean_C25hexaratio.getDouble(0)
    val lkpUDFC25 = udf { (i: Double) => if (i == 0) C25_Mean else i }
    val df25 = df24.withColumn("FinalC25hexaratio", lkpUDFC25(col("C25hexaratio")))

    //
    // ************************************************End Character Features ****************************************************************************************

    // ************************************************Start Word  Features ****************************************************************************************

    val Mean_W1languagewordratio = Samples.agg(mean("W1languagewordratio")).head()
    val W1_Mean = Mean_W1languagewordratio.getDouble(0)
    val lkpUDFW1 = udf { (i: Double) => if (i == 0) W1_Mean else i }
    val df26 = df25.withColumn("FinalW1languagewordratio", lkpUDFW1(col("W1languagewordratio")))

    // 3.

    val Mean_W3lowercaseratio = Samples.agg(mean("W3lowercaseratio")).head()
    val W3_Mean = Mean_W3lowercaseratio.getDouble(0)
    val lkpUDFW3 = udf { (i: Double) => if (i == 0) W3_Mean else i }
    val df27 = df26.withColumn("FinalW3lowercaseratio", lkpUDFW3(col("W3lowercaseratio")))

    // 4. Integer " Mean:
    val Mean_W4longestword = Samples.agg(mean("W4longestword")).head()
    val W4_Mean = Mean_W4longestword.getDouble(0)
    val lkpUDFW4 = udf { (i: Double) => if (i == 0) W4_Mean else i }
    val df28 = df27.withColumn("FinalW4longestword", lkpUDFW4(col("W4longestword")))

    // 5. Boolean (Double ) W5IscontainURL
    // 6.

    val Mean_W6badwordratio = Samples.agg(mean("W6badwordratio")).head()
    val W6_Mean = Mean_W6badwordratio.getDouble(0)
    val lkpUDFW6 = udf { (i: Double) => if (i == 0) W6_Mean else i }
    val df29 = df28.withColumn("FinalW6badwordratio", lkpUDFW6(col("W6badwordratio")))

    // 7.

    val Mean_W7uppercaseratio = Samples.agg(mean("W7uppercaseratio")).head()
    val W7_Mean = Mean_W7uppercaseratio.getDouble(0)
    val lkpUDFW7 = udf { (i: Double) => if (i == 0) W7_Mean else i }

    val df30 = df29.withColumn("FinalW7uppercaseratio", lkpUDFW7(col("W7uppercaseratio")))

    // 8.
    val Mean_W8banwordratio = Samples.agg(mean("W8banwordratio")).head()
    val W8_Mean = Mean_W8banwordratio.getDouble(0)
    val lkpUDFW8 = udf { (i: Double) => if (i == 0) W8_Mean else i }
    val df31 = df30.withColumn("FinalW8banwordratio", lkpUDFW8(col("W8banwordratio")))

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
    val S1_Mean = roundDouble(Mean_S1CommentTailLength.getDouble(0))
    val lkpUDFS1 = udf { (i: Double) => if (i == 0) S1_Mean else i }
    val df37 = df36.withColumn("FinalS1CommentTailLength", lkpUDFS1(col("S1CommentTailLength")))

    // 2. Double  but Not ratio values :
    val Mean_S2SimikaritySitelinkandLabel = Samples.agg(mean("S2SimikaritySitelinkandLabel")).head()
    val S2_Mean = roundDouble(Mean_S2SimikaritySitelinkandLabel.getDouble(0))
    val lkpUDFS2 = udf { (i: Double) => if (i == 0) S2_Mean else i }
    val df39 = df37.withColumn("FinalS2SimikaritySitelinkandLabel", lkpUDFS2(col("S2SimikaritySitelinkandLabel")))

    // 3. Double  but Not ratio values :
    val Mean_S3SimilarityLabelandSitelink = Samples.agg(mean("S3SimilarityLabelandSitelink")).head()
    val S3_Mean = roundDouble(Mean_S3SimilarityLabelandSitelink.getDouble(0))
    val lkpUDFS3 = udf { (i: Double) => if (i == 0.0) S3_Mean else i }
    val df40 = df39.withColumn("FinalS3SimilarityLabelandSitelink", lkpUDFS3(col("S3SimilarityLabelandSitelink")))

    // 4.  Double  but Not ratio values :
    val Mean_S4SimilarityCommentComment = Samples.agg(mean("S4SimilarityCommentComment")).head()
    val S4_Mean = roundDouble(Mean_S4SimilarityCommentComment.getDouble(0))
    val lkpUDFS4 = udf { (i: Double) => if (i == 0.0) S4_Mean else i }
    val df41 = df40.withColumn("FinalS4SimilarityCommentComment", lkpUDFS4(col("S4SimilarityCommentComment")))

    val df42 = df41.withColumn(
      "StringFeatures",

      concat(

        // statement String features:
        $"SS1Property", // 1-
        lit("<3VandalismDetector4>"), $"SS2DataValue", // 2-
        lit("<3VandalismDetector4>"), $"SS3ItemValue", // 3-

        // User String Features:
        lit("<3VandalismDetector4>"), $"U1IsPrivileged", // 4-
        lit("<3VandalismDetector4>"), $"U2IsBotUser", // 5-
        lit("<3VandalismDetector4>"), $"U3IsBotuserWithFlaguser", // 6-
        lit("<3VandalismDetector4>"), $"U4IsProperty", // 7-
        lit("<3VandalismDetector4>"), $"U5IsTranslator", // 8-
        lit("<3VandalismDetector4>"), $"U6IsRegister", // 9-
        lit("<3VandalismDetector4>"), $"U7IPValue", // 10-
        lit("<3VandalismDetector4>"), $"U8UserID", // 11-
        lit("<3VandalismDetector4>"), $"U9HasBirthDate", // 12
        lit("<3VandalismDetector4>"), $"U10HasDeathDate", // 13

        // Item String features :

        lit("<3VandalismDetector4>"), $"I1NumberLabels", // 14
        lit("<3VandalismDetector4>"), $"I2NumberDescription", // 15
        lit("<3VandalismDetector4>"), $"I3NumberAliases", // 16
        lit("<3VandalismDetector4>"), $"I4NumberClaims", // 17
        lit("<3VandalismDetector4>"), $"I5NumberSitelinks", // 18
        lit("<3VandalismDetector4>"), $"I6NumberStatement", // 19
        lit("<3VandalismDetector4>"), $"I7NumberReferences", // 20
        lit("<3VandalismDetector4>"), $"I8NumberQualifier", // 21
        lit("<3VandalismDetector4>"), $"I9NumberQualifierOrder", // 22
        lit("<3VandalismDetector4>"), $"I10NumberBadges", // 23
        lit("<3VandalismDetector4>"), $"I11ItemTitle", // 24

        // Revision  String Features:
        // lit("<3VandalismDetector4>"), $"R1languageRevision",// 25
        lit("<3VandalismDetector4>"), $"R2RevisionLanguageLocal", // 26
        lit("<3VandalismDetector4>"), $"R3IslatainLanguage", // 27
        lit("<3VandalismDetector4>"), $"R4JsonLength", // 28

        lit("<3VandalismDetector4>"), $"R5RevisionAction", // 29
        lit("<3VandalismDetector4>"), $"R6PrevReviAction", // 30
        lit("<3VandalismDetector4>"), $"R7RevisionAccountChange", // 31
        lit("<3VandalismDetector4>"), $"R8ParRevision", // 32

        lit("<3VandalismDetector4>"), $"R9RevisionTime", // 33
        lit("<3VandalismDetector4>"), $"R10RevisionSize", // 34
        lit("<3VandalismDetector4>"), $"R11ContentType", // 35
        lit("<3VandalismDetector4>"), $"R12BytesIncrease", // 36

        lit("<3VandalismDetector4>"), $"R13TimeSinceLastRevi", // 37
        lit("<3VandalismDetector4>"), $"R14CommentLength", // 38

        // extra
        lit("<3VandalismDetector4>"), $"HasHashTable", // 39
        lit("<3VandalismDetector4>"), $"IsspecialUser", // 40
        lit("<3VandalismDetector4>"), $"IsProperty", //
        lit("<3VandalismDetector4>"), $"IsPropertyQuestion", //

        // Meta data string
        lit("<3VandalismDetector4>"), $"FinalUSER_COUNTRY_CODE", // 41
        lit("<3VandalismDetector4>"), $"FinalUSER_CONTINENT_CODE", // 42
        lit("<3VandalismDetector4>"), $"FinalUSER_TIME_ZONE", // 43
        lit("<3VandalismDetector4>"), $"FinalUSER_REGION_CODE", // 44
        lit("<3VandalismDetector4>"), $"FinalUSER_CITY_NAME", // 45
        lit("<3VandalismDetector4>"), $"FinalUSER_COUNTY_NAME", // 46
        lit("<3VandalismDetector4>"), $"FinalREVISION_TAGS")) // 47

    val toArray = udf((record: String) => record.split("<3VandalismDetector4>").map(_.toString()))
    val test1 = df42.withColumn("StringFeatures", toArray(col("StringFeatures")))

    val word2Vec = new Word2Vec().setInputCol("StringFeatures").setOutputCol("result").setVectorSize(48).setMinCount(0) // it was 44 before add the extra
    val model = word2Vec.fit(test1)
    val result = model.transform(test1) // .rdd

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

      // Meta , truth , Freq
      // meta :
      "FinalREVISION_SESSION_ID",
      // Truth:
      "FinalUNDO_RESTORE_REVERTED",

      // Freq:
      "FinalNumberofRevisionsUserContributed",
      "FinalNumberofUniqueItemsUseredit", "FinalNumberRevisionItemHas", "FinalNumberUniqUserEditItem", "FinalFreqItem")).setOutputCol("features")
    val Training_Data = assembler.transform(test_new2)

    Training_Data.createOrReplaceTempView("DB")
    val TrainingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED  as label from DB")

    TrainingData

  }

  // Full All Features String:
  def allFeatures(row: Row): String = {

    var temp = ""

    // Revision ID
    val Rid = row(0).toString()
    temp = Rid.toString().trim()

    // all characters
    val character_Str_String = characterFeatures(row)
    temp = temp + "<1VandalismDetector2>" + character_Str_String

    // all Words
    val Words_Str_String = wordFeatures(row)
    temp = temp + "<1VandalismDetector2>" + Words_Str_String

    // all sentences
    val Sentences_Str_String = sentenceFeatures(row)
    temp = temp + "<1VandalismDetector2>" + Sentences_Str_String

    // all statements
    val Statement_Str_String = statementFeatures(row)
    temp = temp + "<1VandalismDetector2>" + Statement_Str_String

    // User Features -  there are 3 Joins in last stage when we have Data Frame
    val User_Str_String = userFeaturesNormal(row)
    temp = temp + "<1VandalismDetector2>" + User_Str_String

    // Item Features -  there are 3 Joins in last stage when we have Data Frame
    val Item_Str_String = itemFeatures(row)
    temp = temp + "<1VandalismDetector2>" + Item_Str_String

    // Revision Features
    val Revision_Str_String = revisionFeatures(row)
    temp = temp + "<1VandalismDetector2>" + Revision_Str_String

    temp.trim()

  }

  // Function for character features
  def characterFeatures(row: Row): String = {

    var str_results = ""
    // 1. Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // 2. Revision ID current operation:
    var RevisionID = row(0)
    // 3. row(2) =  represent the Comment:
    var CommentRecord_AsString = row(2).toString()
    // 4. extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
    val Temp_commentTail = Comment.extractCommentTail(CommentRecord_AsString)

    if (Temp_commentTail != "" && Temp_commentTail != "NA") { // That means the comment is normal comment:
      var vectorElements = Character.characterFeatures(Temp_commentTail)

      var Str_vector_Values = arrayToString(vectorElements)
      str_results = Str_vector_Values

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

      var Str_vector_Values = arrayToString(RatioValues)
      str_results = Str_vector_Values

    }
    // CharacterFeatures
    str_results.trim()
  }

  // Function for Word features
  def wordFeatures(row: Row): String = {

    var str_results = ""
    // Row from  partitioned Pair RDD:
    // Revision ID current operation:
    var RevisionID = row(0)
    // row(2) =  represent the Comment:
    var CommentRecord_AsString = row(2).toString()

    // Extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
    val Temp_commentTail = Comment.extractCommentTail(CommentRecord_AsString)

    var tempQids = 0.0
    var temLinks = 0.0
    var temlangs = 0.0

    if (row(19) != null && row(25) != null) {

      val word = new Word()
      var current_Body_Revision = row(2).toString() + row(8).toString()
      var Prev_Body_Revision = row(19).toString() + row(25).toString()

      // Feature PortionOfQids
      var count_Qids_Prev = word.getNumberOfQId(Prev_Body_Revision)
      var count_Qids_Current = word.getNumberOfQId(current_Body_Revision)
      var porortion_Qids = word.proportion(count_Qids_Prev, count_Qids_Current)
      tempQids = porortion_Qids

      // Feature PortionOfLanguageAdded
      var count_Lang_Prev = word.getNumberOfLanguageWord(Prev_Body_Revision)
      var count_lang_Current = word.getNumberOfLanguageWord(current_Body_Revision)
      var porportion_Lang = word.proportion(count_Lang_Prev, count_lang_Current)
      temlangs = porportion_Lang

      // Feature PortionOfLinksAddes
      var count_links_Prev = word.getNumberOfLinks(Prev_Body_Revision)
      var count_links_Current = word.getNumberOfLinks(current_Body_Revision)
      var porportion_links = word.proportion(count_links_Prev, count_links_Current)
      temLinks = porportion_links
    } else {

      var porortion_Qids = tempQids // =0.0
      var porportion_Lang = temlangs // =0.0
      var porportion_links = temLinks // =0.0

    }
    if (Temp_commentTail != "" && Temp_commentTail != "NA") {
      val word = new Word()
      // 10- Features have Double type
      var ArrayElements = word.wordFeatures(Temp_commentTail)

      if (row(19) != null) {
        var prevComment = row(19)
        if (prevComment != null) {
          var Prev_commentTail = Comment.extractCommentTail(prevComment.toString())
          if (Prev_commentTail != "") {

            // 11.Feature Current_Previous_CommentTial_NumberSharingWords:

            val NumberSharingWords = word.currentPreviousCommentTialNumberSharingWords(Temp_commentTail, Prev_commentTail)
            ArrayElements(12) = NumberSharingWords.toDouble
            // 12.Feature Current_Previous_CommentTial_NumberSharingWords without Stopword:
            val NumberSharingWordsWithoutStopwords = word.currentPreviousCommentTialNumberSharingWordsWithoutStopWords(Temp_commentTail, Prev_commentTail)
            ArrayElements(13) = NumberSharingWordsWithoutStopwords.toDouble

          } else {
            ArrayElements(12) = 0.0
            ArrayElements(13) = 0.0
          }

        }
      } else {
        ArrayElements(12) = 0.0
        ArrayElements(13) = 0.0

      }

      ArrayElements(14) = tempQids
      ArrayElements(15) = temlangs
      ArrayElements(16) = temLinks

      var Str_vector_Values = arrayToString(ArrayElements)
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

      var Str_vector_Values = arrayToString(RatioValues)
      str_results = Str_vector_Values

    }

    str_results
  }

  // Function for Sentences features
  def sentenceFeatures(row: Row): String = {

    var str_results = ""
    // This will be used to save values in vector
    var DoubleValues = new Array[Double](4)

    // 2. Revision ID current operation:
    var RevisionID = row(0)
    // 3. row(2) =  represent the Full Comment:
    var CommentRecord_AsString = row(2).toString()
    // 4. extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
    val Temp_commentTail = Comment.extractCommentTail(CommentRecord_AsString)

    if (Temp_commentTail != "" && Temp_commentTail != "NA") {

      // This is CommentTail Feature:-----------------------------------------------------
      val comment_Tail_Length = Temp_commentTail.length()

      // Feature 1 comment tail length
      DoubleValues(0) = comment_Tail_Length

      // Feature 2 similarity  between comment contain Sitelink and label :
      // Check the language in comment that contain sitelinkword: --------------------

      if (CommentRecord_AsString.contains("sitelink")) { // start 1 loop
        // 1. First step : get the language from comment
        val languagesitelink_from_Comment = Sentence.extractCommentSiteLinkLanguageType(CommentRecord_AsString).trim()

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
            DoubleValues(1) = roundDouble(result)
          } else {
            DoubleValues(1) = 0.0
          }

        } else {
          DoubleValues(1) = 0.0
        }
      } else {

        DoubleValues(1) = 0.0

      }

      // Feature 3 similarity between comment contain label word and sitelink
      // Check the language in comment that contain Label word:-----------------------
      if (CommentRecord_AsString.contains("label")) {
        // 1. First step : get the language from comment
        val languageLabel_from_Comment = Sentence.extractCommentLabelLanguageType(CommentRecord_AsString).trim()
        // 2. second step: get  the site link  tage from json table :
        if (row(13).toString() != "[]") { // start 2 loop
          val jsonStr = "\"\"\"" + row(13).toString() + "\"\"\"" // row(13) is the sitelink record
          val jsonObj: JSONObject = new JSONObject(row(13).toString())
          var text_lang = languageLabel_from_Comment + "wiki"
          var key_lang = "\"" + text_lang + "\""
          if (jsonStr.contains(""""site"""" + ":" + key_lang)) {
            val value_from_sitelink: String = jsonObj.getJSONObject(text_lang).getString("title")
            val result = StringUtils.getJaroWinklerDistance(Temp_commentTail, value_from_sitelink)
            DoubleValues(2) = roundDouble(result)

          } else {
            DoubleValues(2) = 0.0

          }

        } else {
          DoubleValues(2) = 0.0

        }

      } else {
        DoubleValues(2) = 0.0

      }

      if (row(19) != null) {

        val prevComment = row(19)
        var Prev_commentTail = Comment.extractCommentTail(prevComment.toString())
        val Similarityresult = StringUtils.getJaroWinklerDistance(Temp_commentTail, Prev_commentTail)

        if (!Similarityresult.isNaN()) {
          DoubleValues(3) = roundDouble(Similarityresult)
        } else {

          DoubleValues(3) = 0.0
        }

      } else {
        DoubleValues(3) = 0.0

      }

      var Str_vector_Values = arrayToString(DoubleValues)
      str_results = Str_vector_Values

    } else {

      DoubleValues(0) = 0.0
      DoubleValues(1) = 0.0
      DoubleValues(2) = 0.0
      DoubleValues(3) = 0.0

      var Str_vector_Values = arrayToString(DoubleValues)
      str_results = Str_vector_Values

    }

    str_results

  }

  // statement Features :
  def statementFeatures(row: Row): String = {
    var full_Str_Result = ""
    // 1. row(2) =  represent the Comment:
    var fullcomment = row(2).toString()

    val property = Statement.getProperty(fullcomment)
    val DataValue = Statement.getDataValue(fullcomment)
    val Itemvalue = Statement.getItemValue(fullcomment)

    // Feature 1 - Property
    if (property != "" && property != null) {
      full_Str_Result = property.trim()
    } else {
      full_Str_Result = "NA"

    }
    // Feature 2 - DataValue
    if (DataValue != "" && DataValue != null) {

      full_Str_Result = full_Str_Result.trim() + "<1VandalismDetector2>" + DataValue.trim()

    } else {
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"

    }
    // Feature 3 - Itemvalue
    if (Itemvalue != "" && Itemvalue != null) {

      full_Str_Result = full_Str_Result.trim() + "<1VandalismDetector2>" + Itemvalue.trim()

    } else {
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"
    }

    full_Str_Result.trim()
  }
  // User Normal Features :
  def userFeaturesNormal(row: Row): String = {

    var str_results = "NA"

    // Row from  partitioned Pair RDD:
    // row(7) =  represent the Contributor name:
    var full_comment = row(2).toString()
    var contributor_Name = row(7).toString()
    var contributor_ID = row(6).toString()
    var contributor_IP = row(5).toString()

    if (contributor_Name != "NA") {

      // 1. Is privileged :  There are 5 cases : if one of these cases is true that mean it is privileged else it is not privileged user
      var flag_case1 = User.checkNameIsGlobalSysopUser(contributor_Name)
      var flag_case2 = User.checkNameIsGlobalRollBackerUser(contributor_Name)
      var flag_case3 = User.checkNameIsGlobalStewarUser(contributor_Name)
      var flag_case4 = User.checkNameIsAdmin(contributor_Name)
      var flag_case5 = User.checkNameIsRollBackerUser(contributor_Name)

      if (flag_case1 == true || flag_case2 == true || flag_case3 == true || flag_case4 == true || flag_case5 == true) {

        str_results = "YES".trim()
      } else {
        str_results = "NO".trim()
      }
    } else {

      str_results = "NO".trim()
    }
    if (contributor_Name != "NA") {

      // 2. is BotUser : There are 3 cases  :
      var flag_case1_1 = User.checkNameIsLocalBotUser(contributor_Name)
      var flag_case2_2 = User.checkNameIsGlobalbotUser(contributor_Name)
      var flag_case3_3 = User.checkNameIsExtensionBotUser(contributor_Name)

      if (flag_case1_1 == true || flag_case2_2 == true || flag_case3_3 == true) {

        str_results = str_results + "<1VandalismDetector2>" + "YES".trim()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
    }

    if (contributor_Name != "NA") {

      // 3. is Bot User without BotflagUser : There is 1 case  :
      var flag_BUWBF = User.checkNameIsBotUserWithoutBotFlagUser(contributor_Name)

      if (flag_BUWBF == true) {
        str_results = str_results + "<1VandalismDetector2>" + "YES".trim()
      } else {
        str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
      }

    } else {

      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
    }

    if (contributor_Name != "NA") {
      // 4. is Property  creator :
      var flagCreator = User.checkNameIsPropertyCreator(contributor_Name)

      if (flagCreator == true) {
        str_results = str_results + "<1VandalismDetector2>" + "YES".trim()
      } else {
        str_results = str_results + "<1VandalismDetector2>" + "NO".trim()

      }
    } else {

      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()

    }

    if (contributor_Name != "NA") {

      // 5. is translator :
      var flagTranslator = User.checkNameIsTranslator(contributor_Name)
      if (flagTranslator == true) {
        str_results = str_results + "<1VandalismDetector2>" + "YES".trim()
      } else {
        str_results = str_results + "<1VandalismDetector2>" + "NO".trim()

      }

    } else {

      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()

    }

    if (contributor_Name != "NA") {

      // 6. is register user:

      var flagRegistered = User.isRegisteredUser(contributor_Name)
      if (flagRegistered == true) {
        str_results = str_results + "<1VandalismDetector2>" + "YES".trim()
      } else {
        str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
      }

    } else {

      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()

    }
    // 7. IP as a long value
    if (contributor_IP != "0") {
      str_results = str_results + "<1VandalismDetector2>" + contributor_IP.toString().trim()
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
    }

    // 8. ID
    if (contributor_ID != "0") {
      str_results = str_results + "<1VandalismDetector2>" + contributor_ID.toString().trim()
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
    }

    // 9-  BitrthDate

    var BirthDate = User.hasBirthDate(full_comment)
    var DeathDate = User.hasDeathDate(full_comment)

    if (BirthDate == true) {
      str_results = str_results + "<1VandalismDetector2>" + "YES".trim()
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()
    }

    // 10 - DeatDate:
    if (DeathDate == true) {
      str_results = str_results + "<1VandalismDetector2>" + "YES".trim()
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "NO".trim()

    }

    str_results.trim()

  }

  def itemFeatures(row: Row): String = {

    var str_results = ""
    // Row from  partitioned Pair RDD:
    var new_Back_Row = Row()

    // 1. Feature depending on Label:
    var NumberOfLabel = 0
    if (row(9) != null) {
      var Label_String = row(9).toString()
      if (Label_String != "[]") {
        NumberOfLabel = Item.getNumberOfLabels(Label_String)
        str_results = NumberOfLabel.toString()
      } else {
        str_results = "0".toString()
      }
    } else {
      str_results = "0".toString()

    }

    // 2. Feature depending on Description:
    var NumberOfDescription = 0
    if (row(10) != null) {
      var Description_String = row(10).toString()
      if (Description_String != "[]") {
        NumberOfDescription = Item.getNumberOfDescription(Description_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfDescription.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()

    }

    // 3. Feature depending on Aliases:
    var NumberOfAliases = 0
    if (row(11) != null) {
      var Aliases_String = row(11).toString()
      if (Aliases_String != "[]") {
        NumberOfAliases = Item.getNumberOfAliases(Aliases_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfAliases.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()

    }
    // 4. Feature depending on Claims :
    var NumberOfClaims = 0
    if (row(12) != null) {
      var Claims_String = row(12).toString()
      if (Claims_String != "[]") {
        NumberOfClaims = Item.getNumberOfClaim(Claims_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfClaims.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()
      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }

    // 5. Feature depending on SiteLink
    var NumberOfSitelink = 0
    if (row(13) != null) {
      var SiteLink_String = row(13).toString()
      if (SiteLink_String != "[]") {
        NumberOfSitelink = Item.getNumberOfSiteLinks(SiteLink_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfSitelink.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }

    // 6. Feature depending on Claims - statements :
    var NumberOfstatement = 0

    if (row(12) != null) {
      var statement_String = row(12).toString() // from claim
      if (statement_String != "[]") {
        NumberOfstatement = Item.getNumberOfStatements(statement_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfstatement.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }

    // 7. Feature depending on Claims - References  :
    var NumberOfReferences = 0
    if (row(12) != null) {
      var References_String = row(12).toString() // from claim
      if (References_String != "[]") {
        NumberOfReferences = Item.getNumberOfReferences(References_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfReferences.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }
    // 8. Feature depending on claim
    var NumberOfQualifier = 0
    if (row(12) != null) {
      var Qualifier_String = row(12).toString() // from claim
      if (Qualifier_String != "[]") {
        NumberOfQualifier = Item.getNumberOfQualifier(Qualifier_String)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfQualifier.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }
    // 9. Features depending on  claim
    var NumberOfQualifier_order = 0
    if (row(12) != null) {
      var Qualifier_String_order = row(12).toString() // from claim
      if (Qualifier_String_order != "[]") {
        NumberOfQualifier_order = Item.getNumberOfQualifierOrder(Qualifier_String_order)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfQualifier_order.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }
    // 10. Feature depending on  Site link
    var NumberOfBadges = 0

    if (row(13) != null) {
      var BadgesString = row(13).toString() // from claim
      if (BadgesString != "[]") {
        NumberOfBadges = Item.getNumberOfBadges(BadgesString)
        str_results = str_results + "<1VandalismDetector2>" + NumberOfBadges.toString()

      } else {
        str_results = str_results + "<1VandalismDetector2>" + "0".toString()

      }
    } else {
      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }

    // 11. Item Title (instead of Item  ID)
    if (row(1) != null) {
      var Item_Id_Title = row(1).toString().replace("Q", "")
      var Item = Item_Id_Title.trim().toInt
      str_results = str_results + "<1VandalismDetector2>" + Item

    } else {

      str_results = str_results + "<1VandalismDetector2>" + "0".toString()
    }

    str_results.trim()

  }

  def revisionFeatures(row: Row): String = {

    var full_Str_Result = ""
    // 1. Row from  partitioned Pair RDD:
    var new_Back_Row = Row()
    // 2. Revision ID current operation:
    var RevisionID = row(0)
    // 3. row(2) =  represent the Comment:
    var fullcomment = row(2).toString()
    // DoubleValues(0) = length

    // 1. Revision Language  local:----------------------------------------------------------------------------
    val language = Revision.extractRevisionLanguage(fullcomment)
    if (language != "NA") {
      if (language.contains("-")) { // E.g.Revision ID = 10850 sample1
        var LocalLangArray: Array[String] = language.split("-", 2)
        var location = LocalLangArray(1)
        full_Str_Result = location.trim()
      } else {

        full_Str_Result = "NA"
      }

    } else {
      full_Str_Result = "NA"
    }

    // 2. Is it Latin Language or Not:-------------------------------------------------------------------------
    val flagLatin = Revision.checkContainLanguageLatinNonLatin(language)

    if (flagLatin == true) {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "YES"

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NO"
    }

    // 3. Json Length : be care full to RDD where the json before parsed--------------------------------------
    var Jason_Text = row(8).toString()
    var Json_Length = Jason_Text.length()

    full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + Json_Length.toString()

    // 4. Revision Action -:-----------------------------------------------------------------------
    val actions1 = Comment.extractActionsFromComments(fullcomment)

    if (actions1 != "" && actions1.contains("_")) {
      var ActionsArray1: Array[String] = actions1.split("_", 2)
      var action1 = ActionsArray1(0).toString()
      // var SubAction = ActionsArray(1)
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + action1.trim()
    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"
    }
    // 5.  Revision Prev-Action :-------------------------------------------------------------------------------
    if (row(19) != null) {
      var Prev_fullcomment1 = row(19).toString()
      val Prev_actions1 = Comment.extractActionsFromComments(fullcomment)
      if (Prev_actions1 != "" && Prev_actions1.contains("_")) {

        var Prev_ActionsArray1: Array[String] = Prev_actions1.split("_", 2)
        var Prev_action1 = Prev_ActionsArray1(0).trim()
        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + Prev_action1.trim()
      } else {

        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"

      }
    } else {
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"
    }
    //
    // 6. Revision Account user type change :----------------------------------------------------------------------------
    var changeFlag = false
    if (row(23) != null) {
      var Prev_Contributor_ID = row(23).toString()
      var Current_Contributor_ID = row(6).toString()

      if (Prev_Contributor_ID != Current_Contributor_ID) {

        changeFlag = true
        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "YES"

      } else {
        changeFlag = false
        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NO"
      }

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NO"

    }

    // 7.Revision Parent :-----------------------------------------------------------------------------------------------------
    if (row(3) != null && row(3) != "") {
      var RevisionParent = row(3).toString()
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + RevisionParent.toString().trim()
    } else {
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"

    }
    // 8. Revision Time Stamp------------------------------------------------------------------------------------------------
    if (row(4) != null && row(4) != "") {
      var RevisionTimeZone = row(4).toString()

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + RevisionTimeZone.trim()
    } else {
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"
    }
    // 9. Revision Size:----------------------------------------------------------------------------------------------------

    var RevesionBoday = row.toString()
    var RevSize = RevesionBoday.length()
    var strRevSize = RevSize.toString()

    full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + strRevSize

    // 10. ContentType: take Action1 as input : --------------------------------------------------------------

    val actions_New = Comment.extractActionsFromComments(fullcomment)
    if (actions_New != "" && actions_New.contains("_")) {

      var ActionsArrayNew: Array[String] = actions_New.split("_", 2)
      var actionNew = ActionsArrayNew(0)
      var contentType = Revision.getContentType(actionNew.trim())
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + contentType.trim()
    } else {
      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "NA"

    }
    // 11. Bytes Increase (  subtract Bytes current revision with previous revision ):--------------------------------------------------------------

    var CurrentRevision = "NA"
    var PreviRevision = "NA"

    // For Current Revision
    CurrentRevision = row(0).toString() + row(2).toString() + row(3).toString() + row(4).toString() + row(8).toString() + row(14).toString() + row(15).toString() + row(16).toString()
    if (row(5).toString() != "0") {
      CurrentRevision = CurrentRevision.trim() + row(5).toString()
    } else {
      CurrentRevision = CurrentRevision.trim() + row(6).toString() + row(7).toString()
    }

    // For Previous Revision :
    if (row(17) != null && row(19) != null && row(20) != null && row(21) != null && row(25) != null && row(31) != null && row(32) != null && row(33) != null) {
      if (row(22) != null && row(22).toString() != "0") {
        var PreviRevision = row(17).toString() + row(19).toString() + row(20).toString() +
          row(21).toString() + row(25).toString() + row(31).toString() + row(32).toString() + row(33).toString() + row(22).toString()

      } else if (row(23) != null && row(24) != null) {
        var PreviRevision = row(17).toString() + row(19).toString() + row(20).toString() +
          row(21).toString() + row(25).toString() + row(31).toString() + row(32).toString() + row(33).toString() + row(23).toString() + row(24).toString()
      } else {

        PreviRevision = "NA"
      }

    } else {
      PreviRevision = "NA"
    }

    if (PreviRevision != "NA") {

      var Bytes1 = CurrentRevision.length()
      var Bytes2 = PreviRevision.length()
      var BytesResults = Bytes1 - Bytes2

      if (BytesResults < 0) {

        BytesResults = BytesResults * -1
        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + BytesResults.toString()
      } else {

        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + BytesResults.toString()

      }
    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "0"

    }

    // 12. Time since last Revision: ----------------------------------------------------------------------

    if (row(21) != null) {

      var CurrentTime = dateToLong(row(4).toString())

      var PreviousTime = dateToLong(row(21).toString())

      var FinalTime = CurrentTime - PreviousTime

      if (FinalTime < 0) {
        FinalTime = FinalTime * -1
        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + FinalTime.toString()

      } else {

        full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + FinalTime.toString()
      }

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "0"

    }

    // 13. Comment Length:---------------------------------------
    var lengthcomment = fullcomment.length().toString()
    full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + lengthcomment

    // -------------------------
    // extra1 :

    if (fullcomment.contains("#")) {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "yes"

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "No"

    }

    // ---------------------------
    // extra2:
    if (fullcomment.contains("[[Special:Contributions")) {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "yes"

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "No"

    }

    // ---------------------------
    // extra3:
    if (fullcomment.contains("[[Property:P641]]")) {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "yes"

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "No"

    }

    // ---------------------------
    // extra4:

    if (fullcomment.contains("[[Q41466]]")) {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "yes"

    } else {

      full_Str_Result = full_Str_Result + "<1VandalismDetector2>" + "No"

    }

    full_Str_Result

  }

  def dateToLong(strDate: String): Long = {

    var str = strDate.replace("T", " ")
    str = str.replace("Z", "").trim()
    val ts1: java.sql.Timestamp = java.sql.Timestamp.valueOf(str)
    val tsTime1: Long = ts1.getTime()

    tsTime1
  }

  def replacingWithQuoto(keyValue: String, str: String): String = {

    var Full_Key = "\"" + "key" + "\"" + ":" + "\"" + keyValue + "\"" + ","
    var returnedResult = "" // if the Json text is not available

    if (str != null) {
      if (str != "NA" && str != "") { // be sure if the Json text is not available
        var container = str
        // val x= '"'+'"'+'"'+str+'"'+'"'+'"'
        val before = "</format>" + "<text xml:space=" + """"preserve"""" + ">"
        val after = "</text>"
        var quot = "\"\"\""

        val flag1 = container.contains(before)

        if (flag1 == true) {
          //    var dd= container.replace(before, quot)
          container = container.replace(before, "").trim()

        }
        //
        val flag2 = container.contains(after)

        if (flag2 == true) {
          //    var dd= tem.replace(after1, quot)
          container = container.replace(after, "").trim()

        }

        if (container != "") {

          var sb: StringBuffer = new StringBuffer(container)
          var result = sb.insert(1, Full_Key).toString()
          returnedResult = result.trim()

        }
      }
    }
    returnedResult
  }
}
