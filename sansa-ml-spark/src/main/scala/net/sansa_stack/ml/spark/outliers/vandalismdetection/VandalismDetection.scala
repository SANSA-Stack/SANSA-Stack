package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.{ SparkContext, RangePartitioner }
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.mapred.JobConf
import java.util.Scanner
import org.json.JSONObject
import org.apache.commons.lang3.StringUtils

class VandalismDetection extends Serializable {

  def Triger(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._ // for UDF
    import org.apache.spark.sql.types._

    println("Please Enter 1 for RDFXML process and 2 for NormalXML process:")
    val num = scala.io.StdIn.readLine()

    //RDF XML file :*********************************************************************************************************
    if (num == "1") {
      println("RDF XML .........!!!!!!")
      // Streaming records:RDFXML file :
      val jobConf_Record = new JobConf()
      val jobConf_Prefixes = new JobConf()

      val RDFXML_Parser_OBJ = new ParseRDFXML()
      val DRF_Builder_RDFXML_OBJ = new FacilitiesClass()

      val RDD_RDFXML = RDFXML_Parser_OBJ.start_RDFXML_Parser(jobConf_Record, jobConf_Prefixes, sc)
      RDD_RDFXML.foreach(println)

      //----------------------------DF for RDF XML ------------------------------------------
      //  Create SQLContext Object:
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val DFR_RDF_XML = DRF_Builder_RDFXML_OBJ.RDD_TO_DFR_RDFXML(RDD_RDFXML, sqlContext)
      DFR_RDF_XML.show()

      // NOrmal XML Example WikiData: ***************************************************************************************************
    } else if (num == "2") {
      // Streaming records:
      val jobConf = new JobConf()
      val NormalXML_Parser_OBJ = new ParseNormalXML()
      val RDD_OBJ = new ParseNormalXML()
      val RDD_All_Record = RDD_OBJ.DB_NormalXML_Parser(sc).distinct().cache()
      //RDD_All_Record.foreach(println)
      //println(RDD_All_Record.count())

      // ======= Json part :
      //Json RDD : Each record has its Revision iD:
      val JsonRDD = RDD_All_Record.map(_.split("NNLL")).map(v => replacing_with_Quoto(v(0), v(8))).distinct().cache()
      //JsonRDD.foreach(println)
      //println(JsonRDD.count())

      // Data set
      val Ds_Json = sqlContext.jsonRDD(JsonRDD).select("key", "id", "labels", "descriptions", "aliases", "claims", "sitelinks").distinct().cache()
      //Ds_Json.show()
      // println(Ds_Json.count())

      // ======= Tags part : // Contributor IP here is in Decimal format not IP format and It is converted in ParseNormalXml stage
      val TagsRDD = RDD_All_Record.map(_.split("NNLL")).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))).cache()
      val DF_Tags = TagsRDD.toDF("Rid", "Itemid", "comment", "pid", "time", "contributorIP", "contributorID", "contributorName", "JsonText", "model", "format", "sha").distinct().cache()
      //    DF_Tags.show()
      //    println(DF_Tags.count())

      //======== Join Json part with Tag Part:============================
      //Joining to have full data
      val DF_First_DF_Result_Join_Tags_and_Json = DF_Tags.as("T1").join(Ds_Json.as("T2"), $"T1.Rid" === $"T2.key", "leftouter").select("Rid", "itemid", "comment", "pid", "time", "contributorIP", "contributorID", "contributorName", "JsonText", "labels", "descriptions", "aliases", "claims", "sitelinks", "model", "format", "sha") //.orderBy("Rid", "Itemid")
      DF_First_DF_Result_Join_Tags_and_Json.registerTempTable("Data1")
      val dfr_DATA_JsonTages1 = sqlContext.sql("select * from Data1 order by itemid ,Rid ").cache()

      // Index the Table for previous Revision Process:

      val w1 = Window.orderBy("itemid")
      val result1 = dfr_DATA_JsonTages1.withColumn("index1", row_number().over(w1))
      //     result1.show()
      //     println(result1.count())

      val colNames = Seq("Rid2", "itemid2", "comment2", "pid2", "time2", "contributorIP2", "contributorID2", "contributorName2", "JsonText2", "labels2", "descriptions2", "aliases2", "claims2", "sitelinks2", "model2", "format2", "sha2")
      val DF_Second = DF_First_DF_Result_Join_Tags_and_Json.toDF(colNames: _*) //.distinct()
      DF_Second.registerTempTable("Data2")
      val dfr_DATA_JsonTages2 = sqlContext.sql("select * from Data2 order by itemid2 ,Rid2 ").cache()

      val w2 = Window.orderBy("itemid2")
      val result2 = dfr_DATA_JsonTages2.withColumn("index2", row_number().over(w2))
      // result2.show()

      //===================================================================Previous Revision==========================================================================

      val DF_Joined = result1.as("df1").join(result2.as("df2"), col("itemid") === col("itemid2") && col("index1") === col("index2") + 1, "leftouter").select("Rid", "itemid", "comment", "pid", "time", "contributorIP", "contributorID", "contributorName", "JsonText", "labels", "descriptions", "aliases", "claims", "sitelinks", "model", "format", "sha", "Rid2", "itemid2", "comment2", "pid2", "time2", "contributorIP2", "contributorID2", "contributorName2", "JsonText2", "labels2", "descriptions2", "aliases2", "claims2", "sitelinks2", "model2", "format2", "sha2")
      //.select("itemid", "Rid","pid","time","itemid2","Rid2","pid2","time2")
      val RDD_After_JoinDF = DF_Joined.rdd.distinct()
      val x = RDD_After_JoinDF.map(row => (row(0).toString().toInt, row)).cache()
      val part = new RangePartitioner(4, x)
      val partitioned = x.partitionBy(part).persist() // persist is important for this case and obligatory.
      partitioned.foreach(println)

      //====================================================================postpone Parent ID====================================================================
      //  //Joining based on Parent Id to get the previous cases:
      //  val DF_Joined = DF_First_DF_Result_Join_Tags_and_Json.as("df1").join(DF_Second.as("df2"), $"df1.pid" === $"df2.Rid2", "leftouter").distinct()
      //  //DF_Joined.show()
      //  //println("nayef"+DF_Joined.count())
      //
      //  val RDD_After_JoinDF = DF_Joined.rdd.distinct()
      //  val x = RDD_After_JoinDF.map(row => (row(0).toString().toInt, row)).cache()
      //  //println("nayef"+x.count())
      //
      //  val part = new RangePartitioner(4, x)
      //
      //  val partitioned = x.partitionBy(part).persist() // persist is important for this case and obligatory.
      //  //partitioned.foreach(println)
      //  //println("nayef"+partitioned.count())

      //=====================================================Start:  Features:==================================================================================
      //   Characters Features:
      //                val Result_Character_Features = partitioned.map { case (x, y) => (x, Character_Features(y)) }
      //                Result_Character_Features.foreach(println)
      //                println("nayef" + Result_Character_Features.count())
      //***************************************************************************
      // Words  Features:
      //      val Result_Words_Features = partitioned.map { case (x, y) => (x, Words_Features(y)) }
      //      Result_Words_Features.foreach(println)
      //      println("nayef" + Result_Words_Features.count())
      //***************************************************************************
      // sentences Features:
      //        val Result_Sentences_Features = partitioned.map { case (x, y) => (x, Sentences_Features(y)) }
      //        Result_Sentences_Features.foreach(println)
      //***************************************************************************
      //Satatement Features :
      //          val Result_Statement_Features = partitioned.map { case (x, y) => (x, Statement_Features(y)) }
      //          Result_Statement_Features.foreach(println)

      //***************************************************************************
      // User Features :

      //**Part1: User Normal Features :
      //          val Result_User_Features = partitioned.map { case (x, y) => (x, User_Features_Normal(y)) }
      //           Result_User_Features.foreach(println)

      //**Part2:Features Based Freq from Tags DataFram:
      //DF_Tags.registerTempTable("TagesTable")
      //
      //  //a.User Frequency:
      //  //number of revisions a user has contributed
      //  //val resu= DF_Tags.groupBy("contributorID").agg(count("Rid"))
      //    val ContributorFreq_for_Each_Revision = sqlContext.sql("select contributorID, count(Rid) as NumberofRevisions from TagesTable where contributorID !='0000' group by contributorID ")
      //    ContributorFreq_for_Each_Revision.show()

      //b.Cumulated : Number of a unique Item a user has contributed.
      //    val CumulatedNumberof_uniqueItemsForUser = sqlContext.sql("select contributorID,  COUNT(DISTINCT itemid) as NumberofUniqueItems from TagesTable where contributorID !='0000' group by contributorID")
      //    CumulatedNumberof_uniqueItemsForUser.show()

      //
      //  //**Part3: Geografical information Feature:
      //  //REVISION_ID|REVISION_SESSION_ID|USER_COUNTRY_CODE|USER_CONTINENT_CODE|USER_TIME_ZONE|USER_REGION_CODE|USER_CITY_NAME|USER_COUNTY_NAME|REVISION_TAGS
      //    val df_GeoInf = sqlContext.read
      //      .format("com.databricks.spark.csv")
      //      .option("header", "true") // Use first line of all files as header
      //      .option("inferSchema", "true") // Automatically infer data types
      //      .load("hdfs://localhost:9000/mydata/nayef2.csv").select("REVISION_ID", "REVISION_SESSION_ID", "USER_COUNTRY_CODE", "USER_CONTINENT_CODE", "USER_TIME_ZONE", "USER_REGION_CODE", "USER_CITY_NAME", "USER_COUNTY_NAME", "REVISION_TAGS")
      //    df_GeoInf.show()
      //
      //    //** Part4: Join everyThing:

      //***************************************************************************
      //Item Features :
      //  val Result_Item_Features = partitioned.map { case (x, y) => (x, Item_Features(y)) }
      //  Result_Item_Features.foreach(println)
      //
      //  DF_Tags.registerTempTable("TagesTable")
      //  //1.Item Frequency: number of revisions an Item has
      //  val ItemFrequ = sqlContext.sql("select itemid, count(Rid) as freqItem from TagesTable  group by itemid")
      //  ItemFrequ.show()
      //  //2. Cumulate number of unique users have edited the Item : Did not consider the users IP. Contributor is an IP or Name. we consider name
      //  val CumulatedNumberof_UniqueUserForItem = sqlContext.sql("select itemid,  COUNT(DISTINCT contributorID) as FreqContributor from TagesTable where contributorID !='0000' group by itemid")
      //  CumulatedNumberof_UniqueUserForItem.show()
      //  //3. freq each Item :
      //  val Fre_Item = sqlContext.sql("select itemid,  COUNT(itemid) as FreqItem from TagesTable  group by itemid")
      //  Fre_Item.show()

      //***************************************************************************
      //  // Revision Features :
      //      val Result_Revision_Features = partitioned.map { case (x, y) => (x, Revision_Features(y)) }
      //      Result_Revision_Features.foreach(println)

      //***************************************************************************

      sc.stop()

    }

    //=================================================Functions Part=============================================================================

    // Function for character features
    def Character_Features(row: Row): Row = {
      //1. Row from  partitioned Pair RDD:
      var new_Back_Row = Row()
      //2. Revision ID current operation:
      var RevisionID = row(0)
      //3. row(2) =  represent the Comment:
      var CommentRecord_AsString = row(2).toString()
      //4. extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
      val CommentObj = new CommentProcessor()
      val Temp_commentTail = CommentObj.Extract_CommentTail(CommentRecord_AsString)

      if (Temp_commentTail != "" && Temp_commentTail != "NA") { // That means the comment is normal comment:
        val CharactersOBJ = new CharactersFeatures()
        var vectorElements = CharactersOBJ.Vector_Characters_Feature(Temp_commentTail)
        //CharacterFeatures = Vector_AsArrayElements
        new_Back_Row = Row(vectorElements)

      } else {

        var RatioValues = new Array[Double](25)
        RatioValues(0) = 0.0
        RatioValues(1) = 0.0
        RatioValues(2) = 0.0
        RatioValues(3) = 0.0
        RatioValues(4) = 0.0
        RatioValues(5) = 0.0
        RatioValues(6) = 0.0
        RatioValues(7) = 0.0
        RatioValues(8) = 0.0
        RatioValues(9) = 0.0
        RatioValues(10) = 0.0
        RatioValues(11) = 0.0
        RatioValues(12) = 0.0
        RatioValues(13) = 0.0
        RatioValues(14) = 0.0
        RatioValues(15) = 0.0
        RatioValues(16) = 0.0
        RatioValues(17) = 0.0
        RatioValues(18) = 0.0
        RatioValues(19) = 0.0
        RatioValues(20) = 0.0
        RatioValues(21) = 0.0
        RatioValues(22) = 0.0
        RatioValues(23) = 0.0
        RatioValues(24) = 0.0

        val FacilityOBJ = new FacilitiesClass()
        val vector_Values = FacilityOBJ.ToVector(RatioValues)
        new_Back_Row = Row(vector_Values)

      }
      // CharacterFeatures
      //new_Back_Row
      new_Back_Row
    }

    // Function for Word features
    def Words_Features(row: Row): Row = {
      //Row from  partitioned Pair RDD:
      var new_Back_Row = Row()
      //Revision ID current operation:
      var RevisionID = row(0)
      //row(2) =  represent the Comment:
      var CommentRecord_AsString = row(2).toString()
      //Extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
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

        var porortion_Qids = tempQids
        var porportion_Lang = temlangs
        var porportion_links = temLinks

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

            //11.Feature Current_Previous_CommentTial_NumberSharingWords:

            val NumberSharingWords = WordsOBJ.Current_Previous_CommentTial_NumberSharingWords(Temp_commentTail, Prev_commentTail)
            ArrayElements(12) = NumberSharingWords.toDouble
            //12.Feature Current_Previous_CommentTial_NumberSharingWords without Stopword:
            val NumberSharingWordsWithoutStopwords = WordsOBJ.Current_Previous_CommentTial_NumberSharingWords_WithoutStopWords(Temp_commentTail, Prev_commentTail)
            ArrayElements(13) = NumberSharingWordsWithoutStopwords.toDouble

          }

        }

        ArrayElements(14) = tempQids
        ArrayElements(15) = temlangs
        ArrayElements(16) = temLinks

        val FacilityOBJ = new FacilitiesClass()
        var vector_Values = FacilityOBJ.ToVector(ArrayElements)
        new_Back_Row = Row(vector_Values)

      } else {

        var RatioValues = new Array[Double](17)
        RatioValues(0) = 0.0
        RatioValues(1) = 0.0
        RatioValues(2) = 0.0
        RatioValues(3) = 0.0
        RatioValues(4) = 0.0
        RatioValues(5) = 0.0
        RatioValues(6) = 0.0
        RatioValues(7) = 0.0
        RatioValues(8) = 0.0
        RatioValues(9) = 0.0
        RatioValues(10) = 0.0
        RatioValues(11) = 0.0
        RatioValues(12) = 0.0
        RatioValues(13) = 0.0
        RatioValues(14) = tempQids
        RatioValues(15) = temlangs
        RatioValues(16) = temLinks

        val FacilityOBJ = new FacilitiesClass()
        val vector_Values = FacilityOBJ.ToVector(RatioValues)
        new_Back_Row = Row(vector_Values)

      }
      new_Back_Row
      //Word_Features
    }

    // Function for Sentences features
    def Sentences_Features(row: Row): Row = {

      //This will be used to save values in vector
      var DoubleValues = new Array[Double](4)

      //1. Row from  partitioned Pair RDD:
      var new_Back_Row = Row()
      //2. Revision ID current operation:
      var RevisionID = row(0)
      //3. row(2) =  represent the Full Comment:
      var CommentRecord_AsString = row(2).toString()
      //4. extract comment tail from the Normal comment-Depending on the paperes, we apply character feature extraction on comment Tail
      val CommentObj = new CommentProcessor()
      val Temp_commentTail = CommentObj.Extract_CommentTail(CommentRecord_AsString)

      if (Temp_commentTail != "" && Temp_commentTail != "NA") {

        // This is CommentTail Feature:-----------------------------------------------------
        val comment_Tail_Length = Temp_commentTail.length().toDouble

        // Feature 1 comment tail length
        DoubleValues(0) = comment_Tail_Length

        // Feature 2 similarity  between comment contain Sitelink and label :
        //Check the language in comment that contain sitelinkword: --------------------
        val Sitelink_inCommentObj = new SentencesFeature()

        if (CommentRecord_AsString.contains("sitelink")) { // start 1 loop
          //1. First step : get the language from comment
          val languagesitelink_from_Comment = Sitelink_inCommentObj.extract_CommentSiteLink_LanguageType(CommentRecord_AsString).trim()

          //2. second step: get  the Label tage from json table :
          if (row(9).toString() != "[]") { // start 2 loop
            // if (row(8).toString() != "") {
            val jsonStr = "\"\"\"" + row(9).toString() + "\"\"\"" // row(9) is the label record
            val jsonObj: JSONObject = new JSONObject(row(9).toString()) // we have here the record which represents Label
            var text_lang = languagesitelink_from_Comment.replace("wiki", "").trim()
            var key_lang = "\"" + text_lang + "\""
            if (jsonStr.contains(""""language"""" + ":" + key_lang)) {
              val value_from_Label: String = jsonObj.getJSONObject(text_lang).getString("value")
              val result = StringUtils.getJaroWinklerDistance(Temp_commentTail, value_from_Label)
              DoubleValues(1) = result
            } else {
              DoubleValues(1) = 0.0
            }
            // }
          } // endd 2 loop 
          else {

            DoubleValues(1) = 0.0

          }
        } // end 1 loop
        else {

          DoubleValues(1) = 0.0

        }

        // Feature 3 similarity between comment contain label word and sitelink
        //Check the language in comment that contain Label word:-----------------------
        val Label_inCommentObj = new SentencesFeature()
        if (CommentRecord_AsString.contains("label")) {
          //1. First step : get the language from comment
          val languageLabel_from_Comment = Label_inCommentObj.extract_CommentLabel_LanguageType(CommentRecord_AsString).trim()
          //2. second step: get  the site link  tage from json table :
          if (row(13).toString() != "[]") { // start 2 loop
            val jsonStr = "\"\"\"" + row(13).toString() + "\"\"\"" // row(13) is the sitelink record
            val jsonObj: JSONObject = new JSONObject(row(13).toString())
            var text_lang = languageLabel_from_Comment + "wiki"
            var key_lang = "\"" + text_lang + "\""
            if (jsonStr.contains(""""site"""" + ":" + key_lang)) {
              val value_from_sitelink: String = jsonObj.getJSONObject(text_lang).getString("title")
              val result = StringUtils.getJaroWinklerDistance(Temp_commentTail, value_from_sitelink)
              DoubleValues(2) = result

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
          DoubleValues(3) = Similarityresult
        } else {
          DoubleValues(3) = 0.0

        }

        val FacilityOBJ = new FacilitiesClass()
        val vector_Values = FacilityOBJ.ToVector(DoubleValues)
        new_Back_Row = Row(vector_Values)
        new_Back_Row

      } else {

        DoubleValues(0) = 0.0
        DoubleValues(1) = 0.0
        DoubleValues(2) = 0.0
        DoubleValues(3) = 0.0

        val FacilityOBJ = new FacilitiesClass()
        val vector_Values = FacilityOBJ.ToVector(DoubleValues)
        new_Back_Row = Row(vector_Values)
        new_Back_Row

      }

      new_Back_Row

    }

    // statement Features :
    def Statement_Features(row: Row): String = {
      var full_Str_Result = ""
      //1. row(2) =  represent the Comment:
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
    def User_Features_Normal(row: Row): Row = {
      var DoubleValues = new Array[Double](10) // you should change the index when add more element feature

      //Row from  partitioned Pair RDD:
      var new_Back_Row = Row()
      //row(7) =  represent the Contributor name:
      var full_comment = row(2).toString()
      var contributor_Name = row(7).toString()
      var contributor_ID = row(6).toString()
      var contributor_IP = row(5).toString()
      if (contributor_Name != "0000") {

        val useFeatureOBJ = new UserFeatures()

        //Is privileged :  There are 5 cases : if one of these cases is true that mean it is privileged else it is not privileged user
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

        //is BotUser : There are 3 cases  :
        var flag_case1_1 = useFeatureOBJ.CheckName_isLocalBotUser(contributor_Name)
        var flag_case2_2 = useFeatureOBJ.CheckName_isGlobalbotUser(contributor_Name)
        var flag_case3_3 = useFeatureOBJ.CheckName_isExtensionBotUser(contributor_Name)

        if (flag_case1_1 == true || flag_case2_2 == true || flag_case3_3 == true) {

          DoubleValues(1) = 1.0

        } else {

          DoubleValues(1) = 0.0
        }

        //is Bot User without BotflagUser : There is 1 case  :
        var flag_BUWBF = useFeatureOBJ.CheckName_isBotUserWithoutBotFlagUser(contributor_Name)

        if (flag_BUWBF == true) {
          DoubleValues(2) = 1.0

        } else {
          DoubleValues(2) = 0.0

        }

        // is Property  creator :
        var flagCreator = useFeatureOBJ.CheckName_isPropertyCreator(contributor_Name)

        if (flagCreator == true) {
          DoubleValues(3) = 1.0

        } else {
          DoubleValues(3) = 0.0

        }

        // is translator :
        var flagTranslator = useFeatureOBJ.CheckName_isTranslator(contributor_Name)
        if (flagTranslator == true) {
          DoubleValues(4) = 1.0
        } else {
          DoubleValues(4) = 0.0
        }

        // is register user:
        var flagRegistered = useFeatureOBJ.IsRegisteroUser(contributor_Name)
        if (flagRegistered == true) {
          DoubleValues(5) = 1.0
        } else {
          DoubleValues(5) = 0.0
        }

      }

      // IP
      if (contributor_IP != "0000") {
        DoubleValues(6) = contributor_IP.toDouble
      } else {
        DoubleValues(6) = 0.0
      }
      // ID

      if (contributor_ID != "0000") {
        DoubleValues(7) = contributor_ID.toDouble
      } else {
        DoubleValues(7) = 0.0
      }

      // BitrthDate  - DeatDate:

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

      val FacilityOBJ = new FacilitiesClass()
      val vector_Values = FacilityOBJ.ToVector(DoubleValues)
      new_Back_Row = Row(vector_Values)
      new_Back_Row
    }

    def Item_Features(row: Row): Row = {

      var DoubleValues = new Array[Double](11)
      //Row from  partitioned Pair RDD:
      var new_Back_Row = Row()
      var ItemOBJ = new ItemFeatures()

      //1. Feature depending on Label:
      var NumberOfLabel = 0.0
      var Label_String = row(9).toString()
      if (Label_String != "[]") {
        NumberOfLabel = ItemOBJ.Get_NumberOfLabels(Label_String)
        DoubleValues(0) = NumberOfLabel
      } else {
        NumberOfLabel = 0.0
        DoubleValues(0) = NumberOfLabel

      }
      //2. Feature depending on Description:
      var Description_String = row(10).toString()
      var NumberOfDescription = 0.0
      if (Description_String != "[]") {
        NumberOfDescription = ItemOBJ.Get_NumberOfDescription(Description_String)
        DoubleValues(1) = NumberOfDescription

      } else {
        NumberOfDescription = 0.0
        DoubleValues(1) = NumberOfDescription

      }
      //3. Feature depending on Aliases:
      var Aliases_String = row(11).toString()
      var NumberOfAliases = 0.0
      if (Aliases_String != "[]") {
        NumberOfAliases = ItemOBJ.Get_NumberOfAliases(Aliases_String)
        DoubleValues(2) = NumberOfAliases

      } else {
        NumberOfAliases = 0.0
        DoubleValues(2) = NumberOfAliases

      }
      //4. Feature depending on Claims :
      var Claims_String = row(12).toString()
      var NumberOfClaims = 0.0
      if (Claims_String != "[]") {
        NumberOfClaims = ItemOBJ.Get_NumberOfClaim(Claims_String)
        DoubleValues(3) = NumberOfClaims

      } else {
        NumberOfClaims = 0.0
        DoubleValues(3) = NumberOfClaims

      }
      //5. Feature depending on SiteLink
      var SiteLink_String = row(13).toString()
      var NumberOfSitelink = 0.0
      if (SiteLink_String != "[]") {
        NumberOfSitelink = ItemOBJ.Get_NumberOfSiteLinks(SiteLink_String)
        DoubleValues(4) = NumberOfSitelink

      } else {
        NumberOfSitelink = 0.0
        DoubleValues(4) = NumberOfSitelink

      }

      //6. Feature depending on Claims - statements :
      var statement_String = row(12).toString() // from claim
      var NumberOfstatement = 0.0
      if (statement_String != "[]") {
        NumberOfstatement = ItemOBJ.Get_NumberOfstatements(statement_String)
        DoubleValues(5) = NumberOfstatement

      } else {
        NumberOfstatement = 0.0
        DoubleValues(5) = NumberOfstatement

      }

      //7. Feature depending on Claims - References  :
      var References_String = row(12).toString() // from claim
      var NumberOfReferences = 0.0
      if (References_String != "[]") {
        NumberOfReferences = ItemOBJ.Get_NumberOfReferences(References_String)
        DoubleValues(6) = NumberOfReferences

      } else {
        NumberOfReferences = 0.0
        DoubleValues(6) = NumberOfReferences

      }
      //8. Feature depending on claim
      var Qualifier_String = row(12).toString() // from claim
      var NumberOfQualifier = 0.0
      if (Qualifier_String != "[]") {
        NumberOfQualifier = ItemOBJ.Get_NumberOfQualifier(Qualifier_String)
        DoubleValues(7) = NumberOfQualifier

      } else {
        NumberOfQualifier = 0.0
        DoubleValues(7) = NumberOfQualifier

      }

      //9. Features depending on  claim
      var Qualifier_String_order = row(12).toString() // from claim
      var NumberOfQualifier_order = 0.0
      if (Qualifier_String_order != "[]") {
        NumberOfQualifier_order = ItemOBJ.Get_NumberOfQualifier_Order(Qualifier_String_order)
        DoubleValues(8) = NumberOfQualifier_order

      } else {
        NumberOfQualifier_order = 0.0
        DoubleValues(8) = NumberOfQualifier_order

      }

      //10. Feature depending on  Site link
      var BadgesString = row(13).toString() // from claim
      var NumberOfBadges = 0.0
      if (BadgesString != "[]") {
        NumberOfBadges = ItemOBJ.Get_NumberOfBadges(BadgesString)
        DoubleValues(9) = NumberOfBadges

      } else {
        NumberOfBadges = 0.0
        DoubleValues(9) = NumberOfBadges

      }

      //11. Item Title (instead of Item  ID)
      var Item_Id_Title = row(1).toString().replace("Q", "")
      var Item = Item_Id_Title.trim().toDouble
      DoubleValues(10) = Item

      val FacilityOBJ = new FacilitiesClass()
      val vector_Values = FacilityOBJ.ToVector(DoubleValues)
      new_Back_Row = Row(vector_Values)
      new_Back_Row
    }

    def Revision_Features(row: Row): String = {

      //var DoubleValues = new Array[Double](6)
      var full_Str_Result = ""
      // Comment Length:---------------------------------------
      //1. Row from  partitioned Pair RDD:
      var new_Back_Row = Row()
      //2. Revision ID current operation:
      var RevisionID = row(0)
      //3. row(2) =  represent the Comment:
      var fullcomment = row(2).toString()
      var length = fullcomment.length()
      // DoubleValues(0) = length

      full_Str_Result = length.toString()

      // Revision Language :---------------------------------------------------------------------------------

      var comment_for_Language = row(2).toString()
      val CommentLanguageOBJ = new RevisionFeatures()
      val language = CommentLanguageOBJ.Extract_Revision_Language(fullcomment)
      full_Str_Result = full_Str_Result + "," + language

      //    // Revision Language  local:----------------------------------------------------------------------------
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

      // Is it Latin Language or Not:-------------------------------------------------------------------------
      val revisionFeatureOBJ = new RevisionFeatures()
      val flagLatin = revisionFeatureOBJ.Check_ContainLanguageLatin_NonLatin(language)

      if (flagLatin == true) {

        full_Str_Result = full_Str_Result + "," + "1.0"

      } else {

        full_Str_Result = full_Str_Result + "," + "0.0"
      }

      // Json Length : be care full to RDD where the json before parsed--------------------------------------
      // var Jason_Text = row(8).toString()

      //replacing_with_Quoto for cleaning the Json tag from extr tags such as <SHA>...
      var Jason_Text = replacing_with_Quoto(row(0).toString(), row(8).toString())
      var Json_Length = Jason_Text.length()

      full_Str_Result = full_Str_Result + "," + Json_Length.toString()

      // Revision Action - SubAction :-----------------------------------------------------------------------
      val CommentProcessOBJ = new CommentProcessor()
      val actions = CommentProcessOBJ.Extract_Actions_FromComments(fullcomment)

      var ActionsArray: Array[String] = actions.split("_", 2)
      var action = ActionsArray(0)
      var SubAction = ActionsArray(1)
      full_Str_Result = full_Str_Result + "," + action.trim()
      full_Str_Result = full_Str_Result + "," + SubAction.trim()

      // ContentType: take Action1 as input : --------------------------------------------------------------

      var CTOBJ = new RevisionFeatures()
      var contentType = CTOBJ.getContentType(action.trim())
      full_Str_Result = full_Str_Result + "," + contentType.trim()

      // Revision Prev-Action  and Prev-SubAction:-------------------------------------------------------------------------------
      if (row(19) != null) {
        var Prev_fullcomment = row(19).toString()
        val Prev_CommentProcessOBJ = new CommentProcessor()
        val Prev_actions = Prev_CommentProcessOBJ.Extract_Actions_FromComments(fullcomment)
        var Prev_ActionsArray: Array[String] = Prev_actions.split("_", 2)
        var Prev_action = ActionsArray(0).trim()
        var Prev_SubAction = ActionsArray(1).trim()
        full_Str_Result = full_Str_Result + "," + Prev_action.trim()
        full_Str_Result = full_Str_Result + "," + Prev_SubAction.trim()

        // println(row(16).toString())
      } else {

        full_Str_Result = full_Str_Result + "," + "NA"
        full_Str_Result = full_Str_Result + "," + "NA"
      }

      // Case IP : RevisionPrev Account user type change :----------------------------------------------------------------------------
      if (row(22) != null) {
        var Prev_Contributor_IP = row(22).toString()

        if (Prev_Contributor_IP != "" && Prev_Contributor_IP != "0000") {

          full_Str_Result = full_Str_Result + "," + Prev_Contributor_IP.trim()

        } else {
          full_Str_Result = full_Str_Result + "," + "NA"

        }

      }

      // Case ID : RevisionPrev Account user type change :----------------------------------------------------------------------------

      if (row(23) != null) {
        var Prev_Contributor_ID = row(23).toString()
        if (Prev_Contributor_ID != "" && Prev_Contributor_ID != "0000") {
          full_Str_Result = full_Str_Result + "," + Prev_Contributor_ID.trim()
        } else {
          full_Str_Result = full_Str_Result + "," + "NA"

        }

      }
      // Revision Parent :-----------------------------------------------------------------------------------------------------
      var RevisionParent = row(3).toString()
      full_Str_Result = full_Str_Result + "," + RevisionParent.toString().trim()

      // Revision hour of Day - Day of week:------------------------------------------------------------------------------------------------

      var RevisionTimeZone = row(4).toString()
      var TimeArray: Array[String] = RevisionTimeZone.split("T", 2)

      //A. Hours of Day
      var HoursOfDay = TimeArray(1).replace("Z", "").trim()
      full_Str_Result = full_Str_Result + "," + HoursOfDay

      //B.Day of Week
      var DayOfWeek = TimeArray(0).replace("Z", "").trim()
      full_Str_Result = full_Str_Result + "," + DayOfWeek

      // Revision Size:------------------------------------------------------------------------------------------------

      var RevisionBody = row(0).toString() + row(2).toString() + row(3).toString() + row(4).toString() + row(8).toString() + row(14).toString() + row(15).toString() + row(16).toString()
      if (row(5).toString() != "0000") {

        RevisionBody = RevisionBody + row(5).toString()
        full_Str_Result = full_Str_Result + "," + RevisionBody.length().toString()

      } else {
        RevisionBody = RevisionBody + row(6).toString() + row(7).toString()
        full_Str_Result = full_Str_Result + "," + RevisionBody.length().toString()

      }

      full_Str_Result

    }

    //========================

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

    def DateToLong(strDate: String): Long = {
      var str = strDate.replace("T", "")
      str = str.replace("z", "").trim()
      val ts1: java.sql.Timestamp = java.sql.Timestamp.valueOf(str)
      val tsTime1: Long = ts1.getTime()

      tsTime1
    }

  }

}