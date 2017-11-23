package xmlpro

import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.spark.sql.Row
import org.apache.spark.sql
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import Array._
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
object scala {

  def main(args: Array[String]) {

    // Spark configuration and Spark context :
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("XMLProcess")
    val sc = new SparkContext(sparkConf)

    // Streaming records:
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<revision>") // start Tag
    jobConf.set("stream.recordreader.end", "</revision>") // End Tag
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, "hdfs://localhost:9000/mydata/Sample3.xml") // input path from Hadoop

    // read data and save in RDD as block
    val wikiData = sc.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])
    println("Hello1" + " " + wikiData.count)
    val RevisionTagewikidata = wikiData.map { case (x, y) => (x.toString()) }
    println("Hello2" + " " + RevisionTagewikidata.count)
    //RevisionTagewikidata.take(7).foreach(println)

    // ABend the revision in one line string
    val RevisioninOneString = RevisionTagewikidata.map(line => abendRevision(line))
    println("Hello13" + " " + RevisioninOneString.count)
    RevisioninOneString.foreach(println)

    //    // Function for build the Property - value
    val RevisionMap = RevisioninOneString.map(line => build_Revision_Map(line))
    println("Hello14" + " " + RevisionMap.count)
    RevisionMap.foreach(println)

    val AbendedArray = RevisionMap.map(line => AbendArrayListinOneString(line))
    println("Hello15" + " " + AbendedArray.count)
    //AbendedArray.foreach(println)

    val FlatedRDDarraylists = AbendedArray.flatMap(line => line.split("\\s+"))
    println("Hello16" + " " + FlatedRDDarraylists.count)
    //    FlatedRDDarraylists.foreach(println)

    val finalRDD = FlatedRDDarraylists.filter(x => !x.isEmpty)
    println("Hello17" + " " + finalRDD.count)
    //    finalRDD.foreach(println)

    //----------------------------DF------------------------------------------
    //  Create SQLContext Object:
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //Create an Encoded Schema in a String Format:
    val schemaString = "RevisionID Property Value"

    //Generate schema:
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

    //Apply Transformation for Reading Data from Text File
    val rowRDD = finalRDD.map(_.split("::")).map(e ⇒ Row(e(0), e(1), e(2)))

    //Apply RowRDD in Row Data based on Schema:
    val RevisionDF = sqlContext.createDataFrame(rowRDD, schema)

    //Store DataFrame Data into Table
    RevisionDF.registerTempTable("PropertyValue")

    //Select Query on DataFrame
    val dfr = sqlContext.sql("SELECT * FROM PropertyValue")
    dfr.show()

    sc.stop();
  }

  //Extract ID revision from tag Tag:
  def Get_ID_Revision(str: String): ArrayList[String] = {

    val IDRevision_Step1: ArrayList[String] = new ArrayList[String]()
    val IDRevision: ArrayList[String] = new ArrayList[String]()
    val inputID: CharSequence = str
    val pattStr_id: String = "<revision><id>.*<timestamp>"
    val p_id: Pattern = Pattern.compile(pattStr_id)
    val m_id: Matcher = p_id.matcher(inputID)
    while ((m_id.find())) {
      val repID: String = m_id.group()
      IDRevision_Step1.add(repID)
    }
    val inputID2: CharSequence = IDRevision_Step1.get(0).toString()
    val pattStr_id2: String = "<revision><id>.*</id>"
    val p_id2: Pattern = Pattern.compile(pattStr_id2)
    val m_id2: Matcher = p_id2.matcher(inputID2)
    while ((m_id2.find())) {
      val repID2: String = m_id2.group()
      val str1 = repID2.replace("<revision><id>", "")
      val str2 = str1.replace("</id>", "").trim()

      IDRevision.add(str2)
    }

    IDRevision
  }

  //Extract TimeStampe value from  Tag:

  def Get_TIMEStamp(str: String): ArrayList[String] = {

    val TimeStamp: ArrayList[String] = new ArrayList[String]()
    val inputTime: CharSequence = str
    val pattStr_time: String = "<timestamp>.*</timestamp>"
    val p_time: Pattern = Pattern.compile(pattStr_time)
    val m_time: Matcher = p_time.matcher(inputTime)
    while ((m_time.find())) {
      val repTime: String = m_time.group()
      val cleaned_value1 = repTime.replace("<timestamp>", "")
      val cleaned_value2 = cleaned_value1.replace("</timestamp>", "").trim()
      TimeStamp.add(cleaned_value2)
    }
    TimeStamp
  }

  // extract Json tage from the string object
  def Get_Json_Revision(str: String): ArrayList[String] = {

    val JsonRevision: ArrayList[String] = new ArrayList[String]()
    val inputJsonStr: CharSequence = str
    val pattStr_JsonStr: String = "</format>.*<sha1>"
    val p_JsonStr: Pattern = Pattern.compile(pattStr_JsonStr)
    val m_Jsonstr: Matcher = p_JsonStr.matcher(inputJsonStr)
    while ((m_Jsonstr.find())) {
      val replabels: String = m_Jsonstr.group()
      val cleaned_value1 = replabels.replace("{", "")
      val cleaned_value2 = cleaned_value1.replace("}", "")
      val cleaned_value3 = cleaned_value2.replace("\"", "")
      val cleaned_value4 = cleaned_value3.replace("</format><textxml:space=preserve>", "")
      val cleaned_value5 = cleaned_value4.replace("<sha1>", "")
      JsonRevision.add(cleaned_value5)
    }

    JsonRevision

  }

  //extract Item ID from Json string
  def Get_Item_ID_FromJson(str: String): String = {

    val ID_Item_FromJason: ArrayList[String] = new ArrayList[String]()
    var x = ""
    val mystr = cleaner(str)
    val inputJsonStr: CharSequence = mystr
    val pattStr_JsonStr: String = "id:.*,labels"
    val p_JsonStr: Pattern = Pattern.compile(pattStr_JsonStr)
    val m_Jsonstr: Matcher = p_JsonStr.matcher(inputJsonStr)
    if ((m_Jsonstr.find())) {
      val replabels: String = m_Jsonstr.group()
      val cleaned_value1 = replabels.replace(",labels", "")
      val cleaned_value2 = cleaned_value1.replace("id:", "")
      ID_Item_FromJason.add(cleaned_value2)
      x = cleaned_value2

    } else {

      x = "NA"
    }
    x

  }

  // extract Pairs Labels values from Json String
  def Get_Labels_FromJson(str: String): ArrayList[String] = {

    val list_pairs_fromlabels: ArrayList[String] = new ArrayList[String]()
    val inputJsonStr: CharSequence = str
    val pattStr_Label: String = "labels:.*,descriptions"
    val p_label: Pattern = Pattern.compile(pattStr_Label)
    val m_label: Matcher = p_label.matcher(inputJsonStr)
    while ((m_label.find())) {
      val replabels: String = m_label.group()
      val cleaned_value1 = replabels.replace("labels:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",descriptions", "").trim()
      list_pairs_fromlabels.add(cleaned_value2)

    }
    list_pairs_fromlabels
  }
  //extract Description from Json File
  def Get_Descri_FromJson(str: String): ArrayList[String] = {

    val list_pairs_fromDescrip: ArrayList[String] = new ArrayList[String]()
    val inputDescription: CharSequence = str
    val pattStr_desc: String = "descriptions:.*,aliases"
    val p_desc: Pattern = Pattern.compile(pattStr_desc)
    val m_desc: Matcher = p_desc.matcher(inputDescription)
    while ((m_desc.find())) {
      val repdesc: String = m_desc.group()
      val cleaned_value1 = repdesc.replace("descriptions:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",aliases", "").trim()
      if (cleaned_value2 == "[]") {
        list_pairs_fromDescrip.add("NA")

      } else {
        list_pairs_fromDescrip.add(cleaned_value2)
      }
    }
    list_pairs_fromDescrip
  }
  // extract Aliases from Json String
  def Get_Alias_FromJson(str: String): ArrayList[String] = {

    val list_pairs_fromAliases: ArrayList[String] = new ArrayList[String]()
    val inputDescription: CharSequence = str
    val pattStr_desc: String = "aliases:.*,claims"
    val p_desc: Pattern = Pattern.compile(pattStr_desc)
    val m_desc: Matcher = p_desc.matcher(inputDescription)
    while ((m_desc.find())) {
      val repdesc: String = m_desc.group()
      val cleaned_value1 = repdesc.replace("aliases:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",claims", "").trim()

      if (cleaned_value2 == "[]") {
        list_pairs_fromAliases.add("NA")

      } else {
        list_pairs_fromAliases.add(cleaned_value2)
      }

    }

    list_pairs_fromAliases

  }

  def Get_Contributor(str: String): String = {
    val reg_contributer = "<contributor><username>.*</username><id>.*</id></contributor>"
    val startIndex3 = str.indexOf("<contributor>");
    val endIndex3 = str.indexOf("</contributor>");
    val result_contributor = str.substring(startIndex3 + 1, endIndex3);
    if (result_contributor == null || result_contributor.isEmpty()) {
      val error = "Error1 in String for  Contributor is Empty or Null"
      error
    } else {
      val contributor_Revision = result_contributor.substring(12).trim()
      contributor_Revision
    }
  }
  def Get_User_NameofRevisionFromcontributor(str: String): String = {

    // user name Revision
    val startIndex4 = str.indexOf("<username>");
    val endIndex4 = str.indexOf("</username>");
    val result_contributor_UserName = str.substring(startIndex4 + 1, endIndex4);
    if (result_contributor_UserName == null || result_contributor_UserName.isEmpty()) {
      val error = "Error1 in String for UseName Contributor is Empty or Null"
      error
    } else {
      val UserName_Revision = result_contributor_UserName.substring(9).trim()
      UserName_Revision
    }

  }
  def Get_Id_UserName_OfRevisionFromContributor(str: String): String = {

    val startIndex5 = str.indexOf("<id>");
    val endIndex5 = str.indexOf("</id>");
    val result_Id_UserName = str.substring(startIndex5 + 1, endIndex5);

    if (result_Id_UserName == null || result_Id_UserName.isEmpty()) {
      val error = "Error1 in String for Id User of revision is Empty or Null"
      error
    } else {

      val Id_user_Revision = result_Id_UserName.substring(3).trim()
      Id_user_Revision
    }
  }
  def Get_Comment(str: String): ArrayList[String] = {

    val comment: ArrayList[String] = new ArrayList[String]()
    val inputComment: CharSequence = str
    val pattStr_comment: String = "<comment>.*</comment>"
    val p_comment: Pattern = Pattern.compile(pattStr_comment)
    val m_comment: Matcher = p_comment.matcher(inputComment)
    while ((m_comment.find())) {
      val repcomment: String = m_comment.group()
      val cleaned_value1 = repcomment.replace("<comment>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</comment>", "").trim()

      comment.add(cleaned_value2)

    }
    comment
  }

  // FlatMap the content of arraylist in multi string lines :
  def AbendArrayListinOneString(arrList: ArrayList[String]): String = {

    var temp: String = ""
    for (j <- 0 until arrList.size()) {
      temp = temp + " " + arrList.get(j).toString().trim()
    }
    temp
  }
  // Build Map revision that includes all info of Revision
  def build_Revision_Map(obj: String): ArrayList[String] = {

    val Map_Revision: ArrayList[String] = new ArrayList[String]()
    val Message: ArrayList[String] = new ArrayList[String]()

    // Id Revision
    val IdRevision = Get_ID_Revision(obj)
    val ID = IdRevision.get(0).toString()
    //=============Start:======= extract information from the json string
    //Json Revision :
    val JsonStr = Get_Json_Revision(obj)
    val Json = JsonStr.get(0).toString()

    //1. ID Item
    val IdItemArray = Get_Item_ID_FromJson(Json)
    //      val IdItem = IdItemArray.get(0).toString().trim()
    val element = ID + "::" + "id_Item" + "::" + IdItemArray
    Map_Revision.add(element)
    //2. Labels Array becuase the lables sometime take multi value with  multi languages
    val Labels: ArrayList[String] = Get_Labels_FromJson(Json)
    if (Labels.size() > 0) {
      val groupLabels = Labels.get(0).toString()
      val inputlabels2: CharSequence = groupLabels
      val pattStr_labels2: String = "(language:[A-Za-z]+,value:[A-Za-z]+)"
      val p_labels2: Pattern = Pattern.compile(pattStr_labels2)
      val m_labels2: Matcher = p_labels2.matcher(inputlabels2)
      var i = 1
      while ((m_labels2.find())) {
        val replabels2: String = m_labels2.group()
        val parts: Array[String] = replabels2.split(":", 2)
        val pairs = parts(1)
        val pairValue: Array[String] = pairs.split(":", 2)
        val finalvalue = pairValue(1)
        val element = ID + "::" + "labels_Value" + (i) + "::" + finalvalue
        Map_Revision.add(element)
        i = i + 1
      }
    } else {

      val element = ID + "::" + "labels_Value" + "::" + "NA"
      Map_Revision.add(element)

    }
    //3. descriptions
    val Description: ArrayList[String] = Get_Descri_FromJson(Json)
    if (Description.size() > 0) {
      if (Description.get(0) != "NA") {
        val groupDescription = Description.get(0).toString()
        val inputdescrip: CharSequence = groupDescription
        val pattStr_Descrip: String = "(language:[A-Za-z]+,value:[A-Za-z]+)"
        val p_Description: Pattern = Pattern.compile(pattStr_Descrip)
        val m_Description: Matcher = p_Description.matcher(inputdescrip)
        var i = 1
        while ((m_Description.find())) {
          val replabels2: String = m_Description.group()
          val parts: Array[String] = replabels2.split(":", 2)
          val pairs = parts(1)
          val pairValue: Array[String] = pairs.split(":", 2)
          val finalvalue = pairValue(1)
          val element = ID + "::" + "descriptions_Value" + (i) + "::" + finalvalue
          Map_Revision.add(element)
          i = i + 1;
        }
      } else {
        val element = ID + "::" + "descriptions_Value" + "::" + "NA"
        Map_Revision.add(element)
      }

    }
    //4. aliases:
    val Aliases: ArrayList[String] = Get_Alias_FromJson(Json)
    if (Aliases.size() > 0) {
      if (Aliases.get(0) != "NA") {
        val groupAlias = Aliases.get(0).toString()
        val inputAliases: CharSequence = groupAlias
        val pattStr_Aliases: String = "(language:[A-Za-z]+,value:[A-Za-z]+)"
        val p_Aliases: Pattern = Pattern.compile(pattStr_Aliases)
        val m_Aliases: Matcher = p_Aliases.matcher(inputAliases)
        var i = 1
        while ((m_Aliases.find())) {
          val replabels2: String = m_Aliases.group()
          val parts: Array[String] = replabels2.split(":", 2)
          val pairs = parts(1)
          val pairValue: Array[String] = pairs.split(":", 2)
          val finalvalue = pairValue(1)
          val element = ID + "::" + "aliases_Value" + (i) + "::" + finalvalue
          Map_Revision.add(element)
          i = i + 1
        }
      } else {
        val element = ID + "::" + "aliases_Value" + "::" + "NA"
        Map_Revision.add(element)
      }
    }
    //      else {
    //
    //        val element = ID + "::" + "aliases_Value" + "::" + "NA"
    //        Map_Revision.add(element)
    //
    //      }

    //5.Claims

    //6.Sitelink:

    //.7 Comments :
    val commentarray = Get_Comment(obj)
    val comment = commentarray.get(0)
    val elementcomment = ID + "::" + "comment" + "::" + comment
    Map_Revision.add(elementcomment)

    Map_Revision
    //    } else {
    //
    //      Message.add("xxNayef")
    //
    //      println(Message.get(0).toString())
    //
    //      Message
    //    }
  }
  //============= Under testing =========================================================
  //    //8. info contributer:
  //    val contributor = Get_Contributor(obj)
  //
  //    if (contributor == null || contributor.isEmpty()) {
  //      val error = "Warning about Empty or Null String (contributor) in Build Revision Map Function"
  //    } else {
  //
  //      //9. usename contributor:
  //      val username = Get_User_NameofRevisionFromcontributor(contributor)
  //      //10. Id User in contributor
  //      val ID_contributor = Get_Id_UserName_OfRevisionFromContributor(contributor)
  //    }
  //
  //    //    val startIndex8 = obj.indexOf("</format>");
  //    //    val endIndex8 = obj.indexOf("<sha1>");
  //    //    val result_json = obj.substring(startIndex8 + 1, endIndex8);
  //    //    val Json_In_Revision = result_json.substring(34).trim();
  //    //    // delete all quotes from the string
  //    //    val withoutQuotes_Json_In_Revision = Json_In_Revision.replace("\"", "");
  //
  //    // id Item  Page...Data Model7
  //    //    val startIndex9 = withoutQuotes_Json_In_Revision.indexOf("id");
  //    //    val endIndex9 = withoutQuotes_Json_In_Revision.indexOf(",labels");
  //    //    val id_Item = withoutQuotes_Json_In_Revision.substring(startIndex9 + 1, endIndex9).substring(2).trim();
  //    //    val Item_Identifier = id_Item;
  //
  //    //TimeStamp Revision...Data Model2
  //    //    val timestamp = Get_TIMEStamp(obj)
  //
  //    //=============================
  //    //contributor revision :
  //    //    val reg_contributer = "<contributor><username>.*</username><id>.*</id></contributor>"
  //    //    val startIndex3 = obj.indexOf("<contributor>");
  //    //    val endIndex3 = obj.indexOf("</contributor>");
  //    //    val result_contributor = obj.substring(startIndex3 + 1, endIndex3);
  //    //    val contributor_Revision = result_contributor.substring(12).trim()
  //    //
  //    //    // user name Revision...Data Model3
  //    //    val startIndex4 = contributor_Revision.indexOf("<username>");
  //    //    val endIndex4 = contributor_Revision.indexOf("</username>");
  //    //    val result_contributor_UserName = contributor_Revision.substring(startIndex4 + 1, endIndex4);
  //    //    val UserName_Revision = result_contributor_UserName.substring(9).trim()
  //    //
  //    //    // Id User Revision......Data Model4
  //    //    val startIndex5 = contributor_Revision.indexOf("<id>");
  //    //    val endIndex5 = contributor_Revision.indexOf("</id>");
  //    //    val result_Id_UserName = contributor_Revision.substring(startIndex5 + 1, endIndex5);
  //    //    val Id_user_Revision = result_Id_UserName.substring(3).trim()
  //    //===================================
  //    //    // Comment Revision ...Data Model5
  //    //    val startIndex6 = obj.indexOf("<comment>");
  //    //    val endIndex6 = obj.indexOf("</comment>");
  //    //    val result_comment = obj.substring(startIndex6 + 1, endIndex6);
  //    //    val comment_In_Revision = result_comment.substring(8).trim()
  //
  //    // Model Revision...Data Model6
  //    val startIndex7 = obj.indexOf("<model>");
  //    val endIndex7 = obj.indexOf("</model>");
  //    val result_model = obj.substring(startIndex7 + 1, endIndex7);
  //    val model_In_Revision = result_model.substring(6).trim()
  //
  //    //======================
  //
  //    //======================
  //

  //
  //    //    //claims...Data Model11
  //    //    val startIndex13 = withoutQuotes_Json_In_Revision.indexOf("claims");
  //    //    val endIndex13 = withoutQuotes_Json_In_Revision.indexOf(",sitelinks");
  //    //    val claims = withoutQuotes_Json_In_Revision.substring(startIndex13, endIndex13);
  //    //
  //    //    //site links...Data Model12
  //    //    val startIndex14 = withoutQuotes_Json_In_Revision.indexOf("sitelinks");
  //    //    val endIndex14 = withoutQuotes_Json_In_Revision.indexOf(getLast(withoutQuotes_Json_In_Revision));
  //    //    val sitelinks = withoutQuotes_Json_In_Revision.substring(startIndex14, endIndex14);
  //
  //    //=============End:======= extract information from the json string
  //    //=============Header Item : ID, Label, Description, Aliases:

  // for extract the sitelink
  def getLast(str: String): Char = {
    val len = str.length();
    val result = str.charAt(len - 1);
    result
  }
  // make the revision as one string
  def abendRevision(str: String): String = {
    val st = str.replaceAll("\\s", "")
    st

  }

  // def clean string from {, } , ""

  def cleaner(str: String): String = {

    val cleaned_value1 = str.replace("{", "").trim()
    val cleaned_value2 = str.replace("}", "").trim()
    val cleaned_value3 = cleaned_value2.replace("\"", "");

    cleaned_value3
  }

}
    
    
    
    
    
    
    

