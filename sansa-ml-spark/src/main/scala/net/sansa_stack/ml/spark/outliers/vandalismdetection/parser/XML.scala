package net.sansa_stack.ml.spark.outliers.vandalismdetection.parser

import java.math.BigInteger
import java.net.InetAddress
import java.util.ArrayList
import java.util.regex.{ Matcher, Pattern }
import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.streaming.StreamInputFormat

object XML extends Serializable {

  def parse(spark: SparkSession): RDD[String] = {

    // Streaming records:==================================================================Input Files
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<revision>") // start Tag
    jobConf.set("stream.recordreader.end", "</revision>") // End Tag
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, "hdfs://localhost:9000/mydata/3.xml") // input path from Hadoop

    // read data and save in RDD as block
    val triples = spark.sparkContext.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text]) // .distinct()
    val revisionTagTriples = triples.map { case (x, y) => (x.toString()) }
    
    // Abend the revision in one line string
    val revisionInOneString = revisionTagTriples.map(line => abendRevision(line)).cache()
    val revisions= revisionInOneString.map(line => buildRevision(line)).cache()

    revisions

  }

  // make the revision as one string
  def abendRevision(str: String): String = {
    val s1 = str.replaceAll("[\r\n]+", " ");
    val s2 = s1.replaceAll(">[.\\s]+<", "><");
    // val s3 = s2.replace("\"", "");
    s2
  }

  def buildRevision(obj: String): String = {
    var Store_Record_String = ""
    // Json Revision :
    val JsonStr = getJSONRevision(obj)
    val Standered_JsonStr = standaredGetJSONRevision(obj) // for full string Jason with all formating for parsing by spark
    val Json_Standered = Standered_JsonStr.get(0).toString() // for full string Jason with all formating for parsing by spark
    val Json = JsonStr.get(0).toString()

    // 0.Id Revision
    val IdRevision = getIDOfRevision(obj)
    if (IdRevision != "") {
      val ID = IdRevision.toString().trim()
      Store_Record_String = ID.trim()
    }
    //    else {
    //      Store_Record_String = "0"
    //    }
    // 1. Item Title :
    val ItemTitle: ArrayList[String] = getItemTitleFromJSON(Json)
    if (ItemTitle.size() > 0) {
      val groupItemTilte = ItemTitle.get(0).toString()
      val inputItemTitle: CharSequence = groupItemTilte
      val pattStr_ItemTitle: String = "(id:[Q0-9]*)"
      val p_ItemTilte: Pattern = Pattern.compile(pattStr_ItemTitle)
      val m_ItemTitle: Matcher = p_ItemTilte.matcher(inputItemTitle)
      while ((m_ItemTitle.find())) {
        val repItemID: String = m_ItemTitle.group().toString()
        val recordList_ItemID: Array[String] = repItemID.split(",")
        for (record <- recordList_ItemID) {
          if (record.contains("id")) {
            // value: xxxxxxxxx:
            val parts: Array[String] = record.split(":", 2)
            val record_value = parts(1)
            Store_Record_String = Store_Record_String + "NNLL" + record_value.trim()

          }
        }
      }
    }

    // =============Start:======= extract information from the json string
    // 2.Comments :
    val commentarray = getComment(obj)
    val comment = commentarray.get(0)
    if (comment.nonEmpty) {
      Store_Record_String = Store_Record_String.trim() + "NNLL" + comment.trim()
    } else {
      Store_Record_String = Store_Record_String.trim() + "NNLL" + "NA"
    }

    // 3.Parent ID :
    val ParentIDStr = getParentID(obj)

    if (ParentIDStr.nonEmpty) {
      val ParentID = ParentIDStr
      Store_Record_String = Store_Record_String + "NNLL" + ParentID.trim

    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "0"

    }
    // 4.Timestamp:
    val TimeStamparray = getTIMEStamp(obj)
    val TimeSta = TimeStamparray.get(0)
    if (TimeSta.nonEmpty) {
      Store_Record_String = Store_Record_String + "NNLL" + TimeSta.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "NA"
    }

    // 5. Contributor Data( IP ):
    val Contributstr = getContributorIP(obj)
    // val ContributorSta = Contributorarray.get(0)
    if (Contributstr != "0") {
      Store_Record_String = Store_Record_String + "NNLL" + Contributstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "0"
    }

    // 6. Contributor ID :
    val Contributor_IDStr = getContributorID(obj)
    // val Contributor_IDSta = Contributor_IDarray.get(0)
    if (Contributor_IDStr != "0") {
      Store_Record_String = Store_Record_String + "NNLL" + Contributor_IDStr.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "0"
    }

    // 7. Contributor Name :
    val Contributor_NameStr = getContributorName(obj)
    // val Contributor_IDSta = Contributor_IDarray.get(0)
    if (Contributor_NameStr != "NA") {
      Store_Record_String = Store_Record_String + "NNLL" + Contributor_NameStr.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "NA"
    }

    // 8. Full Json Tag for Parsing:
    if (Json_Standered.nonEmpty) {
      Store_Record_String = Store_Record_String + "NNLL" + Json_Standered.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "NA"
    }

    // 9. Model :

    val modelstr = getModel(obj)
    if (modelstr.nonEmpty) {
      Store_Record_String = Store_Record_String + "NNLL" + modelstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "NA"
    }
    // 10.Format:
    val Formatstr = getFormat(obj)
    if (Formatstr.nonEmpty) {
      Store_Record_String = Store_Record_String + "NNLL" + Formatstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "NA"
    }
    // 11.SHA1 :
    val SHAstr = getSHA1(obj)
    if (SHAstr.nonEmpty) {
      Store_Record_String = Store_Record_String + "NNLL" + SHAstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "NNLL" + "NA"
    }

    Store_Record_String

  }

  def arrayStringToString(arrlist: ArrayList[String]): String = {

    var temp = ""
    for (j <- 0 until arrlist.size()) {
      temp = temp + "&&&" + arrlist.get(j).toString().trim()
    }

    temp
  }

  def getIDOfRevision(str: String): String = {
    var tem = ""

    if (str.contains("</id><parentid>")) {

      val start = str.indexOf("<revision><id>")
      val end = str.indexOf("</id><parentid>")

      var id = str.substring(start, end)
      id = id.replace("<revision><id>", "")

      tem = id.trim()
    } else if (str.contains("</id><timestamp>")) {

      val start = str.indexOf("<revision><id>")
      val end = str.indexOf("</id><timestamp>")

      var id = str.substring(start, end)
      id = id.replace("<revision><id>", "")

      tem = id.trim()

    }

    tem
  }

  // Extract TimeStampe value from  Tag:
  def getTIMEStamp(str: String): ArrayList[String] = {

    val TimeStamp: ArrayList[String] = new ArrayList[String]()
    val inputTime: CharSequence = str
    val pattStr_time: String = "<timestamp>.*</timestamp>"
    val p_time: Pattern = Pattern.compile(pattStr_time)
    val m_time: Matcher = p_time.matcher(inputTime)
    while ((m_time.find())) {
      val repTime: String = m_time.group()
      val cleaned_value1 = repTime.replace("<timestamp>", "")
      val cleaned_value2 = cleaned_value1.replace("</timestamp>", "").trim()
      TimeStamp.add(cleaned_value2.trim())
    }
    TimeStamp
  }
  
  // Extract Json tage from the string object
  def getJSONRevision(str: String): ArrayList[String] = {
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
      //      JsonRevision.add(cleaned_value5.trim())
      JsonRevision.add(replabels)

    }

    JsonRevision

  }

  def standaredGetJSONRevision(str: String): ArrayList[String] = {
    val JsonRevision: ArrayList[String] = new ArrayList[String]()
    val inputJsonStr: CharSequence = str
    val pattStr_JsonStr: String = "</format>.*<sha1>"
    val p_JsonStr: Pattern = Pattern.compile(pattStr_JsonStr)
    val m_Jsonstr: Matcher = p_JsonStr.matcher(inputJsonStr)
    while ((m_Jsonstr.find())) {
      val replabels: String = m_Jsonstr.group()
      JsonRevision.add(replabels)
    }
    JsonRevision
  }

  // extract Item Title from Json string
  def getItemTitleFromJSON(str: String): ArrayList[String] = {

    val Item_Title_FromJason: ArrayList[String] = new ArrayList[String]()
    val mystr = cleaner(str)
    val inputJsonStr: CharSequence = mystr
    val pattStr_JsonStr: String = "id:.*,labels"
    val p_JsonStr: Pattern = Pattern.compile(pattStr_JsonStr)
    val m_Jsonstr: Matcher = p_JsonStr.matcher(inputJsonStr)
    while ((m_Jsonstr.find())) {
      val repItemTitle: String = m_Jsonstr.group()
      Item_Title_FromJason.add(repItemTitle.trim())

    }

    Item_Title_FromJason

  }

  // extract Pairs Labels values from Json String
  def getLabelsFromJSON(str: String): ArrayList[String] = {

    val list_pairs_fromlabels: ArrayList[String] = new ArrayList[String]()
    val inputJsonStr: CharSequence = str
    val pattStr_Label: String = "labels:.*,descriptions"
    val p_label: Pattern = Pattern.compile(pattStr_Label)
    val m_label: Matcher = p_label.matcher(inputJsonStr)
    while ((m_label.find())) {
      val replabels: String = m_label.group()
      val cleaned_value1 = replabels.replace("labels:", "").trim()
      val cleaned_value2 = cleaned_value1.replace(",descriptions", "").trim()
      list_pairs_fromlabels.add(cleaned_value2.trim())

    }
    list_pairs_fromlabels
  }

  def getContributorName(str: String): String = {
    var tem = ""
    if (str.contains("<contributor>")) {

      val inputContributor: CharSequence = str
      val pattStr_Contributor: String = "<contributor><username>.*</username>"
      val p_pattStr_Contributor: Pattern = Pattern.compile(pattStr_Contributor)
      val m_Contributor: Matcher = p_pattStr_Contributor.matcher(inputContributor)
      while ((m_Contributor.find())) {
        val repTime: String = m_Contributor.group()
        val cleaned_value1 = repTime.replace("<contributor><username>", "")
        val cleaned_value2 = cleaned_value1.replace("</username>", "").trim()
        tem = cleaned_value2.toString().trim()

      }
    } else {
      tem = "NA"

    }
    tem
  }
  
  def getContributorIP(str: String): String = {
    var tem = ""
    if (str.contains("<ip>")) {
      val inputContributor: CharSequence = str
      val pattStr_Contributor: String = "<ip>.*</ip>"
      val p_pattStr_Contributor: Pattern = Pattern.compile(pattStr_Contributor)
      val m_Contributor: Matcher = p_pattStr_Contributor.matcher(inputContributor)
      while ((m_Contributor.find())) {
        val repTime: String = m_Contributor.group()
        val cleaned_value1 = repTime.replace("<ip>", "")
        val cleaned_value2 = cleaned_value1.replace("</ip>", "").trim()

        // convert from IP to decimal
        val adress: InetAddress = InetAddress.getByName(cleaned_value2)
        val bytesIPV4: Array[Byte] = adress.getAddress
        ArrayUtils.reverse(bytesIPV4)
        val bigInt: BigInteger = new BigInteger(1, adress.getAddress)
        val longAdress: Long = bigInt.longValue()
        tem = longAdress.toString().trim()
      }

    } else {
      tem = "0"

    }
    tem
  }

  def getContributorID(str: String): String = {
    var tem = ""

    if (!str.contains("<ip>")) {
      val inputContributor_ID: CharSequence = str
      val pattStr_Contributor_ID: String = "</username><id>.*</id></contributor>"
      val p_pattStr_Contributor: Pattern = Pattern.compile(pattStr_Contributor_ID)
      val m_Contributor_ID: Matcher = p_pattStr_Contributor.matcher(inputContributor_ID)
      while ((m_Contributor_ID.find())) {
        val repTime: String = m_Contributor_ID.group()
        val cleaned_value1 = repTime.replace("</username><id>", "")
        val cleaned_value2 = cleaned_value1.replace("</id></contributor>", "").trim()
        tem = cleaned_value2.trim()
      }

    } else {
      tem = "0"

    }

    tem
  }

  def getUserNameofRevisionFromContributor(str: String): String = {
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
  def getIDUserNameOfRevisionFromContributor(str: String): String = {

    val startIndex5 = str.indexOf("<id>");
    val endIndex5 = str.indexOf("</id>");
    val result_Id_UserName = str.substring(startIndex5 + 1, endIndex5);

    if (result_Id_UserName == null || result_Id_UserName.isEmpty()) {
      val error = "Error1 in String for Id User of revision is Empty or Null"
      error
    } else {

      val Id_user_Revision = result_Id_UserName.substring(3).trim()
      Id_user_Revision.trim()
    }
  }
  
  def getComment(str: String): ArrayList[String] = {

    val comment: ArrayList[String] = new ArrayList[String]()
    val inputComment: CharSequence = str
    val pattStr_comment: String = "<comment>.*</comment>"
    val p_comment: Pattern = Pattern.compile(pattStr_comment)
    val m_comment: Matcher = p_comment.matcher(inputComment)
    while ((m_comment.find())) {
      val repcomment: String = m_comment.group()
      val cleaned_value1 = repcomment.replace("<comment>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</comment>", "").trim()

      comment.add(cleaned_value2.trim())

    }
    comment
  }

  def getParentID(str: String): String = {
    var tem = ""
    val ParentIDS: ArrayList[String] = new ArrayList[String]()
    val inputParentID: CharSequence = str
    val pattStr_ParentID: String = "<parentid>.*</parentid>"
    val p_ParentID: Pattern = Pattern.compile(pattStr_ParentID)
    val m_ParentID: Matcher = p_ParentID.matcher(inputParentID)
    while ((m_ParentID.find())) {
      val repcomment: String = m_ParentID.group()
      val cleaned_value1 = repcomment.replace("<parentid>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</parentid>", "").trim()

      ParentIDS.add(cleaned_value2.trim())
      tem = cleaned_value2.trim()
    }

    tem
  }

  def getModel(str: String): String = {
    var tem = ""
    val ParentIDS: ArrayList[String] = new ArrayList[String]()
    val inputParentID: CharSequence = str
    val pattStr_ParentID: String = "<model>.*</model>"
    val p_ParentID: Pattern = Pattern.compile(pattStr_ParentID)
    val m_ParentID: Matcher = p_ParentID.matcher(inputParentID)
    while ((m_ParentID.find())) {
      val repcomment: String = m_ParentID.group()
      val cleaned_value1 = repcomment.replace("<model>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</model>", "").trim()

      ParentIDS.add(cleaned_value2.trim())
      tem = cleaned_value2.trim()
    }

    tem
  }

  def getFormat(str: String): String = {
    var tem = ""
    val ParentIDS: ArrayList[String] = new ArrayList[String]()
    val inputParentID: CharSequence = str
    val pattStr_ParentID: String = "<format>.*</format>"
    val p_ParentID: Pattern = Pattern.compile(pattStr_ParentID)
    val m_ParentID: Matcher = p_ParentID.matcher(inputParentID)
    while ((m_ParentID.find())) {
      val repcomment: String = m_ParentID.group()
      val cleaned_value1 = repcomment.replace("<format>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</format>", "").trim()

      ParentIDS.add(cleaned_value2.trim())
      tem = cleaned_value2.trim()
    }

    tem
  }

  def getSHA1(str: String): String = {
    var tem = ""
    val ParentIDS: ArrayList[String] = new ArrayList[String]()
    val inputParentID: CharSequence = str
    val pattStr_ParentID: String = "<sha1>.*</sha1>"
    val p_ParentID: Pattern = Pattern.compile(pattStr_ParentID)
    val m_ParentID: Matcher = p_ParentID.matcher(inputParentID)
    while ((m_ParentID.find())) {
      val repcomment: String = m_ParentID.group()
      val cleaned_value1 = repcomment.replace("<sha1>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</sha1>", "").trim()

      ParentIDS.add(cleaned_value2.trim())
      tem = cleaned_value2.trim()
    }

    tem
  }
  
  def cleaner(str: String): String = {

    val cleaned_value1 = str.replace("{", "").trim()
    val cleaned_value2 = str.replace("}", "").trim()
    val cleaned_value3 = cleaned_value2.replace("\"", "").trim();

    cleaned_value3.trim()
  }

  // Append Map the content of arraylist in multi string lines :
  def appendMapArrayListinOneString(arrList: ArrayList[String]): String = {

    var temp: String = ""
    for (j <- 0 until arrList.size()) {
      temp = temp + "&&&" + arrList.get(j).toString().trim()
    }
    temp
  }
}
