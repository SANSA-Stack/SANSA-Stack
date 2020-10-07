package net.sansa_stack.ml.spark.outliers.vandalismdetection.parser

import java.math.BigInteger
import java.net.InetAddress
import java.util.ArrayList
import java.util.regex.{ Matcher, Pattern }

import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.streaming.StreamInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import net.sansa_stack.ml.common.outliers.vandalismdetection.feature.Utils._

object XML extends Serializable {

  def parse(input: String, spark: SparkSession): RDD[String] = {

    // Streaming records:==================================================================Input Files
    val jobConf = new JobConf()

    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<revision>") // start Tag
    jobConf.set("stream.recordreader.end", "</revision>") // End Tag

    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, input)

    // read data and save in RDD as block
    val triples = spark.sparkContext.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat], classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text]) // .distinct()

    val revisionTagTriples = triples.map { case (x, y) => (x.toString()) }

    val revisionInOneString = revisionTagTriples.map(line => abendRevision(line)).filter(line => line.nonEmpty)

    val revisions = revisionInOneString.map(line => buildRevision(line))

    revisions
  }

  // make the revision as one string
  def abendRevision(str: String): String = {

    val s1 = str.replaceAll("[\r\n]+", " ");
    val s2 = s1.replaceAll(">[.\\s]+<", "><");
    s2
  }

  def buildRevision(obj: String): String = {
    var Store_Record_String = ""

    // Json Revision :
    // By this 2 line we extract the Json Tag info by Pattern ans array includes one element : Between "</format>.*<sha1>"
    val Json = getJSONRevision(obj)

    // 0.Id Revision
    val IdRevision = getIDOfRevision(obj)
    if (IdRevision != "") {
      val ID = IdRevision.toString().trim()
      Store_Record_String = ID.trim()
    } else {
      Store_Record_String = "0"
    }

    // =============Start:======= extract information from the json string
    // 1.Comments :
    val commentstr = getComment(obj)

    if (commentstr != "") {
      Store_Record_String = Store_Record_String.trim() + "<1VandalismDetector2>" + commentstr.trim()
    } else {
      Store_Record_String = Store_Record_String.trim() + "<1VandalismDetector2>" + "NA"
    }

    // 2.Parent ID :
    val ParentIDStr = getParentID(obj)

    if (ParentIDStr != "") {
      val ParentID = ParentIDStr
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + ParentID.trim

    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "0"

    }

    // 3.Timestamp:
    val TimeStr = getTIMEStamp(obj)
    if (TimeStr != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + TimeStr.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "NA"
    }

    // 4. Contributor Data( IP ):
    val ContributstrIP = getContributorIP(obj)
    if (ContributstrIP != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + ContributstrIP.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "0"
    }

    // 5. Contributor ID :
    val Contributor_IDStr = getContributorID(obj)
    if (Contributor_IDStr != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + Contributor_IDStr.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "0"
    }

    // 6. Contributor Name :
    val Contributor_NameStr = getContributorName(obj)
    if (Contributor_NameStr != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + Contributor_NameStr.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "NA"
    }

    // 7. Full Json Tag for Parsing:
    if (Json != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + Json.trim()

    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "NA"
    }

    // 8. Model :
    val modelstr = getModel(obj)
    if (modelstr != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + modelstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "NA"
    }

    // 9.Format:
    val Formatstr = getFormat(obj)
    if (Formatstr != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + Formatstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "NA"
    }
    // 10.SHA1 :
    val SHAstr = getSHA1(obj)
    if (SHAstr != "") {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + SHAstr.trim()
    } else {
      Store_Record_String = Store_Record_String + "<1VandalismDetector2>" + "NA"
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
    var tem = "0"
    if (str.contains("</id><parentid>")) {

      val start = str.indexOf("<revision><id>")
      val end = str.indexOf("</id><parentid>")
      if (start != -1 && end != -1) {
        var id = str.substring(start, end)
        id = id.replace("<revision><id>", "")
        tem = id.trim()
      }
    } else if (str.contains("</id><timestamp>")) {

      val start = str.indexOf("<revision><id>")
      val end = str.indexOf("</id><timestamp>")

      if (start != -1 && end != -1) {
        var id = str.substring(start, end)
        id = id.replace("<revision><id>", "")
        tem = id.trim()
      }
    }
    tem
  }

  // Extract TimeStampe value from  Tag
  def getTIMEStamp(str: String): String = {

    var tem = ""
    val inputTime: CharSequence = str
    val pattStr_time: String = "<timestamp>.*</timestamp>"
    val p_time: Pattern = Pattern.compile(pattStr_time)
    val m_time: Matcher = p_time.matcher(inputTime)
    while ((m_time.find())) {
      val repTime: String = m_time.group()
      val cleaned_value1 = repTime.replace("<timestamp>", "")
      val cleaned_value2 = cleaned_value1.replace("</timestamp>", "").trim()
      tem = cleaned_value2.trim()
    }
    tem
  }

  def getJSONRevision(str: String): String = {
    var tem = ""
    val start = str.indexOf("</format><text")
    val end = str.indexOf("<sha1>")
    if (start != -1 && end != -1) {
      var Json = str.substring(start, end)
      tem = Json.trim()
    }
    tem
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

  def getContributorName(str: String): String = {

    var tem = ""

    if (str.contains("<contributor>") && !str.contains("<ip>")) {
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

    }

    tem

  }

  def getContributorID(str: String): String = {
    var tem = ""
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
    tem
  }

  def getComment(str: String): String = {
    var tem = ""
    val pattStr_comment: String = "<comment>.*</comment>"
    val p_comment: Pattern = Pattern.compile(pattStr_comment)
    val m_comment: Matcher = p_comment.matcher(str)
    while ((m_comment.find())) {
      val repcomment: String = m_comment.group()
      val cleaned_value1 = repcomment.replace("<comment>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</comment>", "").trim()

      tem = cleaned_value2
    }

    tem
  }

  def getParentID(str: String): String = {
    var tem = ""
    if (str.contains("</id><parentid>")) {
      val start = str.indexOf("<parentid>")
      val end = str.indexOf("</parentid>")

      if (start != -1 && end != -1) {

        var parentid = str.substring(start, end)
        var parent = parentid.replace("<parentid>", "")
        tem = parent.trim()
      }
    }
    tem
  }

  def getModel(str: String): String = {
    var tem = ""
    val inputParentID: CharSequence = str
    val pattStr_ParentID: String = "<model>.*</model>"
    val p_ParentID: Pattern = Pattern.compile(pattStr_ParentID)
    val m_ParentID: Matcher = p_ParentID.matcher(inputParentID)
    while ((m_ParentID.find())) {
      val repcomment: String = m_ParentID.group()
      val cleaned_value1 = repcomment.replace("<model>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</model>", "").trim()
      tem = cleaned_value2.trim()
    }
    tem
  }

  def getFormat(str: String): String = {
    var tem = ""
    val inputParentID: CharSequence = str
    val pattStr_ParentID: String = "<format>.*</format>"
    val p_ParentID: Pattern = Pattern.compile(pattStr_ParentID)
    val m_ParentID: Matcher = p_ParentID.matcher(inputParentID)
    while ((m_ParentID.find())) {
      val repcomment: String = m_ParentID.group()
      val cleaned_value1 = repcomment.replace("<format>", "").trim()
      val cleaned_value2 = cleaned_value1.replace("</format>", "").trim()

      tem = cleaned_value2.trim()
    }

    tem
  }

  def getSHA1(str: String): String = {
    var tem = ""
    val start = str.indexOf("<sha1>")
    val end = str.indexOf("</sha1>")
    if (start != -1 && end != -1) {

      var sha = str.substring(start, end)
      sha = sha.replace("<sha1>", "")
      tem = sha.trim()
    }
    tem
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
