package net.sansa_stack.inference.spark.misc

import scala.reflect.ClassTag

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

/**
  * @author Lorenz Buehmann
  */
object TripleLoading {

  case class JenaTriple(s: Node, p: Node, o: Node) // extends Tuple3[Node, Node, Node](s, p, o)

  // alias for the type to convert to and from
  type JenaTripleEncoded = (Node, Node, Node)

  // implicit conversions
  implicit def toEncoded(t: Triple): JenaTripleEncoded = (t.getSubject, t.getPredicate, t.getObject)
  implicit def fromEncoded(e: JenaTripleEncoded): Triple = Triple.create(e._1, e._2, e._3)

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL basic example")
    .getOrCreate()

  implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
    Encoders.tuple[A1, A2, A3](e1, e2, e3)

  implicit def rowToTriple(row: Row): Triple = Triple.create(row.getAs[Node](0), row.getAs[Node](1), row.getAs[Node](2))

  def main(args: Array[String]): Unit = {
    import spark.implicits._

//    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[JenaTriple]

    var tripleDS = spark
      .createDataset(
        Seq[JenaTripleEncoded](
          Triple.create(NodeFactory.createURI("<a>"), NodeFactory.createURI("<p>"), NodeFactory.createURI("<b>")),
          Triple.create(NodeFactory.createURI("<b>"), NodeFactory.createURI("<p>"), NodeFactory.createURI("<c>")),
          Triple.create(NodeFactory.createURI("<a>"), NodeFactory.createURI("<p>"), NodeFactory.createLiteral("lit"))
        )
      )
      .toDF("s", "p", "o")
      .as[JenaTripleEncoded]

    tripleDS.printSchema()
    tripleDS.show(10)

    tripleDS = tripleDS.filter(t => t.getObject.isURI)

    val joinedDS =
      tripleDS.alias("ds1")
        .join(tripleDS.alias("ds2"), $"ds1.o" === $"ds2.s")
        .select($"ds1.s", $"ds1.p", $"ds2.o")
    joinedDS.explain()
    joinedDS.show(10)

    joinedDS.collect().map(rowToTriple _).foreach(println _)


  }

}
