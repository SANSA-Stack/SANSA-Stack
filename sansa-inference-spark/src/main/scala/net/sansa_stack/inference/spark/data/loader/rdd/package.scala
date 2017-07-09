package net.sansa_stack.inference.spark.data.loader.rdd

import org.apache.hadoop.fs.Path

import net.sansa_stack.inference.utils.{JenaTripleToNTripleString, NTriplesStringToJenaTriple}
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameWriter, SaveMode}

package object rdf {

  /**
    * Adds methods, `ntriples` and `turtle`, to SparkContext that allows to write N-Triples and Turtle files
    */
  implicit class RDFWriter[T](triples: RDD[Triple]) {

    val converter = new JenaTripleToNTripleString()

    def saveAsNTriplesFile(path: String, mode: SaveMode = SaveMode.ErrorIfExists): Unit = {

      val fsPath = new Path(path)
      val fs = fsPath.getFileSystem(triples.sparkContext.hadoopConfiguration)

      mode match {
        case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
        case SaveMode.Overwrite => fs.delete(fsPath, true)
        case SaveMode.ErrorIfExists => sys.error("Given path: " + path + " already exists!!"); sys.exit(1)
        case SaveMode.Ignore => sys.exit()
      }

      triples
        .map(converter) // map to N-Triples string
        .saveAsTextFile(path)
    }

  }

  /**
    * Adds methods, `rdf`, `ntriples` and `turtle`, to SparkContext that allows to read N-Triples and Turtle files
    */
  implicit class RDFReader(sc: SparkContext) {
    /**
      * Load RDF data into an `RDD[Triple]`. Currently, only N-Triples and Turtle syntax are supported
      * @param lang the RDF language (Turtle or N-Triples)
      * @return the RDD
      */
    def rdf(lang: Lang): String => RDD[Triple] = lang match {
      case i if lang == Lang.NTRIPLES => ntriples
      case j if lang == Lang.TURTLE => turtle
      case _ => throw new IllegalArgumentException(s"${lang.getLabel} syntax not supported yet!")
    }

    /**
      * Load RDF data in N-Triples syntax into an `RDD[Triple]`
      * @return the RDD
      */
    def ntriples: String => RDD[Triple] = path =>
      sc
      .textFile(path, 4) // read the text file
      .map(new NTriplesStringToJenaTriple())

    /**
      * Load RDF data in Turtle syntax into an `RDD[Triple]`
      * @return the RDD
      */
    def turtle: String => RDD[Triple] = path =>
      sc
      .textFile(path, 4) // read the text file
      .map(new NTriplesStringToJenaTriple())
  }
}