package org.dissect.rdf.spark.model

import com.hp.hpl.jena.graph.{ Triple => JTriple }
import com.hp.hpl.jena.graph.{ Node => JNode }

case class Triple(s: JNode, p: JNode, o: JNode) extends JTriple(s, p, o) with  Serializable