package org.sansa.inference.utils.graph

import org.jgrapht.graph.DefaultEdge

/**
  * @author Lorenz Buehmann
  */
case class LabeledEdge[V](s: V, t: V, label: String) extends DefaultEdge {}
