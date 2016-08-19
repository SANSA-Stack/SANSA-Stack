package org.sansa.inference.utils.graph

import org.jgrapht.graph.DefaultEdge

/**
  * @author Lorenz Buehmann
  */
case class LabeledEdge[V, L](s: V, t: V, label: L) extends DefaultEdge {}
