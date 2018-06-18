package net.sansa_stack.inference.utils.graph

import org.jgrapht.graph.DefaultEdge

/**
  * A labeled edge that also keeps trakc of source and target node.
  *
  * @param s     source node
  * @param t     target node
  * @param label the label
  * @author Lorenz Buehmann
  */
case class LabeledEdge[V, L](s: V, t: V, label: L) extends DefaultEdge {}
