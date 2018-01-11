package net.sansa_stack.rdf.spark.graph

/*
 * Edge - keeps edges of a graph. 
 */
class EdgeAtt(val nodeID: Long, val edgeAtt: String) extends Serializable {

  //def hasNodeID: Boolean = null match { case _ => false }

  override def toString() = "Edge [nodeID=" + nodeID + ", edgeAtt=" + edgeAtt + "]"

  override def equals(other: Any): Boolean =
    other match {
      case that: EdgeAtt => (that isEqual (this)) && (this.edgeAtt == that.edgeAtt && this.nodeID == that.nodeID)
      case _ => false
    }
 
  final def isEqual(other: Any): Boolean = other.isInstanceOf[EdgeAtt]

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (edgeAtt == null) 0 else edgeAtt.hashCode)
    //result = prime * result + (if (nodeID == null) 0 else nodeID.hashCode)
    result
  }

}