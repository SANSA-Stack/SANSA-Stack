package net.sansa_stack.ml.spark.anomalydetection

import org.apache.log4j.Logger

object DistADLogger extends Serializable {
  @transient lazy val LOG = Logger.getLogger(getClass.getName)
}
