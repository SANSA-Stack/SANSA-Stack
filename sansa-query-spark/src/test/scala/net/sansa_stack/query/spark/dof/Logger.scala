package net.sansa_stack.query.spark.dof

import net.sansa_stack.query.spark.dof.bindings.{ Result, ResultRDD }

class Logger(logAllowed: Boolean) extends Serializable {
  @transient lazy val logger =
    org.apache.log4j.Logger.getLogger(this.getClass.getName)

  def log(result: Result[ResultRDD[String]]): Unit =
    if (logAllowed) result.rdd.foreach { i => logger.warn(i.toString) }

  def log(o: Object): Unit = logger.info(o.toString)
}
