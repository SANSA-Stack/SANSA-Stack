package net.sansa_stack.owl.common.parsing

case class ParserException (
                        private val msg: String = "",
                        private val cause: Throwable = None.orNull)
  extends Exception(msg, cause)
