package net.sansa_stack.rdf.common.partition.core

case class RdfPartitionStateDefault(subjectType: Byte,
                                    predicate: String,
                                    objectType: Byte,
                                    datatype: String,
                                    langTagPresent: Boolean,
                                    lang: Option[String])