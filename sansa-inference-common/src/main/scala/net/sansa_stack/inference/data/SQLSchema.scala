package net.sansa_stack.inference.spark.data.model

/**
  * The SQL schema used for RDF triples in a Dataframe.
  *
  * @param triplesTable the name of the triples table
  * @param subjectCol   the name of the subject column
  * @param predicateCol the name of the predicate column
  * @param objectCol    the name of the object column
  *
  * @author Lorenz Buehmann
  */
class SQLSchema(val triplesTable: String, val subjectCol: String, val predicateCol: String, val objectCol: String) {}

object SQLSchemaDefault extends SQLSchema("TRIPLES", "s", "p", "o") {}
