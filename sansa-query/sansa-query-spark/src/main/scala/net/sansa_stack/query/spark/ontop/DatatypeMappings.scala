package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.model.`type`.{RDFDatatype, TypeFactory}
import it.unibz.inf.ontop.model.vocabulary.XSD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes._


/**
 * Mapping from Ontop RDF datatype to Spark SQL datatype
 *
 * @author Lorenz Buehmann
 */
object DatatypeMappings {
  def apply(typeFactory: TypeFactory): Map[RDFDatatype, DataType] = Map(
    typeFactory.getXsdStringDatatype -> StringType,
    typeFactory.getXsdIntegerDatatype -> IntegerType,
    typeFactory.getXsdDecimalDatatype -> createDecimalType(),
    typeFactory.getXsdDoubleDatatype -> DoubleType,
    typeFactory.getXsdBooleanDatatype -> BooleanType,
    typeFactory.getXsdFloatDatatype -> FloatType,
    typeFactory.getDatatype(XSD.SHORT) -> ShortType,
    typeFactory.getDatatype(XSD.DATE) -> DateType,
    typeFactory.getXsdDatetimeDatatype -> TimestampType,
    typeFactory.getXsdDatetimeStampDatatype -> TimestampType,
    typeFactory.getDatatype(XSD.BYTE) -> ByteType,
    typeFactory.getDatatype(XSD.LONG) -> LongType
  )
}
