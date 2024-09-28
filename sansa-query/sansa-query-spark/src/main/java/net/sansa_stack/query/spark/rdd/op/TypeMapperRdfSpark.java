package net.sansa_stack.query.spark.rdd.op;

import org.aksw.jena_sparql_api.rdf.collections.NodeMapper;
import org.aksw.jena_sparql_api.rdf.collections.NodeMapperDelegating;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.vocabulary.XSD;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.Calendar;
import java.util.Map;

public class TypeMapperRdfSpark {
    /**
     * Function that returns a registry of custom rdf datatype to Spark mappings.
     * <p>
     * FIXME Convert this method into a proper registry:
     *   A singleton instance that wraps the map and upon initialization adds the default registrations
     *
     * @return
     */
    public static Map<String, NodeToSparkMapper> getNodeToSparkMapperRegistry() {
        Map<String, NodeToSparkMapper> registry = new java.util.HashMap<>();

        TypeMapper typeMapper = TypeMapper.getInstance();

        // Mapping of xsd:date to DataTypes.DateType
        {
            String datatypeIri = XSD.date.getURI();
            DataType dataType = DataTypes.DateType;
            RDFDatatype rdfDatatype = typeMapper.getSafeTypeByName(datatypeIri);
            NodeMapper<Object> nodeMapper = NodeMapperDelegating.<Object>create(
                    java.sql.Date.class,
                    x -> x.isLiteral() && x.getLiteralDatatype() != null && XSDDateTime.class.equals(x.getLiteralDatatype().getJavaClass()),
                    x -> NodeFactory.createLiteralByValue(sqlDateToCalendar((java.sql.Date)x), rdfDatatype),
                    x -> new java.sql.Date(((XSDDateTime)x.getLiteralValue()).asCalendar().getTimeInMillis())
            );
            NodeToSparkMapper nodeToSparkMapper = new NodeToSparkMapperImpl(dataType, nodeMapper);
            registry.put(datatypeIri, nodeToSparkMapper);
        }
        return registry;
    }

    public static Calendar sqlTimestampToCalendar(java.sql.Timestamp timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp.getTime());
        return calendar;
    }

    public static Calendar sqlDateToCalendar(java.sql.Date timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp.getTime());
        return calendar;
    }
}
