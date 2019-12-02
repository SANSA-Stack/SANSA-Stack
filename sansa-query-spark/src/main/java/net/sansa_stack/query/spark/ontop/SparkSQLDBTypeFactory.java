package net.sansa_stack.query.spark.ontop;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.model.type.*;
import it.unibz.inf.ontop.model.type.impl.DefaultSQLDBTypeFactory;
import it.unibz.inf.ontop.model.type.impl.NonStringNonNumberNonBooleanNonDatetimeDBTermType;
import it.unibz.inf.ontop.model.type.impl.StringDBTermType;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.vocabulary.XSD;

class SparkDBTypeFactory extends DefaultSQLDBTypeFactory {

    static class SparkStringDBTermType extends StringDBTermType {

        protected SparkStringDBTermType(String name, TermTypeAncestry parentAncestry, RDFDatatype xsdStringDatatype) {
            super(name, parentAncestry, xsdStringDatatype);
        }
        protected SparkStringDBTermType(String name, String castName, TermTypeAncestry parentAncestry,
                                        RDFDatatype xsdStringDatatype) {
            super(name, castName, parentAncestry, xsdStringDatatype);
        }

    }

    public static final String TEXT_STR = "STRING";

    @AssistedInject
    private SparkDBTypeFactory(@Assisted TermType rootTermType, @Assisted TypeFactory typeFactory) {
        super(createSparkSQLTypeMap(rootTermType, typeFactory), createSparkSQLCodeMap());
    }

    private static Map<String, DBTermType> createSparkSQLTypeMap(TermType rootTermType, TypeFactory typeFactory) {
        Map<String, DBTermType> map = createDefaultSQLTypeMap(rootTermType, typeFactory);

        DBTermType rootDBType = map.get(DefaultSQLDBTypeFactory.ABSTRACT_DB_TYPE_STR);

        TermTypeAncestry rootAncestry = rootDBType.getAncestry();

        RDFDatatype xsdString = typeFactory.getXsdStringDatatype();
        map.put(TEXT_STR, new SparkStringDBTermType(TEXT_STR, rootAncestry, xsdString));

        RDF factory = new SimpleRDF();

        RDFDatatype xsdNonNegInt = typeFactory.getDatatype(factory.createIRI(XSDDatatype.XSDnonNegativeInteger.getURI()));
        map.put(TEXT_STR, new SparkStringDBTermType(TEXT_STR, rootAncestry, xsdString));
        return map;
    }

    private static ImmutableMap<DefaultTypeCode, String> createSparkSQLCodeMap() {
        Map<DefaultTypeCode, String> map = createDefaultSQLCodeMap();
        map.put(DefaultTypeCode.STRING, TEXT_STR);
        return ImmutableMap.copyOf(map);
    }

    @Override
    public Optional<String> getDBNaNLexicalValue() {
        return Optional.empty();
    }
}