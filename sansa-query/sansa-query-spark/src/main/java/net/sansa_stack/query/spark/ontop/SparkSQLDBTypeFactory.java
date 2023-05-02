package net.sansa_stack.query.spark.ontop;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableMap;
import it.unibz.inf.ontop.model.type.*;
import it.unibz.inf.ontop.model.type.impl.DBTermTypeImpl;
import it.unibz.inf.ontop.model.type.impl.DefaultSQLDBTypeFactory;
import it.unibz.inf.ontop.model.type.impl.StringDBTermType;
import it.unibz.inf.ontop.model.vocabulary.XSD;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.datatypes.xsd.XSDDatatype;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;

import static it.unibz.inf.ontop.model.type.DBTermType.Category.FLOAT_DOUBLE;
import static it.unibz.inf.ontop.model.type.DBTermType.Category.INTEGER;

class SparkDBTypeFactory extends DefaultSQLDBTypeFactory {

    static class SparkStringDBTermType extends StringDBTermType {
        protected SparkStringDBTermType(String name, String castName, TermTypeAncestry parentAncestry,
                                        RDFDatatype xsdStringDatatype) {
            super(name, castName, parentAncestry, xsdStringDatatype);
        }
    }

    /**
     * Copied from Ontop because of protected access only.
     */
    static class NumberDBTermType extends DBTermTypeImpl {
        private final String castName;
        @Nullable
        private final RDFDatatype rdfDatatype;
        private final Category category;

        protected NumberDBTermType(String name, TermTypeAncestry parentAncestry, RDFDatatype rdfDatatype, Category category) {
            super(name, parentAncestry, false, category);
            this.rdfDatatype = rdfDatatype;
            this.castName = name;
            this.category = category;
        }

        public Optional<RDFDatatype> getNaturalRDFDatatype() {
            return Optional.ofNullable(this.rdfDatatype);
        }

        public boolean isNeedingIRISafeEncoding() {
            return false;
        }

        public boolean areEqualitiesStrict() {
            return this.category == Category.INTEGER;
        }

        public Optional<Boolean> areEqualitiesStrict(DBTermType otherType) {
            return Optional.of(otherType.getCategory() == Category.INTEGER);
        }

        public boolean areEqualitiesBetweenTwoDBAttributesStrict() {
            return true;
        }

        public String getCastName() {
            return this.castName;
        }
    }

    protected static final String TEXT_STR = "STRING";
    protected static final String BYTE_STR = "BYTE";
    protected static final String SHORT_STR = "SHORT";
    protected static final String LONG_STR = "LONG";
    protected static final String DEC_STR = "DEC";

    @AssistedInject
    private SparkDBTypeFactory(@Assisted TermType rootTermType, @Assisted TypeFactory typeFactory) {
        super(createSparkSQLTypeMap(rootTermType, typeFactory), createSparkSQLCodeMap());
    }

    private static Map<String, DBTermType> createSparkSQLTypeMap(TermType rootTermType, TypeFactory typeFactory) {
        Map<String, DBTermType> map = createDefaultSQLTypeMap(rootTermType, typeFactory);

        DBTermType rootDBType = map.get(DefaultSQLDBTypeFactory.ABSTRACT_DB_TYPE_STR);

        TermTypeAncestry rootAncestry = rootDBType.getAncestry();

        DBTermType byteType = new NumberDBTermType(BYTE_STR, rootAncestry, typeFactory.getDatatype(XSD.BYTE), INTEGER);
        DBTermType shortType = new NumberDBTermType(SHORT_STR, rootAncestry, typeFactory.getDatatype(XSD.SHORT), INTEGER);
        DBTermType intType = new NumberDBTermType(INT_STR, rootAncestry, typeFactory.getDatatype(XSD.INT), INTEGER);
        DBTermType longType = new NumberDBTermType(LONG_STR, rootAncestry, typeFactory.getDatatype(XSD.LONG), INTEGER);
        DBTermType floatType = new NumberDBTermType(FLOAT_STR, rootAncestry, typeFactory.getDatatype(XSD.FLOAT), FLOAT_DOUBLE);

        RDFDatatype xsdString = typeFactory.getXsdStringDatatype();
        map.put(TEXT_STR, new SparkStringDBTermType(TEXT_STR, "string", rootAncestry, xsdString));
//        map.put(TEXT_STR, new SparkStringDBTermType(TEXT_STR, "string", rootAncestry, typeFactory.getDatatype(XSD.STRING)));
        map.put(BYTE_STR,byteType);
        map.put(TINYINT_STR,byteType);
        map.put(SHORT_STR,shortType);
        map.put(SMALLINT_STR,shortType);
        map.put(INT_STR,intType);
        map.put(INTEGER_STR,intType);
        map.put(LONG_STR,longType);
        map.put(BIGINT_STR,longType);
        map.put(FLOAT_STR,floatType);
        map.put(REAL_STR,floatType);
        map.put(DEC_STR, map.get(DECIMAL_STR));

        RDF factory = new SimpleRDF();

        RDFDatatype xsdNonNegInt = typeFactory.getDatatype(factory.createIRI(XSDDatatype.XSDnonNegativeInteger.getURI()));
//        map.put(TEXT_STR, new SparkStringDBTermType(TEXT_STR, "string", rootAncestry, xsdString));
        return map;
    }

    @Override
    public DBTermType getDBStringType() {
        return super.getDBStringType();
    }

    private static ImmutableMap<DefaultTypeCode, String> createSparkSQLCodeMap() {
        Map<DefaultTypeCode, String> map = createDefaultSQLCodeMap();
        map.put(DefaultTypeCode.STRING, TEXT_STR);
        map.put(DefaultTypeCode.HEXBINARY, BINARY_STR);
        return ImmutableMap.copyOf(map);
    }

    @Override
    public Optional<String> getDBNaNLexicalValue() {
        return Optional.empty();
    }
}