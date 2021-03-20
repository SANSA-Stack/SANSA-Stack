package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.model.type.RDFDatatype;
import it.unibz.inf.ontop.model.type.TermTypeAncestry;
import it.unibz.inf.ontop.model.type.impl.NonStringNonNumberNonBooleanNonDatetimeDBTermType;

public class SparkGeometryDBTermType extends NonStringNonNumberNonBooleanNonDatetimeDBTermType {
    protected SparkGeometryDBTermType(String name, TermTypeAncestry parentAncestry, RDFDatatype rdfDatatype) {
        super(name, parentAncestry, rdfDatatype, true);
    }
}
