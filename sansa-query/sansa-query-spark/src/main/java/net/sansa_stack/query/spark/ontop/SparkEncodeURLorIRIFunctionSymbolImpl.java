package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.MySQLEncodeURLorIRIFunctionSymbolImpl;
import it.unibz.inf.ontop.model.type.DBTermType;

public class SparkEncodeURLorIRIFunctionSymbolImpl extends MySQLEncodeURLorIRIFunctionSymbolImpl {
    protected SparkEncodeURLorIRIFunctionSymbolImpl(DBTermType dbStringType, boolean preserveInternationalChars) {
        super(dbStringType, preserveInternationalChars);
    }
}
