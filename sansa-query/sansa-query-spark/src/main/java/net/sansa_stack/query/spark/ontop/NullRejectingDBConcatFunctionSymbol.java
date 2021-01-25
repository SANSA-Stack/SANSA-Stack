package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.model.term.Constant;
import it.unibz.inf.ontop.model.term.ImmutableTerm;
import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbolSerializer;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.AbstractDBConcatFunctionSymbol;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.Serializers;
import it.unibz.inf.ontop.model.type.DBTermType;

public class NullRejectingDBConcatFunctionSymbol extends AbstractDBConcatFunctionSymbol {


    protected NullRejectingDBConcatFunctionSymbol(String nameInDialect, int arity, DBTermType dbStringType,
                                                  DBTermType rootDBTermType, boolean isOperator) {
        super(nameInDialect, arity, dbStringType, rootDBTermType,
                isOperator
                        ? Serializers.getOperatorSerializer(nameInDialect)
                        : Serializers.getRegularSerializer(nameInDialect));
    }

    protected NullRejectingDBConcatFunctionSymbol(String nameInDialect, int arity, DBTermType dbStringType,
                                                  DBTermType rootDBTermType, DBFunctionSymbolSerializer serializer) {
        super(nameInDialect, arity, dbStringType, rootDBTermType, serializer);
    }

    @Override
    protected String extractString(Constant constant) {
        if (constant.isNull())
            throw new MinorOntopInternalBugException("Was expecting a non-null constant. Should be reached this point");
        return constant.getValue();
    }

    @Override
    public boolean isAlwaysInjectiveInTheAbsenceOfNonInjectiveFunctionalTerms() {
        return false;
    }

    /**
     * TODO: allow post-processing
     */
    @Override
    public boolean canBePostProcessed(ImmutableList<? extends ImmutableTerm> arguments) {
        return false;
    }
}