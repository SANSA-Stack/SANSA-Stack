package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.iq.node.VariableNullability;
import it.unibz.inf.ontop.model.term.Constant;
import it.unibz.inf.ontop.model.term.ImmutableTerm;
import it.unibz.inf.ontop.model.term.IncrementalEvaluation;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.AbstractDBConcatFunctionSymbol;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.Serializers;
import it.unibz.inf.ontop.model.type.DBTermType;

public class NullToleratingDBConcatFunctionSymbol extends AbstractDBConcatFunctionSymbol {

    protected NullToleratingDBConcatFunctionSymbol(String nameInDialect, int arity, DBTermType dbStringType,
                                                   DBTermType rootDBTermType, boolean isOperator) {
        super(nameInDialect, arity, dbStringType, rootDBTermType,
                isOperator
                        ? Serializers.getOperatorSerializer(nameInDialect)
                        : Serializers.getRegularSerializer(nameInDialect));
    }

    @Override
    public boolean isAlwaysInjectiveInTheAbsenceOfNonInjectiveFunctionalTerms() {
        return false;
    }

    @Override
    public boolean canBePostProcessed(ImmutableList<? extends ImmutableTerm> arguments) {
        return false;
    }

    @Override
    protected boolean tolerateNulls() {
        return true;
    }

    /**
     * Never returns NULL
     */
    public IncrementalEvaluation evaluateIsNotNull(ImmutableList<? extends ImmutableTerm> terms, TermFactory termFactory,
                                                   VariableNullability variableNullability) {
        return IncrementalEvaluation.declareIsTrue();
    }

    @Override
    protected String extractString(Constant constant) {
        if (constant.isNull())
            throw new MinorOntopInternalBugException("Was expecting a non-null constant. Should be reached this point");
        return constant.getValue();
    }
}