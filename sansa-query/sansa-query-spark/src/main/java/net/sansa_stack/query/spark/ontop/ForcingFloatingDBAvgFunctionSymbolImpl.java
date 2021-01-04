package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbolSerializer;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.NullIgnoringDBAvgFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class ForcingFloatingDBAvgFunctionSymbolImpl extends NullIgnoringDBAvgFunctionSymbol {

    private static final String TEMPLATE = "AVG(1.0*%s)";
    private static final String DISTINCT_TEMPLATE = "AVG(DISTINCT(1.0*%s))";


    protected ForcingFloatingDBAvgFunctionSymbolImpl(@Nonnull DBTermType inputDBType, @Nonnull DBTermType targetDBType,
                                                     boolean isDistinct) {
        super(inputDBType, targetDBType, isDistinct, isDistinct
                ? getDistinctAggregationSerializer()
                : getRegularSerializer());
    }

    public static DBFunctionSymbolSerializer getRegularSerializer() {
        return (terms, termConverter, termFactory) -> {
            String parameterString = terms.stream()
                    .map(termConverter)
                    .collect(Collectors.joining(","));
            return String.format(TEMPLATE, parameterString);
        };
    }

    public static DBFunctionSymbolSerializer getDistinctAggregationSerializer() {
        return (terms, termConverter, termFactory) -> {
            String parameterString = terms.stream()
                    .map(termConverter)
                    .collect(Collectors.joining(","));
            return String.format(DISTINCT_TEMPLATE, parameterString);
        };
    }
}