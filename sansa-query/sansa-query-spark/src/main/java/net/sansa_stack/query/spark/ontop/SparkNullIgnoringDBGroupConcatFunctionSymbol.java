package net.sansa_stack.query.spark.ontop;

import javax.annotation.Nonnull;

import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbolSerializer;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.NullIgnoringDBGroupConcatFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;

/**
 * @author Lorenz Buehmann
 */
public class SparkNullIgnoringDBGroupConcatFunctionSymbol extends NullIgnoringDBGroupConcatFunctionSymbol {
    protected SparkNullIgnoringDBGroupConcatFunctionSymbol(@Nonnull DBTermType dbStringType,
                                                           boolean isDistinct,
                                                           @Nonnull DBFunctionSymbolSerializer serializer) {
        super(dbStringType, isDistinct, serializer);
    }
}
