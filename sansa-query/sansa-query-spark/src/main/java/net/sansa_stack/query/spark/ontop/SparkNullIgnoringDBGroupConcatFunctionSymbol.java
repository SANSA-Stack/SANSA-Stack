package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbolSerializer;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.NullIgnoringDBGroupConcatFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;

import javax.annotation.Nonnull;

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
