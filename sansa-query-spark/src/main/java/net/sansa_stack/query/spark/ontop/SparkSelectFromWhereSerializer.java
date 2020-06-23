package net.sansa_stack.query.spark.ontop;

import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.answering.reformulation.generation.dialect.SQLDialectAdapter;
import it.unibz.inf.ontop.answering.reformulation.generation.serializer.SQLTermSerializer;
import it.unibz.inf.ontop.answering.reformulation.generation.serializer.impl.DefaultSelectFromWhereSerializer;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.dbschema.QuotedIDFactoryStandardSQL;
import it.unibz.inf.ontop.dbschema.impl.BasicDBParametersImpl;

/**
 * @author Lorenz Buehmann
 */
public class SparkSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer {

    @Inject
    protected SparkSelectFromWhereSerializer(SQLTermSerializer sqlTermSerializer, SQLDialectAdapter dialectAdapter) {
        super(sqlTermSerializer, dialectAdapter);
    }
}
