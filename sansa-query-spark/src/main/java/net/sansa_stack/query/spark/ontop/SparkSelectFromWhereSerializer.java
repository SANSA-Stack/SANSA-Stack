package net.sansa_stack.query.spark.ontop;

import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.generation.dialect.SQLDialectAdapter;
import it.unibz.inf.ontop.answering.reformulation.generation.serializer.SQLTermSerializer;
import it.unibz.inf.ontop.answering.reformulation.generation.serializer.impl.DefaultSelectFromWhereSerializer;

/**
 * @author Lorenz Buehmann
 */
public class SparkSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer {

    @Inject
    protected SparkSelectFromWhereSerializer(SQLTermSerializer sqlTermSerializer, SQLDialectAdapter dialectAdapter) {
        super(sqlTermSerializer, dialectAdapter);
    }
}
