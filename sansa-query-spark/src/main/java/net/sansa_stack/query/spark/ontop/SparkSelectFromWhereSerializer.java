package net.sansa_stack.query.spark.ontop;

import com.google.inject.Inject;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.generation.serializer.impl.DefaultSelectFromWhereSerializer;
import it.unibz.inf.ontop.generation.serializer.impl.SQLTermSerializer;

/**
 * @author Lorenz Buehmann
 */
public class SparkSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer {

    @Inject
    protected SparkSelectFromWhereSerializer(SQLTermSerializer sqlTermSerializer) {
        super(sqlTermSerializer);
    }

    @Override
    public QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters) {
        return selectFromWhere.acceptVisitor(
                new DefaultRelationVisitingSerializer(dbParameters.getQuotedIDFactory()) {

                    @Override
                    protected String serializeLimit(long limit) {
                        if (limit < 0) {
                            return "";
                        } else {
                            return String.format("LIMIT %d", limit);
                        }
                    }

                    @Override
                    protected String serializeLimitOffset(long limit, long offset) {
                        if (limit == 0) {
                            return "LIMIT 0";
                        }

                        if (limit < 0) {
                            if (offset <= 0) {
                                return "";
                            } else {
                                return String.format("OFFSET %d ROWS", offset);
                            }
                        } else {
                            if (offset < 0) {
                                // If the offset is not specified
                                return String.format("LIMIT %d", limit);
                            } else {
                                return String.format("OFFSET %d ROWS\nLIMIT %d", offset, limit);
                            }
                        }
                    }

                    @Override
                    protected String serializeOffset(long offset) {
                        if (offset < 0) {
                            // If the offset is not specified
                            return "";
                        } else {
                            return String.format("OFFSET %d ROWS", offset);
                        }
                    }
                }
        );
    }
}
