package net.sansa_stack.query.spark.ontop;

import com.google.inject.Inject;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableMap;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableSortedSet;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.dbschema.impl.MySQLCaseSensitiveTableNamesQuotedIDFactory;
import it.unibz.inf.ontop.dbschema.impl.QuotedIDImpl;
import it.unibz.inf.ontop.dbschema.impl.SQLStandardQuotedIDFactory;
import it.unibz.inf.ontop.generation.algebra.*;
import it.unibz.inf.ontop.generation.serializer.SQLSerializationException;
import it.unibz.inf.ontop.generation.serializer.impl.DefaultSelectFromWhereSerializer;
import it.unibz.inf.ontop.generation.serializer.impl.SQLTermSerializer;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.utils.ImmutableCollectors;
import it.unibz.inf.ontop.utils.StringUtils;
import org.aksw.commons.sql.codec.api.SqlCodec;
import org.aksw.commons.sql.codec.util.SqlCodecUtils;
import org.aksw.r2rml.jena.sql.transform.SqlParseException;
import org.aksw.r2rml.sql.transform.SqlUtils;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Lorenz Buehmann
 */
public class SparkSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer {

    @Inject
    private SparkSelectFromWhereSerializer(TermFactory termFactory) {
        super(new SparkSQLTermSerializer(termFactory));
    }

//    @Inject
//    protected SparkSelectFromWhereSerializer(SQLTermSerializer sqlTermSerializer) {
//        super(sqlTermSerializer);
//    }

    public static boolean offsetEnabled = true;
    public static boolean orderByWorkaround = false;

    public static QuotedIDFactory quotedIdFactory = new SparkQuotedIDFactory();

    public static SqlCodec codecDoubleQuotes = SqlCodecUtils.createSqlCodecDoubleQuotes();
    public static SqlCodec codecBackTicks = SqlCodecUtils.createSqlCodecForApacheSpark();

    public static String reEncodeSpark(String rendering) {
        try {
            return SqlUtils.reencodeColumnName(rendering, SparkSelectFromWhereSerializer.codecDoubleQuotes, SparkSelectFromWhereSerializer.codecBackTicks);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        return rendering;
    }

    @Override
    public QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters) {
        return selectFromWhere.acceptVisitor(
                new DefaultRelationVisitingSerializer(quotedIdFactory) {

                    final Deque<Boolean> handleOffsetStack = new ArrayDeque<>();
                    String rowNumCol = "row_num";
                    private boolean needOffsetProcessed() {
                        return Optional.ofNullable(handleOffsetStack.peek()).orElse(false);
                    }

                    final Deque<Boolean> distinctStack = new ArrayDeque<>();
                    final Deque<ImmutableSortedSet<Variable>> projectionVariableStack = new ArrayDeque<>();
                    final Deque<ImmutableMap<Variable, QuotedID>> variableAliasesStack = new ArrayDeque<>();

                    @Override
                    public QuerySerialization visit(SelectFromWhereWithModifiers selectFromWhere) {
                        // for offset handling
                        long offset = selectFromWhere.getOffset().orElse(0L);
                        handleOffsetStack.push(offsetEnabled && offset > 0);

                        // for "distinct - order by" workaround
                        distinctStack.push(selectFromWhere.isDistinct());
                        projectionVariableStack.push(selectFromWhere.getProjectedVariables());

                        QuerySerialization querySerialization = super.visit(selectFromWhere);

                        if (needOffsetProcessed()) {
                            String query = querySerialization.getString();
                            String offsetExpression = String.format("%s > %d", rowNumCol, offset);
                            long limit = selectFromWhere.getLimit().orElse(0L);
                            if (limit > 0) {
                                offsetExpression += String.format(" AND %s < %d", rowNumCol, offset+limit);
                            }
                            String newQuery = String.format("WITH result AS (%s) SELECT * FROM result WHERE %s", query, offsetExpression);
                            querySerialization = new DefaultSelectFromWhereSerializer.QuerySerializationImpl(newQuery, querySerialization.getColumnIDs());
                        }

                        handleOffsetStack.pop();

                        projectionVariableStack.pop();
                        distinctStack.pop();
                        variableAliasesStack.pop();
                        return querySerialization;
                    }

                    @Override
                    protected String serializeProjection(ImmutableSortedSet<Variable> projectedVariables,
                                                         ImmutableMap<Variable, QuotedID> variableAliases,
                                                         ImmutableSubstitution<? extends ImmutableTerm> substitution,
                                                         ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
                        variableAliasesStack.push(variableAliases);
                        String projection = super.serializeProjection(projectedVariables, variableAliases, substitution, columnIDs);
                        if (needOffsetProcessed()) {
                            String sortExprStr = projectedVariables.stream().map((v) ->
                                    SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(Optional.ofNullable(substitution.get(v)).orElse(v), columnIDs))
                                    .collect(Collectors.joining(", "));
                            projection += String.format(", row_number() over (order by %s) as %s", sortExprStr, rowNumCol);
                        }
                        return projection;
                    }

                    @Override
                    protected String serializeLimit(long limit, boolean noSortCondition) {
                        if (limit < 0) {
                            return "";
                        } else {
                            return String.format("LIMIT %d", limit);
                        }
                    }

                    @Override
                    protected String serializeLimitOffset(long limit, long offset, boolean noSortCondition) {
                        if (limit == 0) {
                            return "LIMIT 0";
                        }

                        if (limit < 0) {
                            if (offset <= 0) {
                                return "";
                            } else {
                                if (!offsetEnabled) {
                                    throw new SQLSerializationException("OFFSET is not supported in Spark");
                                } else {
                                    return "";
                                }
                            }
                        } else {
                            if (offset < 0) {
                                // If the offset is not specified
                                return String.format("LIMIT %d", limit);
                            } else {
                                if (!offsetEnabled) {
                                    throw new SQLSerializationException("OFFSET is not supported in Spark");
                                } else {
                                    return "";
                                }
                            }
                        }
                    }

                    @Override
                    protected String serializeOffset(long offset, boolean noSortCondition) {
                        if (offset < 0) {
                            // If the offset is not specified
                            return "";
                        } else {
                            if (!offsetEnabled) {
                                throw new SQLSerializationException("OFFSET is not supported in Spark");
                            } else {
                                return "";
                            }
                        }
                    }

                    @Override
                    protected String serializeOrderBy(ImmutableList<SQLOrderComparator> sortConditions, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
                        if (orderByWorkaround && Optional.ofNullable(distinctStack.peek()).orElse(false)) {
                            if (sortConditions.isEmpty()) {
                                return "";
                            } else {
                                ImmutableSortedSet<Variable> projectedVars = projectionVariableStack.peek();
                                ImmutableMap<Variable, QuotedID> aliases = variableAliasesStack.peek();

                                String conditionString = sortConditions.stream().map((c) -> {

                                    String s = (c.getTerm() instanceof Variable)
                                            ? (projectedVars.contains(c) ? aliases.get(c).getSQLRendering() : SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(c.getTerm(), columnIDs))
                                            : SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(c.getTerm(), columnIDs);

                                    return s + (c.isAscending() ? " NULLS FIRST" : " DESC NULLS LAST");
                                }).collect(Collectors.joining(", "));

//                                String conditionString = sortConditions.stream().map((c) ->
//                                    SparkSelectFromWhereSerializer.this.sqlTermSerializer.serialize(c.getTerm(), columnIDs) + (c.isAscending() ? " NULLS FIRST" : " DESC NULLS LAST")
//                                ).collect(Collectors.joining(", "));
                                return String.format("ORDER BY %s\n", conditionString);
                            }
                        }
                        return super.serializeOrderBy(sortConditions, columnIDs);
                    }

                    @Override
                    protected String formatBinaryJoin(String operatorString, QuerySerialization left, QuerySerialization right, String onString) {
                        return String.format("(%s\n %s \n%s %s)", left.getString(), operatorString, right.getString(), onString);
                    }

                    private ImmutableMap<Variable, QualifiedAttributeID> replaceRelationAlias(RelationID alias, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
                        return columnIDs.entrySet().stream()
                                .collect(ImmutableCollectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new QualifiedAttributeID(alias, e.getValue().getAttribute())));
                    }

                    @Override
                    public QuerySerialization visit(SQLTable sqlTable) {
                        RelationID alias = generateFreshViewAlias();
                        RelationDefinition relation = sqlTable.getRelationDefinition();
                        String relationRendering = relation.getAtomPredicate().getName();
                        // we have to replace double quotes with backticks here
                        relationRendering = SparkSelectFromWhereSerializer.reEncodeSpark(relationRendering);

                        String sql = String.format("%s %s", relationRendering, alias.getSQLRendering());
                        return new QuerySerializationImpl(sql, attachRelationAlias(alias, sqlTable.getArgumentMap().entrySet().stream()
                                .collect(ImmutableCollectors.toMap(
                                        // Ground terms must have been already removed from atoms
                                        e -> (Variable) e.getValue(),
                                        e -> relation.getAttribute(e.getKey() + 1).getID()))));
                    }

                    ImmutableMap<Variable, QualifiedAttributeID> attachRelationAlias(RelationID alias, ImmutableMap<Variable, QuotedID> variableAliases) {
                        return variableAliases.entrySet().stream()
                                .collect(ImmutableCollectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new QualifiedAttributeID(alias, e.getValue())));
                    }
                }
        );
    }

    protected static class SparkSQLTermSerializer extends DefaultSQLTermSerializer {

        protected SparkSQLTermSerializer(TermFactory termFactory) {
            super(termFactory);
        }

        @Override
        public String serialize(ImmutableTerm term, ImmutableMap<Variable, QualifiedAttributeID> columnIDs)
                throws SQLSerializationException {
            if (term instanceof Variable) {
                return Optional.ofNullable(columnIDs.get(term))
                        .map(a -> {
                            // we have to replace double quotes with backticks here
                            String rendering = a.getSQLRendering();
                            rendering = SparkSelectFromWhereSerializer.reEncodeSpark(rendering);
                            return rendering;
                        })
                        //                               .map(QualifiedAttributeID::getSQLRendering)
                        .orElseThrow(() -> new SQLSerializationException(String.format(
                                "The variable %s does not appear in the columnIDs", term)));
            }
            return super.serialize(term, columnIDs);
        }

        private String serializeConstant(Constant constant) {
            if (constant.isNull())
                return constant.getValue();
            if (!(constant instanceof DBConstant)) {
                throw new SQLSerializationException(
                        "Only DBConstants or NULLs are expected in sub-tree to be translated into SQL");
            }
            return serializeDBConstant((DBConstant) constant);
        }

        protected String serializeDBConstant(DBConstant constant) {
            DBTermType dbType = constant.getType();

            switch (dbType.getCategory()) {
                case DECIMAL:
                case FLOAT_DOUBLE:
                    // TODO: handle the special case of not-a-number!
                    return castFloatingConstant(constant.getValue(), dbType);
                case INTEGER:
                case BOOLEAN:
                    return constant.getValue();
                default:
                    return serializeStringConstant(constant.getValue());
            }
        }

        protected String castFloatingConstant(String value, DBTermType dbType) {
            return String.format("CAST(%s AS %s)", value, dbType.getCastName());
        }

        private static final ImmutableMap<Character, String> BACKSLASH = ImmutableMap.of('\\', "\\\\");
        @Override
        protected String serializeStringConstant(String constant) {
            // parent method + doubles backslashes
            return StringUtils.encode(super.serializeStringConstant(constant), BACKSLASH);
        }


    }
}
